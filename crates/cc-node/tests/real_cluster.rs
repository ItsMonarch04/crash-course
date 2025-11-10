// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn frame(parts: &[&str]) -> Vec<u8> {
    let mut output = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        output.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        output.extend_from_slice(part.as_bytes());
        output.extend_from_slice(b"\r\n");
    }
    output
}

fn request(port: u16, parts: &[&str]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(&frame(parts))?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut output = Vec::new();
    stream.read_to_end(&mut output)?;
    Ok(output)
}

fn wait_for_port(port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline {
        if ("127.0.0.1", port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut addresses| addresses.next())
            .and_then(|address| {
                TcpStream::connect_timeout(&address, Duration::from_millis(100)).ok()
            })
            .is_some()
        {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

fn start(binary: &Path, config: &Path) -> std::io::Result<Child> {
    Command::new(binary)
        .args(["run", "--config"])
        .arg(config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn assert_disk_fault_is_fatal(binary: &Path, config: &Path, variable: &str) {
    let mut child = Command::new(binary)
        .args(["run", "--config"])
        .arg(config)
        .env(variable, "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start disk fault node");
    assert!(wait_for_port(7101), "disk fault node did not start");
    let _ = request(7101, &["SET", "fatal", variable]);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().expect("wait for disk fault node") {
            assert!(!status.success(), "{variable} did not terminate the node");
            break;
        }
        if Instant::now() >= deadline {
            stop(&mut child);
            panic!("{variable} did not terminate the node");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

struct ProcessGuard {
    children: Vec<Child>,
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        for child in &mut self.children {
            stop(child);
        }
    }
}

#[test]
fn three_node_processes_replicate_failover_and_recover() {
    let binary = Path::new(env!("CARGO_BIN_EXE_ccdb"));
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let base = std::env::temp_dir().join(format!("cc-node-process-{unique}"));
    fs::create_dir_all(&base).expect("test base directory");
    let init = Command::new(binary)
        .args(["init", "--cluster", "test", "--nodes", "3", "--base-dir"])
        .arg(&base)
        .output()
        .expect("run init");
    assert!(init.status.success(), "init failed: {:?}", init);

    let mut nodes = Vec::new();
    for node in 1..=3 {
        let child = start(binary, &base.join(format!("n{node}/ccdb.toml"))).expect("start node");
        nodes.push(child);
    }
    let mut guard = ProcessGuard { children: nodes };
    let nodes = &mut guard.children;
    assert!(
        (1..=3).all(|node| wait_for_port(7100 + node)),
        "nodes did not start"
    );

    let set = request(7101, &["SET", "process", "fixture"]).expect("set");
    assert!(set.starts_with(b"+OK"), "set response: {set:?}");
    for _ in 0..4 {
        let reply = request(7101, &["INCR", "counter"]).expect("incr");
        assert!(!reply.starts_with(b"-"), "incr response: {reply:?}");
    }
    let admin = Command::new(binary)
        .args(["admin", "--addr", "127.0.0.1:7102", "status"])
        .output()
        .expect("admin status");
    assert!(admin.status.success(), "admin failed: {admin:?}");
    let admin_text = String::from_utf8_lossy(&admin.stdout);
    assert!(
        admin_text.contains("resolved=127.0.0.1:7101"),
        "{admin_text}"
    );
    assert!(admin_text.contains("role:leader"), "{admin_text}");

    stop(&mut nodes[0]);
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut failover = Vec::new();
    while Instant::now() < deadline {
        if let Ok(reply) = request(7102, &["INCR", "counter"])
            && !reply.starts_with(b"-")
        {
            failover = reply;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(!failover.is_empty(), "failover did not acknowledge a write");

    fs::remove_file(base.join("n1/commands.log")).expect("wipe leader journal");
    fs::remove_file(base.join("n1/trace.log")).expect("wipe leader trace");
    let mut restarted = start(binary, &base.join("n1/ccdb.toml")).expect("restart node");
    assert!(wait_for_port(7101), "restarted node did not start");
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut recovered = Vec::new();
    while Instant::now() < deadline {
        if let Ok(reply) = request(7101, &["GET", "counter"])
            && reply.windows(1).any(|window| window == b"5")
        {
            recovered = reply;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        !recovered.is_empty(),
        "restarted node did not catch up: {recovered:?}"
    );

    stop(&mut restarted);
    for node in &mut nodes[1..] {
        stop(node);
    }
    let fatal_config = base.join("n1/fatal.toml");
    fs::write(
        &fatal_config,
        format!(
            "[node]\nid = 1\ndata_dir = \"{}\"\nlisten_client = \"127.0.0.1:7101\"\nlisten_peer = \"127.0.0.1:7201\"\npeer_nodes = \"127.0.0.1:7201\"\n",
            base.join("n1").display()
        ),
    )
    .expect("write fatal test config");
    assert_disk_fault_is_fatal(binary, &fatal_config, "CCDB_FAIL_FSYNC");
    assert_disk_fault_is_fatal(binary, &fatal_config, "CCDB_FAIL_ENOSPC");
    fs::remove_dir_all(base).expect("remove process fixture");
}
