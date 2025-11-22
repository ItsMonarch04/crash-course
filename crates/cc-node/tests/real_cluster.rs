// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_host::journal::{InputJournal, JournalTermination, RecordedBootImage, replay_journal};

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

fn request_eventually(ports: &[u16], parts: &[&str]) -> (u16, Vec<u8>) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        for port in ports {
            if let Ok(reply) = request(*port, parts)
                && !reply.starts_with(b"-")
            {
                return (*port, reply);
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("no node acknowledged {parts:?}");
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

fn assert_disk_fault_is_fatal(
    binary: &Path,
    config: &Path,
    variable: &str,
    record_path: Option<&Path>,
) {
    let mut command = Command::new(binary);
    command
        .args(["run", "--config"])
        .arg(config)
        .env(variable, "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(record_path) = record_path {
        command.args(["--record"]).arg(record_path);
    }
    let mut child = command.spawn().expect("start disk fault node");
    // The initial election itself must persist hard state. A fatal durability
    // shim may therefore terminate before a listener becomes observable.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().expect("wait for disk fault node") {
            assert!(!status.success(), "{variable} did not terminate the node");
            if let Some(record_path) = record_path {
                let journal = InputJournal::decode(&fs::read(record_path).expect("fatal CCIJ"))
                    .expect("decode fatal CCIJ");
                assert_eq!(
                    journal.footer.map(|footer| footer.termination),
                    Some(JournalTermination::FatalIo),
                    "fatal disk failure must label its durable replay prefix"
                );
            }
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
fn trap_real_host_effects_match_replay() {
    let binary = Path::new(env!("CARGO_BIN_EXE_ccdb"));
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let base = std::env::temp_dir().join(format!("cc-node-process-{unique}"));
    fs::create_dir_all(&base).expect("test base directory");
    let init = Command::new(binary)
        .args([
            "init",
            "--cluster",
            "test",
            "--cluster-id",
            "00112233445566778899aabbccddeeff",
            "--nodes",
            "3",
            "--base-dir",
        ])
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

    let ports = [7101, 7102, 7103];
    let (leader_port, set) = request_eventually(&ports, &["SET", "process", "fixture"]);
    assert!(set.starts_with(b"+OK"), "set response: {set:?}");
    for _ in 0..4 {
        let (_, reply) = request_eventually(&ports, &["INCR", "counter"]);
        assert!(!reply.starts_with(b"-"), "incr response: {reply:?}");
    }
    let admin = Command::new(binary)
        .args([
            "admin",
            "--addr",
            &format!("127.0.0.1:{leader_port}"),
            "status",
        ])
        .output()
        .expect("admin status");
    assert!(admin.status.success(), "admin failed: {admin:?}");
    let admin_text = String::from_utf8_lossy(&admin.stdout);
    assert!(
        admin_text.contains(&format!("resolved=127.0.0.1:{leader_port}")),
        "{admin_text}"
    );
    assert!(admin_text.contains("role:leader"), "{admin_text}");

    let leader_index = usize::from(leader_port - 7101);
    stop(&mut nodes[leader_index]);
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut failover = Vec::new();
    while Instant::now() < deadline {
        for port in ports {
            if port == leader_port {
                continue;
            }
            if let Ok(reply) = request(port, &["INCR", "counter"])
                && !reply.starts_with(b"-")
            {
                failover = reply;
                break;
            }
        }
        if !failover.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(!failover.is_empty(), "failover did not acknowledge a write");

    let restarted_id = leader_index + 1;
    let mut restarted =
        start(binary, &base.join(format!("n{restarted_id}/ccdb.toml"))).expect("restart node");
    assert!(wait_for_port(leader_port), "restarted node did not start");
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut recovered = Vec::new();
    while Instant::now() < deadline {
        for port in ports {
            if let Ok(reply) = request(port, &["GET", "counter"])
                && reply.windows(1).any(|window| window == b"5")
            {
                recovered = reply;
                break;
            }
        }
        if !recovered.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        !recovered.is_empty(),
        "restarted node did not catch up: {recovered:?}"
    );

    stop(&mut restarted);
    for node in nodes {
        stop(node);
    }

    let record_base = base.join("record");
    let record_init = Command::new(binary)
        .args([
            "init",
            "--cluster",
            "record",
            "--cluster-id",
            "00112233445566778899aabbccddeeff",
            "--nodes",
            "1",
            "--base-dir",
        ])
        .arg(&record_base)
        .output()
        .expect("initialize recording fixture");
    assert!(
        record_init.status.success(),
        "record init failed: {record_init:?}"
    );
    let record_config = record_base.join("n1/ccdb.toml");
    let record_path = record_base.join("run.ccij");
    let mut recorded = Command::new(binary)
        .args(["run", "--config"])
        .arg(&record_config)
        .args(["--record"])
        .arg(&record_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start recording node");
    assert!(wait_for_port(7101), "recording node did not start");
    let (_, reply) = request_eventually(&[7101], &["SET", "record", "fixture"]);
    assert!(reply.starts_with(b"+OK"), "recorded reply: {reply:?}");
    let (_, first_retry) =
        request_eventually(&[7101], &["CC.REQUEST", "77", "1", "INCR", "retry-counter"]);
    assert_eq!(first_retry, b":1\r\n", "first session reply");
    let (_, duplicate_retry) =
        request_eventually(&[7101], &["CC.REQUEST", "77", "1", "INCR", "retry-counter"]);
    assert_eq!(
        duplicate_retry, b":1\r\n",
        "reconnect retry must reuse the durable result"
    );
    let (_, counter) = request_eventually(&[7101], &["GET", "retry-counter"]);
    assert_eq!(counter, b"$1\r\n1\r\n", "duplicate must not apply twice");
    stop(&mut recorded);
    let journal = InputJournal::decode(&fs::read(&record_path).expect("record file"))
        .expect("decode durable CCIJ prefix");
    assert!(
        !journal.boot_image.is_empty(),
        "recording must embed its boot WAL"
    );
    let boot = RecordedBootImage::decode(&journal.boot_image).expect("decode recorded boot image");
    assert_eq!(boot.config.id.get(), 1, "boot image node identity");
    assert_eq!(
        boot.cluster_id,
        [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]
    );
    assert!(boot.membership.voters.contains(&cc_core::NodeId::new(1)));
    assert_eq!(boot.build_label, env!("CARGO_PKG_VERSION"));
    assert!(
        journal
            .records
            .iter()
            .any(|record| !record.effects.is_empty()),
        "recording must pair a delivered input with an effect batch"
    );
    let replay = replay_journal(&journal).expect("replay the actual real-host receipt");
    assert_eq!(replay.records, journal.records.len());
    assert_eq!(
        replay.termination, None,
        "killed host leaves a prefix receipt"
    );

    let fatal_base = base.join("fatal");
    let fatal_init = Command::new(binary)
        .args([
            "init",
            "--cluster",
            "fatal",
            "--cluster-id",
            "00112233445566778899aabbccddeeff",
            "--nodes",
            "1",
            "--base-dir",
        ])
        .arg(&fatal_base)
        .output()
        .expect("initialize single-node fault fixture");
    assert!(
        fatal_init.status.success(),
        "fatal init failed: {fatal_init:?}"
    );
    let fatal_config = fatal_base.join("n1/ccdb.toml");
    assert_disk_fault_is_fatal(
        binary,
        &fatal_config,
        "CCDB_FAIL_FSYNC",
        Some(&fatal_base.join("fatal-fsync.ccij")),
    );
    assert_disk_fault_is_fatal(binary, &fatal_config, "CCDB_FAIL_ENOSPC", None);
    fs::remove_dir_all(base).expect("remove process fixture");
}
