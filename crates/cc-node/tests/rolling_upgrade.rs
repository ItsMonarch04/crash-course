// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Pinned compatibility-cut to worktree process replacement.
//!
//! CI builds commit `acabf51` separately and supplies its `ccdb` through
//! `CC_COMPAT_CCDB`.  The test is ignored in the ordinary workspace run so a
//! developer never accidentally substitutes the current binary for the old
//! side of the matrix.

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn frame(parts: &[&str]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        bytes.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        bytes.extend_from_slice(part.as_bytes());
        bytes.extend_from_slice(b"\r\n");
    }
    bytes
}

fn request(port: u16, parts: &[&str]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(&frame(parts))?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply)?;
    Ok(reply)
}

fn request_eventually(ports: &[u16], parts: &[&str]) -> (usize, Vec<u8>) {
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut last = Vec::new();
    while Instant::now() < deadline {
        for (index, port) in ports.iter().enumerate() {
            match request(*port, parts) {
                Ok(reply) if !reply.starts_with(b"-") => return (index, reply),
                Ok(reply) => last.push((*port, String::from_utf8_lossy(&reply).into_owned())),
                Err(error) => last.push((*port, error.to_string())),
            }
        }
        if last.len() > ports.len() {
            last.drain(..last.len() - ports.len());
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("no process acknowledged {parts:?}; last replies={last:?}");
}

fn free_ports(count: usize) -> Vec<u16> {
    let mut listeners = Vec::with_capacity(count);
    for _ in 0..count {
        listeners.push(TcpListener::bind(("127.0.0.1", 0)).expect("reserve loopback port"));
    }
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("reserved address").port())
        .collect();
    drop(listeners);
    ports
}

fn rewrite_addresses(base: &Path, client: &[u16], peer: &[u16], metrics: &[u16]) {
    for node in 1..=3 {
        let path = base.join(format!("n{node}/ccdb.toml"));
        let mut text = fs::read_to_string(&path).expect("generated config");
        for member in 1..=3 {
            text = text.replace(
                &format!(":{}", 7100 + member),
                &format!(":{}", client[member - 1]),
            );
            text = text.replace(
                &format!(":{}", 7200 + member),
                &format!(":{}", peer[member - 1]),
            );
            text = text.replace(
                &format!(":{}", 7300 + member),
                &format!(":{}", metrics[member - 1]),
            );
        }
        fs::write(path, text).expect("relocated config");
    }
}

fn start(binary: &Path, config: &Path) -> Child {
    let mut command = Command::new(binary);
    command.args(["run", "--config"]).arg(config);
    if std::env::var_os("CC_TEST_VERBOSE").is_none() {
        command.stdout(Stdio::null()).stderr(Stdio::null());
    }
    command.spawn().expect("start ccdb process")
}

fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(6);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("listener {port} did not become ready");
}

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

struct Processes(Vec<Option<Child>>);

impl Drop for Processes {
    fn drop(&mut self) {
        for child in self.0.iter_mut().flatten() {
            stop(child);
        }
    }
}

fn compat_binary() -> PathBuf {
    let value = std::env::var_os("CC_COMPAT_CCDB")
        .expect("CC_COMPAT_CCDB must name the separately built compatibility-cut binary");
    let path = PathBuf::from(value);
    assert!(
        path.is_file(),
        "compatibility binary is absent: {}",
        path.display()
    );
    path
}

#[test]
#[ignore = "requires the separately built immutable compatibility-cut ccdb"]
fn trap_real_rolling_upgrade_keeps_every_ack() {
    let old = compat_binary();
    let current = Path::new(env!("CARGO_BIN_EXE_ccdb"));
    assert_ne!(
        fs::canonicalize(&old).expect("old binary path"),
        fs::canonicalize(current).expect("current binary path"),
        "mixed-build coverage requires distinct executables"
    );
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock")
        .as_nanos();
    let base = std::env::temp_dir().join(format!("cc-real-upgrade-{unique}"));
    fs::create_dir_all(&base).expect("test base");
    let initialized = Command::new(&old)
        .args([
            "init",
            "--cluster",
            "upgrade",
            "--cluster-id",
            "1234567890abcdef1234567890abcdef",
            "--nodes",
            "3",
            "--base-dir",
        ])
        .arg(&base)
        .output()
        .expect("compatibility init");
    assert!(
        initialized.status.success(),
        "old init failed: {initialized:?}"
    );

    let ports = free_ports(9);
    let client = &ports[0..3];
    let peer = &ports[3..6];
    let metrics = &ports[6..9];
    rewrite_addresses(&base, client, peer, metrics);

    let mut processes = Processes(
        (1..=3)
            .map(|node| Some(start(&old, &base.join(format!("n{node}/ccdb.toml")))))
            .collect(),
    );
    for port in client {
        wait_for_port(*port);
    }

    let (leader, first) = request_eventually(
        client,
        &["CC.REQUEST", "700", "1", "SET", "upgrade:baseline", "kept"],
    );
    assert_eq!(first, b"+OK\r\n");
    let (_, counter) = request_eventually(
        client,
        &["CC.REQUEST", "700", "2", "INCR", "upgrade:counter"],
    );
    assert_eq!(counter, b":1\r\n");
    let mut replacement_order: Vec<usize> = (0..3).filter(|index| *index != leader).collect();
    replacement_order.push(leader);
    for (phase, index) in replacement_order.into_iter().enumerate() {
        stop(processes.0[index].as_mut().expect("running old process"));
        processes.0[index] = Some(start(
            current,
            &base.join(format!("n{}/ccdb.toml", index + 1)),
        ));
        wait_for_port(client[index]);

        let sequence = 3 + phase;
        let sequence_text = sequence.to_string();
        let key = format!("upgrade:phase{}", phase + 1);
        let (_, reply) = request_eventually(
            client,
            &[
                "CC.REQUEST",
                "700",
                &sequence_text,
                "SET",
                &key,
                "acknowledged",
            ],
        );
        assert_eq!(reply, b"+OK\r\n");

        // Retrying the latest identity must return its durable cached result
        // on either build without creating a second operation.
        let (_, retry) = request_eventually(
            client,
            &[
                "CC.REQUEST",
                "700",
                &sequence_text,
                "SET",
                &key,
                "acknowledged",
            ],
        );
        assert_eq!(retry, b"+OK\r\n");
    }

    // CAS was added to the RESP surface after the immutable cut, so exercise
    // it after the leader replacement while retaining the same durable
    // request namespace used throughout the mixed-build interval.
    let (_, cas_seed) = request_eventually(
        client,
        &["CC.REQUEST", "700", "6", "SET", "upgrade:cas", "old"],
    );
    assert_eq!(cas_seed, b"+OK\r\n");
    let (_, cas) = request_eventually(
        client,
        &["CC.REQUEST", "700", "7", "CAS", "upgrade:cas", "old", "new"],
    );
    assert_eq!(cas, b":1\r\n");

    for (key, expected) in [
        ("upgrade:baseline", b"$4\r\nkept\r\n".as_slice()),
        ("upgrade:counter", b"$1\r\n1\r\n".as_slice()),
        ("upgrade:cas", b"$3\r\nnew\r\n".as_slice()),
        ("upgrade:phase1", b"$12\r\nacknowledged\r\n".as_slice()),
        ("upgrade:phase2", b"$12\r\nacknowledged\r\n".as_slice()),
        ("upgrade:phase3", b"$12\r\nacknowledged\r\n".as_slice()),
    ] {
        let (_, reply) = request_eventually(client, &["GET", key]);
        assert_eq!(reply, expected, "lost acknowledged value for {key}");
    }
    let (_, membership) = request_eventually(client, &["CC.ADMIN", "MEMBERS"]);
    let text = String::from_utf8_lossy(&membership);
    assert!(text.contains("cluster_id=1234567890abcdef1234567890abcdef"));
    assert!(text.contains("voters=n1,n2,n3"));

    drop(processes);
    fs::remove_dir_all(base).expect("remove rolling-upgrade fixture");
}
