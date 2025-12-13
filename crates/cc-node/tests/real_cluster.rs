// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_core::{NodeId, PeerAddress};
use cc_host::journal::{InputJournal, JournalTermination, RecordedBootImage, replay_journal};
use cc_resp::RespValue;

#[test]
fn trap_cli_help_matches_real_commands_and_unknowns_fail() {
    let binary = Path::new(env!("CARGO_BIN_EXE_ccdb"));
    let version = Command::new(binary)
        .arg("--version")
        .output()
        .expect("run --version");
    assert!(version.status.success(), "--version failed: {version:?}");
    assert_eq!(
        version.stdout,
        format!("ccdb {}\n", env!("CARGO_PKG_VERSION")).as_bytes()
    );

    let help = Command::new(binary)
        .arg("--help")
        .output()
        .expect("run --help");
    assert!(help.status.success(), "--help failed: {help:?}");
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    for required in [
        "run --join SEED_ADDR",
        "--operator-id ID --sequence N",
        "--new-cluster-id HEX32 --new-node-id ID",
        "--accept-legacy-node-backup",
    ] {
        assert!(help.contains(required), "help omits {required:?}");
    }
    assert!(
        !help
            .lines()
            .any(|line| line.trim_start().starts_with("admin ") && line.contains("snapshot")),
        "help advertises the unsupported snapshot command"
    );

    let unknown = Command::new(binary)
        .arg("definitely-not-a-command")
        .output()
        .expect("run unknown command");
    assert!(!unknown.status.success(), "unknown command succeeded");
    assert!(
        unknown.stdout.is_empty(),
        "unknown command printed normal output"
    );

    let fake_snapshot = Command::new(binary)
        .args(["admin", "snapshot"])
        .output()
        .expect("run unsupported snapshot command");
    assert!(
        !fake_snapshot.status.success(),
        "unsupported snapshot command succeeded"
    );
    assert!(
        fake_snapshot.stdout.is_empty(),
        "unsupported snapshot command fabricated status"
    );
}

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
    // The server's bounded route timeout is three seconds. Wait slightly
    // longer so the client observes UNKNOWN/NOTLEADER rather than converting
    // an honest bounded reply into a local EAGAIN.
    stream.set_read_timeout(Some(Duration::from_secs(4)))?;
    stream.write_all(&frame(parts))?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut output = Vec::new();
    stream.read_to_end(&mut output)?;
    Ok(output)
}

fn request_value(port: u16, value: &RespValue) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(4)))?;
    stream.write_all(&cc_resp::encode(value))?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut output = Vec::new();
    stream.read_to_end(&mut output)?;
    Ok(output)
}

fn bulk(value: &[u8]) -> RespValue {
    RespValue::Bulk(Some(value.to_vec()))
}

fn request_pipeline(port: u16, commands: &[&[&str]]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    for command in commands {
        stream.write_all(&frame(command))?;
    }
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
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = BTreeMap::new();
    let mut preferred = None;
    while Instant::now() < deadline {
        let order = preferred
            .into_iter()
            .chain(
                ports
                    .iter()
                    .copied()
                    .filter(|port| Some(*port) != preferred),
            )
            .collect::<Vec<_>>();
        for port in order {
            match request(port, parts) {
                Ok(reply) if !reply.is_empty() && !reply.starts_with(b"-") => {
                    return (port, reply);
                }
                Ok(reply) => {
                    let text = String::from_utf8_lossy(&reply).into_owned();
                    if let Some(leader_port) = text
                        .split_whitespace()
                        .find_map(|part| part.strip_prefix("addr="))
                        .and_then(|address| address.rsplit_once(':'))
                        .and_then(|(_, port)| port.parse::<u16>().ok())
                        .filter(|port| ports.contains(port))
                    {
                        preferred = Some(leader_port);
                    }
                    last.insert(port, text);
                }
                Err(error) => {
                    last.insert(port, error.to_string());
                }
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("no node acknowledged {parts:?}: {last:?}");
}

fn start(binary: &Path, config: &Path) -> std::io::Result<Child> {
    let mut command = Command::new(binary);
    command.args(["run", "--config"]).arg(config);
    command.stdout(Stdio::null());
    if std::env::var_os("CC_TEST_VERBOSE").is_none() {
        command.stderr(Stdio::null());
    }
    command.spawn()
}

fn start_with_snapshot_threshold(binary: &Path, config: &Path) -> std::io::Result<Child> {
    let mut command = Command::new(binary);
    command
        .args(["run", "--config"])
        .arg(config)
        .env("CCDB_SNAPSHOT_AFTER_BYTES", "131072");
    command.stdout(Stdio::null());
    if std::env::var_os("CC_TEST_VERBOSE").is_none() {
        command.stderr(Stdio::null());
    }
    command.spawn()
}

fn admin_eventually(
    binary: &Path,
    ports: &[u16],
    operation: &[&str],
    operator: u64,
    sequence: u64,
    expected: &str,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = BTreeMap::new();
    while Instant::now() < deadline {
        for port in ports {
            let mut command = Command::new(binary);
            command
                .args(["admin", "--addr", &format!("127.0.0.1:{port}")])
                .args(operation)
                .args([
                    "--operator-id",
                    &operator.to_string(),
                    "--sequence",
                    &sequence.to_string(),
                ]);
            let output = command.output().expect("membership admin command");
            let observation = format!(
                "status={} stdout={} stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            last.insert(*port, observation);
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                if stdout.contains(expected) {
                    return stdout.into_owned();
                }
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "membership operation {operation:?} did not finish as {expected}: {last:?}; members={:?}",
        membership_diagnostics(ports)
    );
}

fn transfer_eventually(binary: &Path, ports: &[u16], target: u16, operator: u64) -> String {
    let target_text = target.to_string();
    let operation = ["transfer-leader", "--node-id", target_text.as_str()];
    let mut terminal = BTreeMap::new();
    for sequence in 1..=4 {
        // The cluster policy's transfer timeout is 15s. Waiting less than that
        // on sequence 1 cannot observe TransferTimeout, and a later sequence
        // is a different admin identity that only sees TransferInProgress.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last = BTreeMap::new();
        while Instant::now() < deadline {
            for port in ports {
                let output = Command::new(binary)
                    .args(["admin", "--addr", &format!("127.0.0.1:{port}")])
                    .args(operation)
                    .args([
                        "--operator-id",
                        &operator.to_string(),
                        "--sequence",
                        &sequence.to_string(),
                    ])
                    .output()
                    .expect("leader transfer command");
                let combined = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                last.insert(*port, combined.clone());
                terminal.insert(*port, combined.clone());
                if combined.contains("result=TransferSuccess") {
                    return combined;
                }
                if combined.contains("result=TransferSuperseded")
                    || combined.contains("result=TransferTimeout")
                {
                    // The prior request has an explicit durable result. A new
                    // sequence is therefore a deliberate new operation, not
                    // an unsafe conversion of an ambiguous retry.
                    break;
                }
            }
            if last.values().any(|observation| {
                observation.contains("result=TransferSuperseded")
                    || observation.contains("result=TransferTimeout")
            }) {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
    panic!(
        "leader transfer to n{target} did not reach durable success: {terminal:?}; members={:?}",
        membership_diagnostics(ports)
    );
}

fn cluster_diagnostics(ports: &[u16], key: &str) -> BTreeMap<u16, String> {
    ports
        .iter()
        .map(|port| {
            let info = request(*port, &["INFO"])
                .map(|reply| String::from_utf8_lossy(&reply).into_owned())
                .unwrap_or_else(|error| error.to_string());
            let stale = request(*port, &["READ", "STALE", "GET", key])
                .map(|reply| String::from_utf8_lossy(&reply).into_owned())
                .unwrap_or_else(|error| error.to_string());
            (*port, format!("INFO={info:?} STALE={stale:?}"))
        })
        .collect()
}

/// Remove a voter that must not be the current leader. Removing the leader is
/// deliberately refused until leadership moves, and nothing pins leadership
/// after a transfer, so an operator whose cluster re-elected the old leader
/// transfers again under a fresh operator id and reissues the same unproposed
/// removal pair. A successful removal can close the removed node's client
/// socket before its terminal reply arrives, so retries must query every
/// surviving replica for the replicated AdminRequest result.
fn remove_voter_eventually(
    binary: &Path,
    ports: &[u16],
    node_id: u16,
    away_from: u16,
    operator: u64,
) -> String {
    let node_text = node_id.to_string();
    let remove = ["remove", "--node-id", node_text.as_str()];
    let mut last = String::new();
    for attempt in 0..4_u64 {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            for port in ports {
                let output = Command::new(binary)
                    .args(["admin", "--addr", &format!("127.0.0.1:{port}")])
                    .args(remove)
                    .args(["--operator-id", &operator.to_string(), "--sequence", "1"])
                    .output()
                    .expect("voter removal command");
                let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
                if output.status.success() && stdout.contains("result=Applied") {
                    return stdout;
                }
                last = format!(
                    "port={port} status={} stdout={stdout} stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            thread::sleep(Duration::from_millis(50));
        }
        transfer_eventually(binary, ports, away_from, operator + 100 + attempt);
    }
    panic!(
        "removal of n{node_id} never applied: {last}; members={:?}",
        membership_diagnostics(ports)
    );
}

fn membership_diagnostics(ports: &[u16]) -> BTreeMap<u16, String> {
    ports
        .iter()
        .map(|port| {
            let members = request(*port, &["CC.ADMIN", "MEMBERS", "CONSISTENT"])
                .map(|reply| String::from_utf8_lossy(&reply).into_owned())
                .unwrap_or_else(|error| error.to_string());
            (*port, members)
        })
        .collect()
}

fn metric_value(port: u16, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.lines().find_map(|line| {
        line.strip_prefix(name)
            .and_then(|suffix| suffix.strip_prefix(' '))
            .and_then(|value| value.parse::<u64>().ok())
    })
}

fn wait_for_metric(ports: &[u16], name: &str, minimum: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if ports
            .iter()
            .any(|port| metric_value(*port, name).is_some_and(|value| value >= minimum))
        {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("metric {name} never reached {minimum} on {ports:?}");
}

fn history_set(history: &mut String, epoch: Instant, ports: &[u16], key: &str, value: &str) {
    let invoke = u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let (_, reply) = request_eventually(ports, &["SET", key, value]);
    let complete = u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
    assert_eq!(reply, b"+OK\r\n", "history SET");
    history.push_str(&format!(
        "SET\t{}\t{}\t{invoke}\t{complete}\n",
        hex(key.as_bytes()),
        hex(value.as_bytes())
    ));
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn rewrite_cluster_ports(
    base: &Path,
    nodes: u16,
    client_base: u16,
    peer_base: u16,
    metrics_base: u16,
) {
    for node in 1..=nodes {
        let path = base.join(format!("n{node}/ccdb.toml"));
        let mut text = fs::read_to_string(&path).expect("read generated config");
        for prior_node in 1..=nodes {
            text = text.replace(
                &format!(":{}", 7100_u16 + prior_node),
                &format!(":{}", client_base + prior_node),
            );
            text = text.replace(
                &format!(":{}", 7200_u16 + prior_node),
                &format!(":{}", peer_base + prior_node),
            );
            text = text.replace(
                &format!(":{}", 7300_u16 + prior_node),
                &format!(":{}", metrics_base + prior_node),
            );
        }
        fs::write(path, text).expect("write isolated config");

        // `init` persists the bootstrap routes in Genesis.  Membership
        // addresses are authoritative after recovery, so a process fixture
        // that relocates its listeners must relocate the durable discovery
        // image too; rewriting ccdb.toml alone would deliberately be ignored.
        let wal_path = base.join(format!("n{node}/raft/wal.0"));
        if !wal_path.exists() {
            continue;
        }
        let recovered = cc_log::recover_framed_record_stream(
            &fs::read(&wal_path).expect("read generated Genesis"),
        )
        .expect("decode generated Genesis");
        assert!(recovered.state.entries.is_empty(), "fresh init log");
        let mut genesis = recovered.state.genesis;
        genesis.membership.addresses = (1..=nodes)
            .map(|member| {
                (
                    NodeId::new(u64::from(member)),
                    PeerAddress::V4 {
                        ip: [127, 0, 0, 1],
                        port: peer_base + member,
                    },
                )
            })
            .collect();
        fs::write(
            wal_path,
            cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(Box::new(
                genesis,
            )))
            .expect("encode relocated Genesis"),
        )
        .expect("write relocated Genesis");
    }
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
#[ignore = "runs the real five-process membership and snapshot demonstration"]
fn trap_real_membership_demo_3_to_5() {
    let runs = std::env::var("CC_MEMBERSHIP_RUNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    assert!(
        (1..=20).contains(&runs),
        "CC_MEMBERSHIP_RUNS must be 1..=20"
    );
    for run in 0..runs {
        run_membership_demo(run);
        eprintln!("membership demo: PASS run={}/{}", run + 1, runs);
    }
}

fn run_membership_demo(run: usize) {
    let binary = Path::new(env!("CARGO_BIN_EXE_ccdb"));
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos()
        .saturating_add(run as u128);
    let base = std::env::temp_dir().join(format!("cc-node-membership-{unique}"));
    fs::create_dir_all(&base).expect("membership base directory");
    let init = Command::new(binary)
        .args([
            "init",
            "--cluster",
            "membership-demo",
            "--cluster-id",
            "102132435465768798a9babbdcddedef",
            "--nodes",
            "3",
            "--base-dir",
        ])
        .arg(&base)
        .output()
        .expect("initialize membership cluster");
    assert!(init.status.success(), "init failed: {init:?}");
    let client_base = 20_000_u16 + u16::try_from(unique % 2_000).expect("port offset");
    let peer_base = client_base + 100;
    let metrics_base = peer_base + 100;
    rewrite_cluster_ports(&base, 3, client_base, peer_base, metrics_base);

    let mut children = Vec::new();
    for node in 1..=3 {
        children.push(
            start_with_snapshot_threshold(binary, &base.join(format!("n{node}/ccdb.toml")))
                .expect("start bootstrap node"),
        );
    }
    let mut guard = ProcessGuard { children };
    assert!(
        (1..=3).all(|node| wait_for_port(client_base + node)),
        "bootstrap nodes did not start"
    );
    let mut active_ports = vec![client_base + 1, client_base + 2, client_base + 3];
    let bootstrap_metrics = [metrics_base + 1, metrics_base + 2, metrics_base + 3];
    let epoch = Instant::now();
    let key = "membership-history";
    let mut history =
        String::from("# CC-HISTORY v1: KIND KEY OBSERVED_VALUE INVOKE_NS COMPLETE_NS\n");
    history_set(&mut history, epoch, &active_ports, key, "phase-0");

    // Cross the deliberately small demo checkpoint threshold before the
    // learner exists. Its first catch-up must therefore use the real CCSN
    // sender rather than replaying the discarded prefix.
    let ballast = "x".repeat(8 * 1024);
    for index in 0..40 {
        let ballast_key = format!("ballast-{index:03}");
        let (_, reply) = request_eventually(&active_ports, &["SET", &ballast_key, &ballast]);
        assert_eq!(reply, b"+OK\r\n", "ballast write");
    }
    wait_for_metric(&bootstrap_metrics, "ccdb_snapshots_created_total", 1);

    for node in 4_u16..=5 {
        let seed_port =
            request_eventually(&active_ports, &["SET", "join-seed", &node.to_string()]).0;
        let join_dir = base.join(format!("n{node}-join"));
        let mut join = Command::new(binary);
        join.args([
            "run",
            "--join",
            &format!("127.0.0.1:{seed_port}"),
            "--node-id",
            &node.to_string(),
            "--peer-addr",
            &format!("127.0.0.1:{}", peer_base + node),
            "--client-addr",
            &format!("127.0.0.1:{}", client_base + node),
            "--metrics-addr",
            &format!("127.0.0.1:{}", metrics_base + node),
            "--data-dir",
        ])
        .arg(&join_dir)
        .env("CCDB_SNAPSHOT_AFTER_BYTES", "131072");
        join.stdout(Stdio::null());
        if std::env::var_os("CC_TEST_VERBOSE").is_none() {
            join.stderr(Stdio::null());
        }
        guard
            .children
            .push(join.spawn().expect("start joining node"));
        assert!(wait_for_port(peer_base + node), "joining peer listener");
        assert!(wait_for_port(client_base + node), "joining client listener");
        let before_admission = request(client_base + node, &["GET", key]).expect("joining reply");
        assert!(
            before_admission.starts_with(b"-TRYAGAIN"),
            "joining node served early: {before_admission:?}"
        );

        let operation = [
            "add-learner",
            "--node-id",
            &node.to_string(),
            "--peer-addr",
            &format!("127.0.0.1:{}", peer_base + node),
        ];
        admin_eventually(
            binary,
            &active_ports,
            &operation,
            1_000 + u64::from(node),
            1,
            "result=Applied",
        );
        history_set(
            &mut history,
            epoch,
            &active_ports,
            key,
            &format!("learner-{node}"),
        );

        let catchup_deadline = Instant::now() + Duration::from_secs(10);
        let expected = format!("learner-{node}");
        loop {
            if let Ok(reply) = request(client_base + node, &["READ", "STALE", "GET", key])
                && reply
                    .windows(expected.len())
                    .any(|window| window == expected.as_bytes())
            {
                break;
            }
            if Instant::now() >= catchup_deadline {
                let mut diagnostic_ports = active_ports.clone();
                diagnostic_ports.push(client_base + node);
                panic!(
                    "learner n{node} did not catch up: {:?}; members={:?}",
                    cluster_diagnostics(&diagnostic_ports, key),
                    membership_diagnostics(&diagnostic_ports)
                );
            }
            thread::sleep(Duration::from_millis(50));
        }
        if node == 4 {
            wait_for_metric(&bootstrap_metrics, "ccdb_snapshots_sent_total", 1);
        }

        if std::env::var_os("CC_TEST_VERBOSE").is_some() {
            eprintln!(
                "learner n{node} caught up: {:?}",
                membership_diagnostics(&active_ports)
            );
        }

        let promote = ["promote", "--node-id", &node.to_string()];
        admin_eventually(
            binary,
            &active_ports,
            &promote,
            1_100 + u64::from(node),
            1,
            "result=Applied",
        );
        active_ports.push(client_base + node);
        history_set(
            &mut history,
            epoch,
            &active_ports,
            key,
            &format!("voter-{node}"),
        );
    }

    let update = [
        "update-address",
        "--node-id",
        "5",
        "--peer-addr",
        &format!("127.0.0.1:{}", peer_base + 5),
    ];
    admin_eventually(binary, &active_ports, &update, 1_205, 1, "result=Applied");
    history_set(&mut history, epoch, &active_ports, key, "address-updated");

    transfer_eventually(binary, &active_ports, 4, 1_300);
    history_set(&mut history, epoch, &active_ports, key, "leader-n4");

    remove_voter_eventually(binary, &active_ports, 1, 4, 1_400);
    active_ports.retain(|port| *port != client_base + 1);
    history_set(&mut history, epoch, &active_ports, key, "final");

    let removal_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < removal_deadline {
        if guard.children[0]
            .try_wait()
            .expect("removed node status")
            .is_some()
        {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        guard.children[0]
            .try_wait()
            .expect("removed node final status")
            .is_some(),
        "removed node n1 kept serving"
    );

    for port in &active_ports {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(reply) = request(*port, &["READ", "STALE", "GET", key])
                && reply
                    .windows(b"final".len())
                    .any(|window| window == b"final")
            {
                break;
            }
            assert!(Instant::now() < deadline, "final probe failed on {port}");
            thread::sleep(Duration::from_millis(50));
        }
    }
    let (_, members) = request_eventually(&active_ports, &["CC.ADMIN", "MEMBERS", "CONSISTENT"]);
    assert!(
        members
            .windows(b"voters=n2,n3,n4,n5".len())
            .any(|window| window == b"voters=n2,n3,n4,n5"),
        "unexpected final membership: {}",
        String::from_utf8_lossy(&members)
    );

    let invoke = u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let (_, final_get) = request_eventually(&active_ports, &["GET", key]);
    let complete = u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
    assert_eq!(final_get, b"$5\r\nfinal\r\n");
    history.push_str(&format!(
        "GET\t{}\t{}\t{invoke}\t{complete}\n",
        hex(key.as_bytes()),
        hex(b"final")
    ));
    fs::write(base.join("membership-history.tsv"), &history).expect("write CC-HISTORY");
    let decoded = cc_checker::decode_history_v1_tsv(&history).expect("decode CC-HISTORY");
    assert!(matches!(
        cc_checker::check(&decoded, cc_checker::CheckerConfig::default()),
        cc_checker::Verdict::Linearizable { .. }
    ));

    drop(guard);
    fs::remove_dir_all(base).expect("remove membership fixture");
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
    let client_base = 20_000_u16 + u16::try_from(unique % 5_000).expect("port offset");
    let peer_base = client_base + 5_000;
    let metrics_base = peer_base + 5_000;
    rewrite_cluster_ports(&base, 3, client_base, peer_base, metrics_base);

    let mut nodes = Vec::new();
    for node in 1..=3 {
        let child = start(binary, &base.join(format!("n{node}/ccdb.toml"))).expect("start node");
        nodes.push(child);
    }
    let mut guard = ProcessGuard { children: nodes };
    let nodes = &mut guard.children;
    assert!(
        (1..=3).all(|node| wait_for_port(client_base + node)),
        "nodes did not start"
    );

    let ports = [client_base + 1, client_base + 2, client_base + 3];
    let (leader_port, set) = request_eventually(&ports, &["SET", "process", "fixture"]);
    assert!(set.starts_with(b"+OK"), "set response: {set:?}");
    let stale_port = ports
        .iter()
        .copied()
        .find(|port| *port != leader_port)
        .expect("follower port");
    let stale =
        request(stale_port, &["READ", "STALE", "GET", "process"]).expect("stale local read");
    assert!(
        stale
            .windows(b"STALE".len())
            .any(|window| window == b"STALE"),
        "stale read must be explicitly tagged: {stale:?}"
    );
    let follower_read_deadline = Instant::now() + Duration::from_secs(5);
    let follower_read = loop {
        let reply = request(stale_port, &["READ", "FOLLOWER", "GET", "process"])
            .expect("follower read response");
        if reply
            .windows(b"FOLLOWER".len())
            .any(|window| window == b"FOLLOWER")
        {
            break reply;
        }
        assert!(
            Instant::now() < follower_read_deadline,
            "v3 follower read did not become available after the ReadIndex round: {reply:?}"
        );
        thread::sleep(Duration::from_millis(25));
    };
    assert!(
        follower_read
            .windows(b"FOLLOWER".len())
            .any(|window| window == b"FOLLOWER"),
        "v3 follower read must be explicitly tagged after the ReadIndex round: {follower_read:?}"
    );
    let disabled_batch = request(leader_port, &["MULTI"]).expect("pre-activation MULTI");
    assert_eq!(disabled_batch, b"-FEATUREDISABLED\r\n");
    let activate_batch = Command::new(binary)
        .args([
            "admin",
            "--addr",
            &format!("127.0.0.1:{leader_port}"),
            "feature",
            "activate",
            "atomic-batch",
            "--operator-id",
            "900",
            "--sequence",
            "1",
        ])
        .output()
        .expect("activate atomic batch feature");
    assert!(activate_batch.status.success(), "{activate_batch:?}");
    assert!(
        String::from_utf8_lossy(&activate_batch.stdout).contains("result=Applied"),
        "{activate_batch:?}"
    );
    let activation_deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < activation_deadline {
        if let Ok(reply) = request(leader_port, &["CC.ADMIN", "MEMBERS"])
            && reply
                .windows(b"active_features=0x2".len())
                .any(|window| window == b"active_features=0x2")
        {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let activated =
        request(leader_port, &["CC.ADMIN", "MEMBERS"]).expect("read activated membership state");
    assert!(
        activated
            .windows(b"active_features=0x2".len())
            .any(|window| window == b"active_features=0x2"),
        "atomic batch activation did not commit: {activated:?}"
    );
    let batch = request_pipeline(
        leader_port,
        &[
            &["MULTI"],
            &["SET", "batch-key", "value"],
            &["GET", "batch-key"],
            &["EXEC"],
        ],
    )
    .expect("MULTI/EXEC pipeline");
    assert_eq!(
        batch,
        b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$5\r\nvalue\r\n"
    );
    let nested_batch = RespValue::Array(vec![
        bulk(b"BATCH"),
        RespValue::Array(vec![
            RespValue::Array(vec![bulk(b"SET"), bulk(b"one-shot"), bulk(b"value")]),
            RespValue::Array(vec![bulk(b"GET"), bulk(b"one-shot")]),
        ]),
    ]);
    assert_eq!(
        request_value(leader_port, &nested_batch).expect("one-shot nested BATCH"),
        b"*2\r\n+OK\r\n$5\r\nvalue\r\n"
    );
    let durable_batch = RespValue::Array(vec![
        bulk(b"CC.REQUEST"),
        bulk(b"902"),
        bulk(b"1"),
        bulk(b"BATCH"),
        RespValue::Array(vec![
            RespValue::Array(vec![bulk(b"INCR"), bulk(b"batch-retry")]),
            RespValue::Array(vec![bulk(b"GET"), bulk(b"batch-retry")]),
        ]),
    ]);
    let first_durable_batch =
        request_value(leader_port, &durable_batch).expect("first durable BATCH");
    assert_eq!(first_durable_batch, b"*2\r\n:1\r\n$1\r\n1\r\n");
    assert_eq!(
        request_value(leader_port, &durable_batch).expect("retry durable BATCH"),
        first_durable_batch,
        "the explicit batch envelope must deduplicate as one unit"
    );
    let (_, stable) = request_eventually(&ports, &["SET", "batch-stable", "before"]);
    assert_eq!(stable, b"+OK\r\n");
    let (_, numeric) = request_eventually(&ports, &["SET", "batch-number", "not-a-number"]);
    assert_eq!(numeric, b"+OK\r\n");
    let failed_batch = request_pipeline(
        leader_port,
        &[
            &["MULTI"],
            &["SET", "batch-stable", "after"],
            &["INCR", "batch-number"],
            &["EXEC"],
        ],
    )
    .expect("failing MULTI/EXEC pipeline");
    assert!(
        failed_batch.ends_with(b"-ERR batch failed at index 1: not-numeric\r\n"),
        "failing batch response: {failed_batch:?}"
    );
    let (_, after_failed_batch) = request_eventually(&ports, &["GET", "batch-stable"]);
    assert_eq!(after_failed_batch, b"$6\r\nbefore\r\n");
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

    // A post-activation learner must first discover the committed cluster
    // without voting or serving.  Its inbound CCHL probe gives the leader a
    // current-generation capability proof; only then may the replicated
    // admission proceed.
    let join_dir = base.join("n4-join");
    let mut join_command = Command::new(binary);
    join_command.args([
        "run",
        "--join",
        &format!("127.0.0.1:{leader_port}"),
        "--node-id",
        "4",
        "--peer-addr",
        &format!("127.0.0.1:{}", peer_base + 4),
        "--client-addr",
        &format!("127.0.0.1:{}", client_base + 4),
        "--metrics-addr",
        &format!("127.0.0.1:{}", metrics_base + 4),
        "--data-dir",
    ]);
    join_command.arg(&join_dir);
    if std::env::var_os("CC_TEST_VERBOSE").is_none() {
        join_command.stdout(Stdio::null()).stderr(Stdio::null());
    }
    nodes.push(join_command.spawn().expect("start joining learner"));
    assert!(
        wait_for_port(peer_base + 4),
        "joining learner peer listener did not start"
    );
    let joining_reply =
        request(client_base + 4, &["GET", "process"]).expect("joining node client refusal");
    assert!(
        joining_reply.starts_with(b"-TRYAGAIN"),
        "joining node served before admission: {joining_reply:?}"
    );
    let mut add_learner = None;
    for _ in 0..4 {
        // Probe every possible leader.  Leadership may move between
        // discovery and admission, and capabilities are deliberately
        // connection-generation-local rather than replicated state.
        for peer in 1..=3 {
            let capability = Command::new(binary)
                .args([
                    "peer",
                    "--addr",
                    &format!("127.0.0.1:{}", peer_base + peer),
                    "--config",
                ])
                .arg(join_dir.join("ccdb.toml"))
                .output()
                .expect("probe learner capability");
            assert!(capability.status.success(), "{capability:?}");
        }
        let attempt = Command::new(binary)
            .args([
                "admin",
                "--addr",
                &format!("127.0.0.1:{leader_port}"),
                "add-learner",
                "--node-id",
                "4",
                "--peer-addr",
                &format!("127.0.0.1:{}", peer_base + 4),
                "--operator-id",
                "901",
                "--sequence",
                "1",
            ])
            .output()
            .expect("run replicated add-learner admin command");
        if attempt.status.success() {
            add_learner = Some(attempt);
            break;
        }
        add_learner = Some(attempt);
        thread::sleep(Duration::from_millis(25));
    }
    let add_learner = add_learner.expect("learner admission attempts");
    assert!(add_learner.status.success(), "{add_learner:?}");
    assert!(
        String::from_utf8_lossy(&add_learner.stdout).contains("result=Applied"),
        "{add_learner:?}"
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut members = Vec::new();
    while Instant::now() < deadline {
        if let Ok(reply) = request(leader_port, &["CC.ADMIN", "MEMBERS"])
            && reply
                .windows(b"learners=n4".len())
                .any(|window| window == b"learners=n4")
        {
            members = reply;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        !members.is_empty(),
        "replicated learner admission did not apply: {members:?}"
    );
    let join_deadline = Instant::now() + Duration::from_secs(3);
    let mut joined = Vec::new();
    while Instant::now() < join_deadline {
        if let Ok(reply) = request(client_base + 4, &["READ", "STALE", "GET", "process"]) {
            let caught_up = reply
                .windows(b"fixture".len())
                .any(|window| window == b"fixture");
            joined = reply;
            if caught_up {
                break;
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        joined
            .windows(b"fixture".len())
            .any(|window| window == b"fixture"),
        "learner did not become Active or catch up: {joined:?}"
    );

    let leader_index = ports
        .iter()
        .position(|port| *port == leader_port)
        .expect("leader in configured client ports");
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
    let record_client_base = client_base + 100;
    let record_peer_base = peer_base + 100;
    let record_metrics_base = metrics_base + 100;
    rewrite_cluster_ports(
        &record_base,
        1,
        record_client_base,
        record_peer_base,
        record_metrics_base,
    );
    let record_port = record_client_base + 1;
    let record_config = record_base.join("n1/ccdb.toml");
    let record_path = record_base.join("run.ccij");
    let mut primed = start(binary, &record_config).expect("start recording fixture for priming");
    assert!(wait_for_port(record_port), "priming node did not start");
    let (_, primed_reply) =
        request_eventually(&[record_port], &["SET", "before-recording", "durable"]);
    assert!(
        primed_reply.starts_with(b"+OK"),
        "priming write: {primed_reply:?}"
    );
    stop(&mut primed);

    let mut recorded = Command::new(binary)
        .args(["run", "--config"])
        .arg(&record_config)
        .args(["--record"])
        .arg(&record_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start recording node");
    assert!(wait_for_port(record_port), "recording node did not start");
    let (_, primed_read) = request_eventually(&[record_port], &["GET", "before-recording"]);
    assert_eq!(
        primed_read, b"$7\r\ndurable\r\n",
        "recording must start from the recovered store state"
    );
    let (_, reply) = request_eventually(&[record_port], &["SET", "record", "fixture"]);
    assert!(reply.starts_with(b"+OK"), "recorded reply: {reply:?}");
    let (_, first_retry) = request_eventually(
        &[record_port],
        &["CC.REQUEST", "77", "1", "INCR", "retry-counter"],
    );
    assert_eq!(first_retry, b":1\r\n", "first session reply");
    let (_, duplicate_retry) = request_eventually(
        &[record_port],
        &["CC.REQUEST", "77", "1", "INCR", "retry-counter"],
    );
    assert_eq!(
        duplicate_retry, b":1\r\n",
        "reconnect retry must reuse the durable result"
    );
    let (_, counter) = request_eventually(&[record_port], &["GET", "retry-counter"]);
    assert_eq!(counter, b"$1\r\n1\r\n", "duplicate must not apply twice");
    // Acknowledging the requests above is not by itself the recording
    // receipt: wait until the independently synced CCIJ prefix contains a
    // transition with effects before killing the process. This keeps the
    // test from racing a concurrently admitted timer frame at the exact
    // kill boundary while still failing if effect recording never happens.
    let record_deadline = Instant::now() + Duration::from_secs(2);
    let mut recorded_effect = false;
    while Instant::now() < record_deadline {
        if let Ok(bytes) = fs::read(&record_path)
            && let Ok(prefix) = InputJournal::decode(&bytes)
            && prefix
                .records
                .iter()
                .any(|record| !record.effects.is_empty())
        {
            recorded_effect = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        recorded_effect,
        "recording never synced an effect-bearing frame"
    );
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
        !boot.store_wal.is_empty(),
        "boot image must retain state committed before recording began"
    );
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
    rewrite_cluster_ports(
        &fatal_base,
        1,
        client_base + 200,
        peer_base + 200,
        metrics_base + 200,
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
