// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Test-only C0 identity fixture emitter.
//!
//! The compatibility generator copies this module beside the recorded cut's
//! `main.rs`.  The bytes therefore come from that cut's private
//! `DiskIdentity::encode`, including its exact reader floors and checksum.

use std::fs;

use super::*;

#[test]
fn emit_c0_identity_fixtures_when_requested() {
    let Some(out) = std::env::var_os("CC_C0_CCID_OUT").map(PathBuf::from) else {
        return;
    };
    fs::create_dir_all(&out).expect("create C0 CCID output");
    for (name, lifecycle, semantic) in [
        (
            "ccid-v1",
            IDENTITY_ACTIVE,
            "active node 1 identity for the fixed C0 cluster id",
        ),
        (
            "ccid-joining-v1",
            IDENTITY_JOINING,
            "joining node 1 identity for the fixed C0 cluster id",
        ),
        (
            "ccid-removed-v1",
            IDENTITY_REMOVED,
            "terminally removed node 1 identity for the fixed C0 cluster id",
        ),
    ] {
        let mut identity = DiskIdentity::fresh(
            ClusterId::from_hex("31313131313131313131313131313131").expect("fixed C0 cluster id"),
            1,
        );
        identity.lifecycle = lifecycle;
        fs::write(out.join(format!("{name}.bin")), identity.encode())
            .expect("write C0 CCID fixture");
        fs::write(
            out.join(format!("{name}.txt")),
            format!(
                "reader_test=trap_ccid_is_exact_checksum_fenced_and_removed_is_terminal\nformat=CCID\nsemantic={semantic}\n"
            ),
        )
        .expect("write C0 CCID sidecar");
    }
}
