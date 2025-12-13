# Contributing

Crash Course is a correctness laboratory. Keep changes small, deterministic,
and readable enough for a student to follow.

## Before opening a change

- Run `cargo fmt --all -- --check`.
- Run `cargo clippy --locked --workspace --all-targets -- -D warnings`.
- Run `./scripts/ci/forbidden-grep.sh`.
- Run `cargo test --locked --workspace --all-targets` and `./scripts/preflight.sh`.
- Add a focused regression test for each known failure mode touched.
- Update the public rustdoc.

Core code must not read ambient time, ambient randomness, environment variables,
the filesystem, or unordered maps. Core quantities are integer-only. Effects
and inputs cross host boundaries as values. A persisted or wire format has a
version, a documented layout, bounds checks, and a corruption test.

Numbered decisions are append-only. If a change crosses crate boundaries,
changes bytes, or changes determinism, write a new ADR before implementation.

Report suspected security vulnerabilities privately — see
[`SECURITY.md`](../SECURITY.md), not a public issue.
