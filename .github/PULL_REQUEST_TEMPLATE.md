## Summary

<!-- What changed and why? -->

## Verification

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `./scripts/ci/forbidden-grep.sh`
- [ ] `cargo test --workspace --all-targets`
- [ ] `./scripts/preflight.sh`

## Correctness and compatibility

- [ ] Every touched failure mode has a named regression test.
- [ ] No frozen surface changed without a new ADR and version note.
- [ ] Persisted/wire layout and limits are documented.
- [ ] Public rustdoc is updated.
- [ ] Any benchmark number includes its environment and workload.

