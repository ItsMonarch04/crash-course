# Compatibility status

The immutable current-format compatibility base is
`2154f1688231ecfc4de7a50a7899a90c327f844b` (`v0.11.14`). Its receipt is
[`tests/golden/compat-base.txt`](../tests/golden/compat-base.txt), including
the full producer commit, Cargo.lock hash, toolchain/target, `ccdb` binary
hash, and exact `ccdb --version` output hash.

[`tests/golden/manifest.tsv`](../tests/golden/manifest.tsv) contains 46
hashed rows: 27 immutable current-format fixtures from the compatibility
base, eight audited `2c733f3` legacy fixtures, and 11 current worktree formats
introduced by the storage, snapshot, batching, and backup roadmap. Each
source generator is run twice into separate directories and the outputs are
compared byte-for-byte before installation. Every row records immutable
producer provenance, a semantic sidecar, a current-reader policy, and its
migration boundary. Sidecars begin with `reader_test=<named receipt>` so the
manifest checker ties each fixture to a checked-in executable test.

The compatibility contract is deliberately narrow. The current reader accepts
the retained v1 WAL, SST, META, command, peer-frame, trace, and history
fixtures. CCBK v1 node-clone fixtures are refused by default and accepted only
through `--accept-legacy-node-backup`; the importer discards node/config
identity and never counts the archive as cluster-complete evidence. CCBK v2
is the logical fresh-cluster backup writer; its production and fixture paths
share `cc-cluster::backup` as the canonical codec owner. A raised CCID reader
floor is refused before opening any other node
state. Neither rule is a claim of arbitrary old directory support.

Preflight now runs the manifest checker in strict mode: an empty manifest or a
missing documented format fails. CI verifies hashes and reader-test ownership;
it never regenerates fixture bytes.
