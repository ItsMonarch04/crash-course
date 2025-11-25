# Compatibility status

The current source tree has not yet established a supported cross-revision
storage compatibility baseline. Existing codec and recovery tests prove the
behavior of the formats documented in [formats.md](formats.md), but a passing
current reader is not by itself a promise that another revision can read the
same directory.

The compatibility fixture schema lives in
[`tests/golden/manifest.tsv`](../tests/golden/manifest.tsv). Each accepted row
will name immutable producer provenance, hashed emitted bytes, a semantic
expected-value sidecar, a current reader policy, and the migration rule. The
sidecar begins with `reader_test=<named receipt>` so the manifest checker can
tie the row to a checked-in reader test.

The manifest intentionally has no data rows until an owner creates the clean,
reproducible compatibility baseline. No working tree is treated as a producer
version, no fixture bytes are hand-authored, and no Storage v2 bytes are
emitted before that baseline exists. Until then, persisted-directory upgrade
or downgrade compatibility is not a product claim.

Before that cut, preflight invokes the checker in explicit schema-only mode.
At the cut, the owner removes that exception: plain `golden-manifest.sh
--check` rejects an empty manifest and verifies that every versioned format in
the format inventory has at least one checked fixture row.
