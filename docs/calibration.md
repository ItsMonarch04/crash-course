# Simulator calibration

Calibration is optional evidence about one named machine; it never changes
the small deterministic defaults used by tests, examples, or ordinary
campaigns. Regenerate the checked-in reference with:

```sh
scripts/calibrate.sh --profile reference-local
```

The command warms each probe 16 times, splits later observations into fitting
and validation samples, writes ignored raw JSON, and publishes only the
normalized [validation CSV](../bench/results/reference-local.csv) plus the
integer-only [profile](../sim/profiles/calibrated/reference-local.toml). It
measures 4 KiB WAL append/fsync, random and sequential SST reads, fsynced
rename-plus-directory-sync publication, persistent loopback RTT, and actual
single-node `CC.REQUEST` commit latency at concurrency 1, 8, and 64. The
profile records the OS, kernel, CPU disclosure, filesystem/mount observation,
storage disclosure, build, warmup, sample counts, and exact command.

The reference below is environment `0f71829da8f641a2` (`ccdb 0.11.14`, APFS
root mount, storage not disclosed). Residual is modeled minus measured; a
negative value is underprediction. The CSV is authoritative for p50, p95,
p99, validation sample count, and tail count.

| Validation workload | Concurrency | Measured p50 | Modeled p50 | Residual | Measured p99 | Modeled p99 | Residual |
|---|---:|---:|---:|---:|---:|---:|---:|
| WAL append | 1 | 4,500 ns | 5,417 ns | +917 ns | 7,084 ns | 6,208 ns | -876 ns |
| WAL fsync | 1 | 53,084 ns | 51,958 ns | -1,126 ns | 54,458 ns | 78,541 ns | +24,083 ns |
| SST random read | 1 | 750 ns | 541 ns | -209 ns | 1,000 ns | 1,041 ns | +41 ns |
| SST sequential read | 1 | 541 ns | 541 ns | 0 ns | 1,291 ns | 1,041 ns | -250 ns |
| Atomic publish | 1 | 143,375 ns | 277,332 ns | +133,957 ns | 145,958 ns | 292,168 ns | +146,210 ns |
| Loopback RTT | 1 | 65,791 ns | 1,000,000 ns | +934,209 ns | 68,875 ns | 1,000,000 ns | +931,125 ns |
| End-to-end commit | 1 | 12,310,042 ns | 1,057,375 ns | -11,252,667 ns | 13,012,833 ns | 1,084,749 ns | -11,928,084 ns |
| End-to-end commit | 8 | 51,769,417 ns | 1,057,375 ns | -50,712,042 ns | 108,825,459 ns | 1,084,749 ns | -107,740,710 ns |
| End-to-end commit | 64 | 732,211,875 ns | 1,057,375 ns | -731,154,500 ns | 2,660,134,542 ns | 1,084,749 ns | -2,659,049,793 ns |

These residuals are deliberately not flattering: the disk buckets calibrate
operation service, while the simple simulator omits most host concurrency and
queueing costs. It does not capture page-cache warmth, scheduler noise, NIC
coalescing, TCP congestion control, or multi-tenant interference. It neither
predicts production nor supports extrapolation beyond the measured workload.
Use `--disk-profile reference-local` only to replay this named model; it is
included in canonical `RunSpec` bytes and therefore in ledger configuration
hashes.
