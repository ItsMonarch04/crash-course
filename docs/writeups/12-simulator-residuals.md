# 12 — What the simulator does not know

The checked `reference-local` calibration makes the simulator's residuals
visible. On the recorded macOS/APFS environment, modeled versus measured p50
was close for individual WAL and SST operations, but not for the composed
host. The single-client end-to-end commit residual was about −11.25 ms; at
concurrency 64 it was about −731 ms. Loopback RTT was overpredicted by roughly
0.93 ms, while atomic publication was overpredicted by roughly 0.13 ms.

Those numbers come from the reviewable validation table, not a tuning claim:

```sh
scripts/calibrate.sh --profile reference-local
```

The command publishes [the validation CSV](../../bench/results/reference-local.csv)
and [the integer profile](../../sim/profiles/calibrated/reference-local.toml).
Residual is modeled minus measured, so negative values are underprediction.
The environment, build, warmup, sample count, command, and storage disclosure
travel with the profile and its configuration hash.

The gap is useful evidence. The model assigns deterministic service time to
typed disk/network actions; it does not model host queueing, scheduler noise,
page-cache temperature, TCP congestion, NIC coalescing, firmware behavior, or
multi-tenant interference. Calibration can replay one named model and expose
where it is wrong. It cannot turn a deterministic lab into a production
latency predictor.
