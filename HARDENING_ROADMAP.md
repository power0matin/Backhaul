# Backhaul v0.8.1 Hardening Roadmap

This roadmap is evidence-gated. A stage is complete only after its changes pass
the stage-specific tests and the full regression gates that are practical at
that point. Reliability and bounded resource behavior take priority over
headline benchmark numbers.

## Stage 1 — Truthful production observability

- [x] Distinguish host-wide metrics from Backhaul process metrics without
  removing or renaming the v0.8.0 JSON fields.
- [x] Expose process CPU, RSS, OS threads, open file descriptors, and exact Go
  heap bytes.
- [x] Surface the existing lifecycle counters in the web monitor.
- [x] Add API/collector regression tests and validate test, race, vet, and
  supported release builds.

Exit gate: `/stats` remains backward compatible, process metrics are directly
usable for production comparisons, and validation is green.

Status: **Complete — 2026-08-08.** Validated with Go 1.26.5 using
`go test ./...`, `go test -race ./...`, and `go vet ./...`. CGO-disabled builds
also passed for Linux amd64/arm64 and Darwin amd64/arm64. The cross-build gate
used `-buildvcs=false` only because this isolated validation worktree cannot
perform Go's VCS metadata stamping; release flags are unchanged.

## Stage 2 — WSMUX session/pool lifecycle

- [ ] Stress burst growth followed by sustained low load.
- [ ] Prove whether excess sessions retire; fix lifecycle/ownership only where
  the test demonstrates a problem.
- [ ] Keep the pool bounded and preserve automatic recovery.

Exit gate: no monotonically growing session/goroutine/FD population under the
tested burst-to-idle workload.

## Stage 3 — Matched-duration v0.7.2 vs v0.8.x A/B harness

- [ ] Run identical traffic, configuration, host limits, and duration against
  an untouched v0.7.2 baseline and the candidate.
- [ ] Record time-series RSS, heap where available, goroutines, FDs, CPU,
  throughput, latency, reconnects, and failures.

Exit gate: the comparison is reproducible and does not infer missing historical
measurements.

## Stage 4 — Full transport and regression gates

- [ ] Exercise TCP, TCPMUX, UDP, WS, WSS, WSMUX, and WSSMUX.
- [ ] Run unit/integration/stress/reconnect tests, `go test -race ./...`, and
  `go vet ./...`.
- [ ] Compare performance against v0.7.2 and reject unjustified regressions.

Exit gate: all supported transports pass the exercised paths with no known race
or reproducible lifecycle leak.

## Stage 5 — Iran ↔ foreign-server canary

- [ ] Deploy to a limited production path first.
- [ ] Capture normalized 24 h, 48 h, and 72 h resource/lifecycle snapshots.
- [ ] Confirm recovery behavior during real transient network conditions.

Exit gate: production telemetry remains stable and no rollback criterion is
triggered during the canary window.

## Stage 6 — Release audit and v0.8.1 decision

- [ ] Review the final diff for compatibility, resource ownership, error
  handling, temporary code, and secret exposure.
- [ ] Update final technical documentation and measured results.
- [ ] Validate release CI and build verified release artifacts/checksums.

Exit gate: only tag/recommend v0.8.1 if the accumulated evidence supports it.
