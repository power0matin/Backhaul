# Backhaul v0.8.0 engineering and performance report

This report compares the v0.8.0 hardening work in this fork with the exact
upstream v0.7.2 baseline:

* upstream repository: <https://github.com/Musixal/Backhaul>
* baseline commit: `df7966f8f725837a680ea7b90bd37ea52666c277`
* comparison date: 2026-08-07
* benchmark host: Linux amd64, AMD EPYC 9V74
* baseline benchmark toolchain: Go 1.23.1, `GOMAXPROCS=4`

The host is shared and CPU affinity is not available in this environment.
Loopback throughput varies substantially with host scheduling, so throughput
results below are reported as observations rather than claimed speedups.
Allocation counts, regression reproductions, race results, and lifecycle
invariants are the stronger evidence.

## Baseline

Before changing behavior, the untouched v0.7.2 worktree passed:

```text
go build ./...       PASS
go test ./...        PASS (the repository contained no tests)
go test -race ./...  PASS (the repository contained no tests)
go vet ./...         PASS
```

There was no executable Go benchmark suite in v0.7.2; the `benchmark/`
directory contained historical result documentation/images. The v0.8.0 work
therefore adds `benchmark/benchmark_test.go`. The benchmark fixture only uses
configuration fields present in v0.7.2, so the same test source can be copied
unchanged into an untouched baseline worktree.

One reproducible setup is:

```sh
git worktree add /tmp/backhaul-v072 df7966f8f725837a680ea7b90bd37ea52666c277
cp benchmark/benchmark_test.go /tmp/backhaul-v072/benchmark/benchmark_test.go
(cd /tmp/backhaul-v072 && GOMAXPROCS=4 go test ./benchmark -run '^$' -bench . -benchmem -count 5)
GOMAXPROCS=4 go test ./benchmark -run '^$' -bench . -benchmem -count 5
```

For lower host-order bias, the fixed-work measurements in this report
alternated separate baseline and fork benchmark binaries.

## Upstream issue audit

| Report | Finding on v0.7.2 | v0.8.0 disposition |
|---|---|---|
| [#66 excessive memory](https://github.com/Musixal/Backhaul/issues/66) | Confirmed a concrete `accept_udp` cause. Every source flow allocated a `chan []byte` with capacity 100,000. A 32-flow reproduction retained 76,838,352 bytes after GC. | Per-flow queue defaults to 64 packets, a transport-wide packet budget defaults to 4096, flow count defaults to 2048, failed/finished flows are removed, and overload stays bounded. |
| [#74 gradual RSS growth](https://github.com/Musixal/Backhaul/issues/74) | The reported month-long growth was not reproduced in the available test window. Code inspection found the same 100,000-slot native-UDP flow channels plus per-packet `time.After(60s)` timer creation. | Native UDP uses bounded queues/global budget and reusable idle timers. A 30 s WSMUX soak and focused UDP churn plateaued, but this does not prove month-long RSS behavior. |
| [#62 local listener channel full](https://github.com/Musixal/Backhaul/issues/62) | Confirmed. TCP-family local acceptors used a non-blocking send and immediately closed an accepted socket when the bounded queue was momentarily full. | The queue remains bounded; a saturated producer waits up to 250 ms for the existing queue to drain, then explicitly rejects and increments an observable counter. A regression test covers a short full-queue burst. |
| [#56 token/framing behavior](https://github.com/Musixal/Backhaul/issues/56) | The exact reported “port instead of token” symptom was not deterministically reproduced. Two independent correctness defects were found in the same area: framed writes assumed one `Write` transmits the full frame, and restart mutated live transport contexts/channels while workers were still using them; the latter produced race-detector failures. | Framed writes now loop until complete and reject values that cannot fit the uint16 framing. Automatic restart creates a fresh transport generation instead of mutating live state. Do not interpret this as proof that every #56 deployment symptom had the same root cause. |
| [#43 UDP lifecycle](https://github.com/Musixal/Backhaul/issues/43) | Confirmed one concrete bug: after the native-UDP control handshake, the server never published `Connected (UDP)`. UDP copy/control ownership also depended too heavily on peer shutdown. | Status is set after authentication; both copy directions share cancellation/close ownership; control I/O is interrupted by local cancellation; reconnect after a replacement server is integration-tested. |
| [#70 WS connectivity](https://github.com/Musixal/Backhaul/issues/70) | Not reproduced. The issue did not contain enough configuration/trace detail to establish a software defect. | Local WS, WSS, WSMUX, and WSSMUX establishment and bidirectional transfer all pass integration tests. No speculative protocol change was made for this report. |
| [#59 streaming performance](https://github.com/Musixal/Backhaul/issues/59) | Not reproduced on loopback. The report describes WAN/video behavior without enough data to isolate SMUX from path loss/RTT/buffering. | No speculative SMUX tuning. Multiplexed transports are included in the identical baseline/fork throughput harness and churn tests. |
| [#63 web monitor exposure](https://github.com/Musixal/Backhaul/issues/63) | Confirmed. Enabling the web monitor bound it to all interfaces without authentication; pprof was also exposed on all interfaces. | Monitor default is `127.0.0.1`, optional Basic authentication is available, and pprof is loopback-only. Non-loopback unauthenticated HTTP monitoring emits a warning. |

## Root causes and reliability changes

### UDP memory ownership

The largest measured defect was structural rather than a garbage-collector
problem. A 100,000-entry channel stores roughly 2.4 MB of slice headers on
amd64 before any packet payloads are retained; multiplying that by source-flow
cardinality explains large RSS jumps without requiring a GC leak.

v0.8.0 introduces three independent bounds:

* `udp_queue_size`: per-flow queued packet slots (default 64);
* `udp_queue_limit`: packet slots retained across the transport (default
  4096);
* `udp_max_flows`: tracked source flows (default 2048).

New-flow overload no longer inserts an entry that could not be queued for
service. Completed congested `accept_udp` flows are removed if they still own
their map entry, without deleting a newer replacement flow for the same
source. Packet idle timeouts reuse one timer per direction rather than
creating a new one for each select iteration.

The focused 32-flow regression measured:

| Build | Live heap retained after GC |
|---|---:|
| v0.7.2 | 76,838,352 B |
| v0.8.0 recorded comparison | 3,424 B |
| v0.8.0 final recheck | 0 B above the pre-churn sample |

The final value is a before/after live-heap delta and can legitimately clamp
to zero when unrelated garbage is collected; it should not be read as “the
feature allocates zero bytes.” The important result is that the tens-of-MB
per-flow-capacity retention is gone.

### Backpressure

Local TCP queues are still fixed-capacity. When full, the accept loop now
allows a 250 ms bounded drain window. If capacity does not become available,
the new socket is closed, the cumulative rejection counter is incremented,
and warnings are rate-limited to one per five seconds. Tunnel/session queues
remain non-blocking where blocking would stall transport health.

No unbounded queue was introduced.

### Restart and shutdown

The v0.7.2 restart path canceled a transport and then rewrote fields such as
its context, control connection, channels, and counters while goroutines from
the canceled generation could still access them. This was reproduced as a
data race, including WSMUX.

Each transport generation is now immutable after cancellation: a restart
cancels/closes the old generation, waits a context-aware two-second handoff,
constructs a new transport object, and carries only runtime metric state
forward. Control sockets, WebSockets, SMUX sessions, streams, forwarding
connections, listeners, timers, and UDP copy pairs have explicit cancellation
or close ownership. Context cancellation also closes control I/O directly so
a half-open peer cannot indefinitely hold a shutdown goroutine in a blocking
read/write.

Dial retries use bounded exponential backoff with ±20% jitter inside a dial
attempt and honor cancellation. TCP/WebSocket dialing uses `DialContext`;
hostnames are resolved on each retry rather than being pinned to a
pre-resolved address.

Representative race-enabled reconnect measurements:

| Failure/replacement scenario | Recovery after replacement became available |
|---|---:|
| WSMUX initial refused connection | 0.506 s |
| Native UDP server replacement | 1.866 s |
| TCP server replacement | 2.020 s |
| WSMUX server replacement | 2.023 s |
| WSMUX client replacement | 2.922 s |

These are local-loopback observations, not WAN latency guarantees.

### Hot reload and validation

A changed TOML file is parsed, defaulted, and validated before the healthy
generation is canceled. The regression test writes an invalid replacement and
continues traffic through the existing tunnel, then writes a valid replacement
and verifies recovery.

Validation covers role selection, endpoints/ports including bracketed IPv6,
transport names, framed token length, pool/channel bounds, SMUX buffer
relationships, UDP queue/flow bounds, web credentials, and WSS certificate/key
requirements.

## Security changes

* Tunnel token comparisons hash both inputs and use constant-time comparison;
  invalid-token logs no longer print the supplied token.
* Binary string framing rejects payloads larger than 65,535 bytes and handles
  short writes.
* WS control messages are limited to 1 KiB and WS data messages to 64 KiB;
  normal Backhaul WS data frames are 16 KiB.
* Web monitoring defaults to loopback and supports HTTP Basic authentication.
  HTTP Basic is not encryption; external monitoring should use a TLS reverse
  proxy or SSH tunnel.
* pprof uses an explicit loopback-only server with shutdown tied to the
  transport context.
* Configuration loading warns when the TOML file is group/other readable.
* WSS/WSSMUX add `tls_verify`. It intentionally defaults to `false` to
  preserve v0.7.2 self-signed deployments; setting it to `true` enables
  normal CA/hostname verification. This compatibility default remains a known
  security limitation.
* Web/WS HTTP servers use header/time limits to reduce slow/malformed-input
  resource exposure.

No custom cryptography was introduced.

## Performance measurements

### UDP steady-state benchmark

Payload is 1200 bytes, loopback echo, five one-second runs, Go 1.23.1,
`GOMAXPROCS=4`. The allocation changes were consistent across runs and match
the allocation profile that motivated the changes.

| Path | Metric | v0.7.2 median | v0.8.0 median | Observed delta |
|---|---|---:|---:|---:|
| Native UDP | ns/op | 62,766 | 55,172 | -12.1% |
| Native UDP | MB/s | 19.12 | 21.75 | +13.8% |
| Native UDP | B/op | 3,712 | 3,248 | -12.5% |
| Native UDP | allocs/op | 60 | 54 | -10.0% |
| UDP over TCP | ns/op | 44,217 | 41,922 | -5.2% |
| UDP over TCP | MB/s | 27.14 | 28.62 | +5.5% |
| UDP over TCP | B/op | 3,055 | 1,543 | -49.5% |
| UDP over TCP | allocs/op | 25 | 21 | -16.0% |

Raw ns/op:

```text
native v0.7.2:   61062 63043 62766 66130 55836
native v0.8.0:   55555 49248 55172 59994 50422
accept v0.7.2:   43936 44741 44217 44480 43652
accept v0.8.0:   41073 42153 45729 41922 39614
```

The deterministic claims here are the allocation reductions. The throughput
direction is encouraging but is not promoted as a statistically isolated
speedup on this shared host.

An `alloc_space` profile before the UDP-over-TCP copy optimization attributed
47.12% (114.64 MB) to the UDP-to-TCP path and 44.04% (107.13 MB) to queue
ownership copies in the sampled workload. v0.8.0 reserves the two-byte framing
header in the same allocation that takes ownership of the datagram, removing
the second full-payload copy.

### Stream throughput

Payload is 256 KiB and each iteration writes then reads the entire payload over
one persistent connection. Four 2000-iteration runs per build were alternated
baseline/fork to reduce run-order bias:

| Transport | v0.7.2 median ns/op | v0.8.0 median ns/op | Median throughput delta |
|---|---:|---:|---:|
| TCP | 681,057 | 671,069 | +1.5% |
| TCPMUX | 722,401 | 759,867 | -4.9% |
| WS | 1,320,689 | 1,260,056 | +4.8% |
| WSMUX | 578,982 | 595,590 | -2.8% |

Raw fixed-work ns/op:

```text
TCP    v0.7.2: 699244 662870 770857 433223
TCP    v0.8.0: 677125 510723 683678 665012
TCPMUX v0.7.2: 829656 729146 715656 684420
TCPMUX v0.8.0: 893023 802747 716986 612084
WS     v0.7.2: 1387502 1116103 1324240 1317138
WS     v0.8.0: 1172293 1347818 1162294 1374858
WSMUX  v0.7.2: 549005 596065 561898 674600
WSMUX  v0.8.0: 704255 598934 592245 565026
```

Within-build spread is much larger than the median deltas and paired runs flip
direction. A separate, longer six-run WSMUX alternation (5000 iterations each)
produced the opposite sign: v0.7.2 median 780,001 ns/op versus v0.8.0 median
762,952 ns/op, about +2.2% throughput for the fork. Therefore no stream
throughput improvement or regression is claimed from this host. CPU profiles
of a temporally slower pair had the same shape and were dominated by
network/syscall work (72.27% baseline versus 74.14% fork flat samples in
`internal/runtime/syscall.Syscall6`), not a new Go hot path.

### Connection setup

A targeted seven-run TCP setup comparison (new TCP connection, five-byte
roundtrip, close) was more repeatable:

```text
v0.8.0 ns/op: 218589 196833 196024 213735 207923 209744 191216
v0.7.2 ns/op: 204091 197579 205866 190689 202469 194171 173699
```

Median setup latency is 207,923 ns for v0.8.0 versus 197,579 ns for v0.7.2,
about +5.2%. Allocations fall from 165 to 158 per operation while bytes/op are
effectively flat (~72.4 KiB). This small setup-latency cost is retained as a
documented reliability tradeoff: proxy handlers now have deterministic
cancellation/peer-close ownership instead of allowing an idle direction to
outlive its context.

Shorter five-run setup batches for TCPMUX, WS, and WSMUX were within host
variance (median deltas -4.5%, -1.2%, and -5.0% ns/op respectively) and are not
used as performance claims.

## Stress and soak evidence

The mandatory integration suite exercises all six stream transports:
TCP, TCPMUX, WS, WSS, WSMUX, WSSMUX. Native UDP and `accept_udp` have
separate bidirectional tests.

Stress coverage includes:

* all stream transports: three rounds × 16 concurrent short-lived
  connections;
* WSMUX lifecycle: 12 rounds × 24 connections with post-GC goroutine/heap
  checks;
* `accept_udp`: 64 source-flow churn;
* WSMUX initial connection refusal followed by recovery;
* server replacement for native UDP, TCP, and WSMUX;
* WSMUX client replacement;
* large bidirectional stream transfers;
* cancellation of a control channel whose peer has stopped consuming I/O.

The final 30-second WSMUX soak recorded:

| Metric | Result |
|---|---:|
| Successful connection/roundtrip batches | 340,704 |
| Failed batches | 0 |
| Goroutines sampled every 2 s | 58 initially; one bounded pool expansion to 68 at 10 s, then 68 |
| Live heap sample range | 1,414,768–2,776,792 B |
| RSS sample range | 17,993,728–23,728,128 B |
| Final RSS | 23,896,064 B |
| Shell CPU time for the test process/toolchain | user 78.757 s; sys 80.691 s |

The heap did not grow monotonically. The single goroutine step coincides with
the ten-second adaptive-pool interval under sustained load and then plateaus;
the pool itself has the configured finite ceiling. This is evidence against
the exercised lifecycle leaks, not a substitute for a multi-day production
soak. CPU time is recorded for reproducibility but has no baseline soak
counterpart, so it is not used to claim a CPU improvement.

## Test and CI coverage

New automated coverage includes:

* configuration/default/port/IPv6 validation;
* binary framing, oversized messages, short-write-safe behavior, and token
  comparison;
* resolver fuzzing;
* context-aware retry cancellation and WebSocket handshake timeout;
* idle proxy-handler cancellation;
* bounded TCP queue backpressure;
* UDP memory/flow cleanup and native-UDP status/control cancellation;
* web authentication and runtime metrics;
* fail-safe invalid hot reload followed by valid replacement;
* real local integration for TCP, TCPMUX, WS, WSS, WSMUX, WSSMUX, UDP, and
  UDP-over-TCP;
* concurrent churn, reconnect/failure injection, and an opt-in soak test.

CI runs build, tests, and vet on the two Go release lines supported at the time
of this report (Go 1.25.12 and Go 1.26.5), race detection on Go 1.26.5, and
CGO-disabled Linux amd64/arm64 builds. Tests use local listeners and generated
ephemeral TLS certificates; mandatory CI does not depend on external network
services.

## Compatibility

Transport names and existing v0.7.2 configuration keys are retained. Binary
token/address framing remains uint16-length-prefixed and no deliberate wire
version change was introduced.

Intentional operational differences:

1. An enabled web monitor with no `web_bind_addr` now binds to
   `127.0.0.1`, not every interface. Set `web_bind_addr = "0.0.0.0"` when
   remote binding is intentional.
2. Configurations with malformed/unsafe values now fail validation rather than
   failing later in worker goroutines.
3. Adaptive pool growth now has a finite ceiling. Existing configs that omit
   `max_pool_size` receive a derived ceiling while retaining adaptive growth.
4. WSS certificate verification remains disabled by default for v0.7.2
   compatibility. Set `tls_verify = true` for trusted certificates.

## Known limits of the evidence

* A 30-second local soak cannot prove the absence of the month-long RSS pattern
  in #74. Production canaries should be monitored over days before broad
  rollout.
* Iran-to-foreign-server packet loss, asymmetric routing, filtering, CDN
  behavior, and real WAN RTT were not reproducible in this local environment.
* DNS is re-resolved per retry by implementation and cancellation is tested,
  but a deterministic DNS-failure-then-recovery integration test was not added
  because the standard resolver is not injected and CI must not mutate host
  DNS state.
* The exact external symptom in #56 and the reports in #59/#70 were not
  conclusively reproduced; the fixes/coverage above should not be presented as
  proof that those reports had a single Backhaul root cause.
* Stream throughput and CPU measurements on the shared host have enough
  variance that only “no stable directional difference observed” is justified.
* WSS/WSSMUX `tls_verify = false` is a compatibility default, not a secure
  recommendation.

## Re-running the final checks

```sh
go build ./...
go test ./...
go test -race ./...
go vet ./...
go test ./internal/utils/network -run '^$' -fuzz FuzzResolveRemoteAddr -fuzztime 10s
BACKHAUL_SOAK=1 BACKHAUL_SOAK_DURATION=30s go test ./integration -run TestWSMUXSoak -count=1 -v
GOMAXPROCS=4 go test ./benchmark -run '^$' -bench . -benchmem -count 5
```

For baseline comparison, run the same benchmark test source at
`df7966f8f725837a680ea7b90bd37ea52666c277`. Do not compare the fork against
a different upstream revision or a different benchmark payload.
