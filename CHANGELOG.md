# Changelog

## v0.8.1

### Security

- Authenticate every pooled TCP and TCPMUX data connection with a fresh
  server-generated challenge and HMAC-SHA256 proof derived from the configured
  tunnel token.
- Never transmit the configured token on pooled TCP/TCPMUX data connections.
- Keep source-IP matching as an additional connection policy; it is no longer
  the authentication mechanism.
- Reject unauthenticated pooled data connections by default.
- Add the temporary `server.allow_legacy_data_connections` option for
  controlled rolling upgrades. It defaults to `false` and must be disabled
  after all paired clients are upgraded.
- Make omitted WSS/WSSMUX `tls_verify` default to certificate verification;
  explicit `false` remains an opt-out for deliberate compatibility cases.

### Reliability and transport correctness

- Prevent stalled TCP/TCPMUX data-channel authentication from serially blocking
  the accept loop by bounding concurrent authentication workers.
- Reject unsupported combinations of `accept_udp` and `proxy_protocol`.
- Validate endpoint hostnames, transport options, timing values, mux buffers,
  server listener overlaps, and configuration keys before startup.
- Disable implicit `SO_REUSEPORT` on server listeners.
- Preserve original destinations in PROXY protocol v2 and TCP half-close
  behavior where the transport supports it.
- Correct UDP buffer sizing, zero-length datagram handling, timestamp wrap,
  and bounded flow draining.
- Keep host-wide TCP tuning opt-in rather than mutating sysctls implicitly.
- Persist usage snapshots atomically and report failed stats collection
  honestly.
- Retire excess idle WSMUX sessions after burst traffic without closing active
  streams; retain capability negotiation for mixed-version WSMUX peers.

### Operations and release

- Improve process-level CPU, RSS, heap, goroutine, thread, and file-descriptor
  telemetry in the monitor.
- Improve release artifact publication retry behavior and pin the GoReleaser
  toolchain/action.
- Keep release archives and SHA-256 checksums for Linux amd64/arm64 and Darwin
  amd64/arm64.

### Upgrade notes

For TCP/TCPMUX deployments, upgrade the server before its paired clients. To
perform a controlled rolling upgrade, temporarily set
`allow_legacy_data_connections = true` on the server while old clients remain.
Upgrade all paired clients to v0.8.1, then set the option back to `false`.
Do not leave legacy mode enabled on a public listener. New v0.8.1 clients
require the new server-side data-channel handshake.

WSS/WSSMUX deployments using self-signed or untrusted certificates must
explicitly set `tls_verify = false` on the client; the omitted default is now
`true`.
