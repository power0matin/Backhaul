# Configuration Reference

This document describes the configuration supported by the current Backhaul fork. Configuration is TOML. Unknown keys are rejected, and exactly one role must be configured in a file: `[server]` or `[client]`.

For a minimal working pair, see the [README quick start](../README.md#quick-start).

## Server and client

| Role | Required address | Purpose |
|---|---|---|
| Server | `bind_addr = "0.0.0.0:3080"` | Listens for the client's tunnel connection |
| Client | `remote_addr = "server.example.com:3080"` | Connects outbound to the server |

Both endpoints must use the same `transport` and the same strong, unique `token`.

## Supported transports

| Value | Transport | Notes |
|---|---|---|
| `tcp` | TCP | Supports optional UDP forwarding through server-side `accept_udp` |
| `tcpmux` | TCP + SMUX | Multiplexes sessions over pooled TCP connections |
| `udp` | UDP | Native UDP transport |
| `ws` | WebSocket | HTTP-based transport |
| `wss` | WebSocket over TLS | Requires a server certificate and private key |
| `wsmux` | Multiplexed WebSocket | Uses SMUX |
| `wssmux` | Multiplexed WebSocket over TLS | Requires a server certificate and private key |

The server and client transport values must match. SMUX version and buffer settings apply to `tcpmux`, `wsmux`, and `wssmux`. `mux_con` controls the server-side stream concurrency for multiplexed transports. The legacy `mux_session` field is not used by the current transport implementations and is omitted from the reference.

## Server options

| Key | Default | Description |
|---|---:|---|
| `bind_addr` | Required | Listener address in `host:port` format |
| `transport` | Required | One of the seven transports above |
| `token` | Required | Shared secret; must be identical on the paired client |
| `ports` | `[]` | Server-side forwarded listeners, using `local[=remote]` mappings |
| `accept_udp` | `false` | Enable UDP forwarding on TCP transport listeners; TCP only |
| `proxy_protocol` | `false` | Enable PROXY protocol handling; supported by TCP, TCPMUX, WSMUX and WSSMUX |
| `allow_legacy_data_connections` | `false` | Temporary TCP/TCPMUX compatibility switch for rolling upgrades |
| `keepalive_period` | `75` | TCP keepalive interval in seconds |
| `heartbeat` | `40` | Tunnel heartbeat interval in seconds |
| `channel_size` | `2048` | Bounded forwarding queue size |
| `nodelay` | `false` | Enable TCP_NODELAY where supported |
| `mux_con` | `8` | SMUX concurrency |
| `mux_version` | `1` | SMUX version; `1` or `2` |
| `mux_framesize` | `32768` | Maximum SMUX frame size in bytes |
| `mux_recievebuffer` | `4194304` | SMUX receive buffer in bytes (key spelling is intentional) |
| `mux_streambuffer` | `65536` | SMUX stream buffer in bytes |
| `udp_queue_size` | `64` | Per-flow UDP queue capacity |
| `udp_queue_limit` | `4096` | Total queued UDP packets across flows |
| `udp_max_flows` | `2048` | Maximum tracked UDP flows |
| `tls_cert` | Required for WSS/WSSMUX | Server certificate path |
| `tls_key` | Required for WSS/WSSMUX | Server private-key path |
| `skip_optz` | `true` | Skip host-wide TCP sysctl tuning; set `false` to opt in |
| `mss` | OS default | TCP maximum segment size |
| `so_rcvbuf` | OS default | TCP socket receive buffer in bytes |
| `so_sndbuf` | OS default | TCP socket send buffer in bytes |

### Port mappings

Each `ports` entry uses `local[=remote]`:

| Example | Result |
|---|---|
| `"443"` | Listen on server port 443 and forward to client port 443 |
| `"443=8443"` | Listen on server port 443 and forward to client port 8443 |
| `"4000-4005"` | Listen on server ports 4000–4005 and forward each to the same port on the client |
| `"127.0.0.1:8443=443"` | Bind server port 8443 only on 127.0.0.1 and forward to client `127.0.0.1:443` |

A port-only remote target resolves to `127.0.0.1:port` on the client. To forward to another client-side host, provide an explicit `host:port` target, for example `"443=10.0.0.5:443"`. Local listener ports must not overlap the control listener or the enabled web monitor port.

## Client options

| Key | Default | Description |
|---|---:|---|
| `remote_addr` | Required | Reachable server address in `host:port` format |
| `transport` | Required | Must match the server transport |
| `token` | Required | Same strong, unique secret configured on the server |
| `connection_pool` | `8` | Initial number of pooled connections |
| `max_pool_size` | Derived | Maximum pool size; when omitted, the larger of `connection_pool * 4` and `connection_pool + 16` |
| `retry_interval` | `3` | Reconnect interval in seconds |
| `dial_timeout` | `10` | Connection timeout in seconds |
| `keepalive_period` | `75` | TCP keepalive interval in seconds |
| `nodelay` | `false` | Enable TCP_NODELAY where supported |
| `aggressive_pool` | `false` | Enable aggressive pool management |
| `edge_ip` | Empty | Optional edge IP for WebSocket-based transports |
| `tls_verify` | `true` | Verify server TLS certificate for WSS/WSSMUX |
| `mux_version` | `1` | SMUX version; `1` or `2` |
| `mux_framesize` | `32768` | Maximum SMUX frame size in bytes |
| `mux_recievebuffer` | `4194304` | SMUX receive buffer in bytes (key spelling is intentional) |
| `mux_streambuffer` | `65536` | SMUX stream buffer in bytes |
| `skip_optz` | `true` | Skip host-wide TCP sysctl tuning; set `false` to opt in |
| `mss` | OS default | TCP maximum segment size |
| `so_rcvbuf` | OS default | TCP socket receive buffer in bytes |
| `so_sndbuf` | OS default | TCP socket send buffer in bytes |

## Monitoring and diagnostics

These options are available under either `[server]` or `[client]`.

| Key | Default | Description |
|---|---:|---|
| `web_port` | `0` (disabled) | Web monitor port |
| `web_bind_addr` | `127.0.0.1` | Monitor bind host |
| `web_username` | Empty | HTTP Basic username; set together with password |
| `web_password` | Empty | HTTP Basic password; set together with username |
| `pprof` | `false` | Enable Go profiling on loopback only |
| `sniffer` | `false` | Enable traffic usage tracking |
| `sniffer_log` | Config directory `backhaul.json` | Usage log path |
| `log_level` | `info` | Log level: panic, fatal, error, warn, info, debug or trace |

The monitor exposes `/stats` and traffic data endpoints. When binding the monitor to a non-loopback address, both username and password are required. HTTP Basic authentication does not encrypt the connection; use a TLS reverse proxy or SSH tunnel on untrusted networks.

pprof listens only on loopback: `127.0.0.1:6060` on the server and `127.0.0.1:6061` on the client.

## TLS

For `wss` and `wssmux`:

- Configure `tls_cert` and `tls_key` on the server.
- Use a certificate trusted by the client and matching the hostname in `remote_addr`.
- Client `tls_verify` defaults to `true`. Set it to `false` only for an intentional compatibility exception; do not disable verification for normal public deployments.

## Security and compatibility

- The token is mandatory, shared by both endpoints, and must not be the built-in legacy value `musix`.
- TCP/TCPMUX pooled data channels use a fresh server challenge and HMAC-SHA256 proof in v0.8.1. The token itself is not sent on pooled data connections.
- `allow_legacy_data_connections` is supported only on TCP/TCPMUX servers. It defaults to `false` and should only be enabled temporarily when upgrading older clients.
- For a rolling TCP/TCPMUX upgrade: enable legacy mode on the server, upgrade the server, upgrade all paired clients, then disable legacy mode. Never leave it enabled on a public listener.
- `skip_optz` defaults to `true`. Set it to `false` only when you explicitly intend Backhaul to change host-wide TCP sysctl values.
- Store configuration files containing tokens with restrictive permissions, for example: `chmod 600 server.toml`.

## Validation limits

Configuration is checked before startup and before a changed file replaces a healthy running generation.

| Setting | Accepted range |
|---|---|
| Listener and destination ports | 1–65535; `web_port` also accepts `0` (disabled) |
| `channel_size` | 1–1,000,000 |
| `mux_con` | 1–4,096 |
| Time intervals | 1–86,400 seconds |
| `connection_pool` / `max_pool_size` | 1–100000; max pool must be at least the initial pool |
| `mux_version` | 1 or 2 |
| `mux_framesize` | 1–65535 bytes |
| Mux receive/stream buffers | 1 byte–256 MiB; stream buffer cannot exceed receive buffer |
| `udp_queue_size` | 1–4096 |
| `udp_queue_limit` | 1–16384 |
| `udp_max_flows` | 1–65536 |

The product of `udp_queue_size * udp_max_flows` must not exceed 1,000,000. Unknown keys, unsupported transport options, invalid endpoints and colliding server listener ports are rejected.
