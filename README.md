<h1 align="center">Backhaul</h1>

<p align="center">
  <strong>Reverse tunneling for services behind NAT and restrictive firewalls.</strong>
</p>

<p align="center">
  <a href="https://github.com/power0matin/Backhaul/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/power0matin/Backhaul/ci.yml?branch=main&style=flat-square&label=CI" alt="CI status"></a>
  <a href="https://github.com/power0matin/Backhaul/releases"><img src="https://img.shields.io/github/v/release/power0matin/Backhaul?style=flat-square" alt="Latest release"></a>
  <a href="https://github.com/power0matin/Backhaul/stargazers"><img src="https://img.shields.io/github/stars/power0matin/Backhaul?style=flat-square" alt="GitHub stars"></a>
  <a href="https://github.com/power0matin/Backhaul/blob/main/LICENSE"><img src="https://img.shields.io/github/license/power0matin/Backhaul?style=flat-square" alt="License"></a>
</p>

<p align="center">
  <a href="https://matinshahabadi.ir/projects/backhaul/">Project page</a> ·
  <a href="https://github.com/power0matin/backhaul-manager">Backhaul Manager</a> ·
  <a href="https://github.com/power0matin/Backhaul/releases">Releases</a>
</p>

Backhaul creates an outbound tunnel from a client to a reachable server. The server can then expose selected ports and forward incoming connections to services available from the client, including hosts behind NAT or restrictive firewalls.

This repository is a production-focused fork of [Musixal/Backhaul](https://github.com/Musixal/Backhaul), based on upstream commit [`df7966f`](https://github.com/Musixal/Backhaul/commit/df7966f8f725837a680ea7b90bd37ea52666c277).

## Highlights

| Area | What this fork provides |
|---|---|
| Transport support | Seven transports, including TCP, UDP, WebSocket, TLS WebSocket and multiplexed variants |
| Resource limits | Bounded UDP queues and flow tracking, capped client pool growth and bounded TCP backpressure |
| Reliability | Validated configuration reloads, fresh reconnect generations and cancellation-aware retries |
| Security | HMAC-SHA256 authentication for pooled TCP/TCPMUX data channels; safer monitor and TLS defaults |
| Observability | Web monitor, process/host metrics, connection counters and optional traffic statistics |

## Transports

| Transport | Description |
|---|---|
| `tcp` | TCP tunnel; optionally supports UDP forwarding with `accept_udp` |
| `tcpmux` | TCP tunnel with SMUX multiplexing |
| `udp` | UDP transport |
| `ws` / `wss` | WebSocket / WebSocket over TLS |
| `wsmux` / `wssmux` | Multiplexed WebSocket / multiplexed WebSocket over TLS |

Use `wss` or `wssmux` when the tunnel transport itself must be protected by TLS. Plain `tcp`, `tcpmux`, `ws`, `wsmux`, and `udp` do not provide TLS encryption.

## Installation

### Guided setup

For the simplest setup, use **[Backhaul Manager](https://github.com/power0matin/backhaul-manager)**. It provides an interactive installer and manager for both tunnel endpoints, supports this fork as its recommended binary source, and includes profiles, link tests, backups, migration and health checks.

[**Install and configure a tunnel with Backhaul Manager →**](https://github.com/power0matin/backhaul-manager?utm_source=chatgpt.com)

### Prebuilt binaries

Download the matching archive from [GitHub Releases](https://github.com/power0matin/Backhaul/releases). Releases include Linux and macOS builds for `amd64` and `arm64`, plus SHA-256 checksums.

Example for Linux:

```bash
ARCH=amd64 # use arm64 on ARM64 systems

curl -fLO "https://github.com/power0matin/Backhaul/releases/latest/download/backhaul_linux_${ARCH}.tar.gz"
curl -fLO "https://github.com/power0matin/Backhaul/releases/latest/download/checksums.txt"

sha256sum --ignore-missing -c checksums.txt
tar -xzf "backhaul_linux_${ARCH}.tar.gz"

./backhaul -v
```

### Build from source

Requires Go 1.23.1 or newer.

```bash
git clone https://github.com/power0matin/Backhaul.git
cd Backhaul
go build -o backhaul .
./backhaul -v
```

## Quick start

The following example exposes port `443` on the server and forwards it to port `443` on the client. Replace the example token with the same strong, unique secret on both endpoints.

**Server — `server.toml`:**

```toml
[server]
bind_addr = "0.0.0.0:3080"
transport = "tcp"
token = "CHANGE_THIS_TO_THE_SAME_RANDOM_SECRET"
ports = ["443=443"]
```

**Client — `client.toml`:**

```toml
[client]
remote_addr = "SERVER_PUBLIC_IP:3080"
transport = "tcp"
token = "CHANGE_THIS_TO_THE_SAME_RANDOM_SECRET"
connection_pool = 8
```

Start Backhaul on each endpoint:

```bash
./backhaul -c server.toml
./backhaul -c client.toml
```

In this mapping, the server listens on port `443` and forwards traffic to port `443` on the client side. Allow the control port (`3080/tcp`) and the forwarded port (`443/tcp`) through the relevant firewalls. Restrict the control port to the client address where possible.

For all configuration keys, defaults and transport-specific restrictions, see the [Configuration Reference](docs/configuration.md).

## Security and upgrades

- Configure a strong, unique `token` on both endpoints. The token is required; the built-in legacy value `musix` is rejected.
- In v0.8.1, every pooled TCP/TCPMUX data connection proves possession of the token with a server challenge and HMAC-SHA256. The token itself is not sent on those data connections.
- For TCP/TCPMUX rolling upgrades, upgrade the **server first**, then all paired clients. Temporarily enable `allow_legacy_data_connections = true` on the server only while old clients remain, then disable it again. Never leave legacy mode enabled on a public listener.
- WSS/WSSMUX requires a certificate and private key on the server. Client certificate verification defaults to enabled; keep it enabled with a trusted certificate.
- The web monitor binds to loopback by default. Non-loopback monitor access requires both HTTP Basic credentials; use a TLS reverse proxy or SSH tunnel because Basic authentication does not encrypt traffic.
- Host-wide TCP sysctl tuning is opt-in. The default `skip_optz = true` avoids changing host settings.

See the [Configuration Reference](docs/configuration.md) for monitor, TLS, PROXY protocol, UDP and migration settings.

## Development

```bash
go build ./...
go test ./...
go test -race ./...
go vet ./...
```

The GitHub Actions workflow runs tests and vet with Go 1.25.12 and 1.26.5, race tests, and Linux `amd64`/`arm64` cross-builds.

## Project documentation

| Document | Description |
|---|---|
| [Configuration Reference](docs/configuration.md) | Configuration keys, defaults, restrictions and examples |
| [Changelog](CHANGELOG.md) | Release changes and upgrade notes |
| [Performance report](PERFORMANCE.md) | Benchmark methodology, measurements and engineering audit |
| [Hardening roadmap](HARDENING_ROADMAP.md) | Security and reliability work |
| [Backhaul Manager](https://github.com/power0matin/backhaul-manager) | Interactive installation and tunnel management |
| [Upstream project](https://github.com/Musixal/Backhaul) | Original Backhaul project |

Licensed under [AGPL-3.0](LICENSE).
