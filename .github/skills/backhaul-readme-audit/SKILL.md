---
name: backhaul-readme-audit
description: Audit and maintain the Backhaul repository README and configuration docs against the Go implementation, tests, releases, and security behavior before editing public documentation.
---

# Backhaul README audit

Use this skill for README rewrites, installation guides, transport documentation, configuration references, and release/upgrade notes in `power0matin/Backhaul`.

## Source-of-truth order

1. Current implementation in `main.go`, `config/config.go`, `cmd/`, and `internal/`.
2. Tests covering the behavior being documented.
3. CI and release workflows plus the latest published release.
4. `CHANGELOG.md`, `PERFORMANCE.md`, and the hardening roadmap.
5. Existing README text (treat it as a draft, not proof).

Never preserve a claim merely because it already appears in README.

## Required audit

1. Inspect the repository tree and identify all user-facing entry points and dedicated docs.
2. Verify CLI flags and version output from `main.go`.
3. Verify all seven supported transports from `config/config.go` and `cmd/validate.go`.
4. Verify configuration keys, defaults, role-specific fields, and transport restrictions from `config/config.go`, `cmd/defaults.go`, and `cmd/validate.go`.
5. Verify port mapping semantics from the server transport implementation before writing examples.
6. Verify security and migration notes against current implementation and tests. In particular:
   - TCP/TCPMUX data-channel HMAC challenge authentication and temporary legacy mode.
   - WSS/WSSMUX certificate requirements and `tls_verify` default.
   - Loopback defaults and authentication rules for the web monitor.
   - Loopback-only pprof.
   - Host-wide TCP tuning being opt-in.
7. Verify current supported OS/architectures and release asset names from `.goreleaser.yaml` and the latest release.
8. Verify build requirements from `go.mod` and CI workflows.
9. Check all links, badges, anchors, examples, and Markdown structure.
10. Keep the main README focused on orientation and successful first use. Put detailed configuration in `docs/configuration.md` and link it.

## Documentation safety

- Use a non-real placeholder for tokens and require the same strong, unique secret at both endpoints.
- Do not use the built-in legacy token `musix` in examples.
- Never describe TCP or plain WS as TLS-encrypted.
- Explain that `accept_udp` is only valid for the TCP server transport.
- Explain that PROXY protocol is supported only for TCP, TCPMUX, WSMUX, and WSSMUX.
- Do not imply that the manager configures Xray/V2Ray or firewall policies unless source proves it.
- Do not claim benchmark improvements beyond the limitations documented in `PERFORMANCE.md`.
- Preserve the TCP/TCPMUX server-first v0.8.1 upgrade order and warn against leaving legacy mode enabled.
- Treat omitted `skip_optz` as safe/disabled host tuning; `false` explicitly opts into host-wide sysctl changes.
- Do not document unsupported flags, environment variables, or configuration keys.

## Final validation

Before opening a PR:
- Confirm every code block uses valid syntax and consistent names.
- Parse TOML examples where tooling is available, then verify semantics against `cmd.ValidateConfig`.
- Check README anchors and all relative links.
- Confirm that no old, contradictory, or duplicated configuration advice remains.
- Run the repository CI when the change is submitted and report its actual result.
