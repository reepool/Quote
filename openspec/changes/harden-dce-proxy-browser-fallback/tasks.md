## 1. Proxy Transport

- [x] 1.1 Add credential-safe acquisition of fresh `akshare_proxy_patch` proxy leases.
- [x] 1.2 Add a loopback-only DCE browser proxy forwarder supporting HTTP absolute-form requests and HTTPS CONNECT with upstream Basic authentication.

## 2. DCE Browser Routing

- [x] 2.1 Implement bounded direct-first browser routing, hard operation timeouts, and cleanup on route failure.
- [x] 2.2 Validate readiness with the requested DCE business endpoint, reuse a healthy session, rotate bounded proxy leases, and circuit-break terminal run failures.
- [x] 2.3 Emit credential-free challenge, proxy, timeout, rotation, and circuit-break metrics through the official provider.

## 3. Exchange Isolation

- [x] 3.1 Keep DCE product enrichment on the validated DCE session and remove DCE readiness from non-DCE product-page browser fallback.

## 4. Configuration And Verification

- [x] 4.1 Configure bounded direct/proxy attempts and operation timeouts for production DCE access.
- [x] 4.2 Add focused unit tests for proxy forwarding, lease rotation, business-request validation, hard timeouts, circuit breaking, safe diagnostics, and exchange isolation.
- [x] 4.3 Run targeted futures provider tests and validate the OpenSpec change.

## 5. Production Follow-up

- [x] 5.1 Recover a previously validated proxy session with one same-session retry and fresh bounded per-recovery leases under a run-wide cap.
- [x] 5.2 Classify DCE route exhaustion before nested timeout text so the open circuit is not retried.
- [x] 5.3 Preserve existing strong calendar evidence when a rolling repair probe is unresolved and expose preserved-evidence diagnostics.
- [x] 5.4 Add focused regressions, run live read-only DCE validation, and revalidate the OpenSpec change.

## 6. Sync Timeout And Proxy Expiry Follow-up

- [x] 6.1 Let the DCE browser client own its bounded request lifecycle instead of applying the generic non-cancelling 20-second sync timeout.
- [x] 6.2 Rotate HTTP 407/expired proxy authorization routes and sanitize credential-bearing upstream errors before logs and results.
- [x] 6.3 Add focused regressions, run a read-only DCE sync-path validation, and revalidate the OpenSpec change.
