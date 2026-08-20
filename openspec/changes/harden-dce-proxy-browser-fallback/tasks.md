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
