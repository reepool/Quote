## Why

DCE official endpoints now require both successful execution of the exchange JavaScript challenge and an egress IP that is not risk-controlled. The current browser path retries an unhealthy direct session for too long, cannot use authenticated proxies for DCE's plain-HTTP requests, and can also delay unrelated exchanges through a shared product-page browser fallback.

## What Changes

- Bound the direct DCE browser attempt and fall back to rotating authenticated proxy leases supplied by the configured `akshare_proxy_patch` endpoint.
- Add a loopback-only authenticated upstream proxy forwarder that supports both HTTP absolute-form requests and HTTPS CONNECT without exposing credentials in logs or browser arguments.
- Validate a DCE browser/proxy session with a real `dayQuotes` or `contractInfo` request, reuse a healthy session within the provider run, and rotate or circuit-break after bounded failures.
- Apply hard timeouts to browser startup, navigation, and in-page evaluation so a failed DCE route cannot multiply into a long per-date delay.
- Keep non-DCE product-page browser fallback independent from DCE challenge readiness.
- Report DCE challenge, proxy lease/rotation/outcome, timeout, and circuit-break diagnostics while preserving unresolved/partial results for dates that cannot be verified.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `official-futures-source-priority`: Require bounded DCE browser/proxy routing, business-request session validation, reuse, rotation, and safe diagnostics on the existing authoritative official-source path.
- `futures-official-trading-calendar-backfill`: Require DCE anti-bot failures to remain unresolved after a bounded session-level circuit breaker instead of repeating full waits per date.
- `futures-market-data`: Require DCE product enrichment to share the validated DCE business session while non-DCE product-page fallback remains independent.

## Impact

- Affects `research/providers/official_futures.py`, DCE source configuration in `config/11_futures.json`, and focused futures provider tests.
- Reuses the existing `akshare_proxy_patch` configuration and keeps `OfficialFuturesMarketDataProvider` as the sole official DCE ingestion owner.
- Does not change storage schemas, public task commands, calendar evidence rules, or fallback source provenance.
