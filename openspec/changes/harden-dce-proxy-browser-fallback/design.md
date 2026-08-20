## Context

DCE's Ruishu protection requires a real headed Chrome session, but challenge completion and egress acceptance are separate conditions. Production probes show that the server's direct egress and some proxy exits receive HTTP 412, while a headed browser through a healthy rotated proxy can call both `maxTradeDate` and `dayQuotes`. The existing client repeatedly starts direct sessions, lacks hard bounds around CDP calls, and relies on nodriver's authenticated proxy forwarding, which rejects DCE's plain-HTTP absolute-form requests.

`OfficialFuturesMarketDataProvider` is the authoritative owner for DCE calendar probes, daily contract bars, and `contractInfo` enrichment. The implementation must preserve that ownership, existing storage formats, and unresolved-date semantics.

## Goals / Non-Goals

**Goals:**

- Recover DCE official access when direct egress is risk-controlled by rotating configured `akshare_proxy_patch` proxy leases.
- Distinguish browser challenge/network failures from a verified usable DCE business session.
- Bound the elapsed time of browser work and stop repeated per-date failures within one provider run.
- Preserve safe diagnostics without logging proxy credentials.
- Prevent unrelated exchange product-page fallback from waiting on DCE readiness.

**Non-Goals:**

- Replacing the official DCE provider with an aggregator or introducing another ingestion owner.
- Changing calendar classification, market-data storage, or source provenance rules.
- Building a general proxy service for unrelated providers.

## Decisions

1. **Use direct headed Chrome first, then bounded proxy leases.** A short direct attempt preserves the cheapest route when DCE lifts the IP restriction. HTTP 412, in-page fetch failure, or a transport timeout invalidates that route and triggers up to the configured number of fresh proxy leases. Blind request-only proxy fallback is insufficient because it cannot execute the JS challenge.

2. **Bridge Chrome through a loopback-only forwarder.** The forwarder accepts HTTP absolute-form requests and HTTPS CONNECT and adds Basic authentication only when connecting to the upstream proxy. It binds to `127.0.0.1`, rejects malformed/non-proxy targets, and never logs the upstream URL or credentials. This is required because DCE serves HTTP APIs and nodriver's authenticated proxy helper handles CONNECT only.

3. **A business request establishes readiness.** `maxTradeDate` may be used only as a lightweight challenge probe. A session is considered healthy only after the requested `dayQuotes` or `contractInfo` call succeeds. The client reuses that browser/proxy session for subsequent dates, avoiding a challenge per date.

4. **Apply hard async timeouts and a session circuit breaker.** Browser start, navigation, and page evaluation are each wrapped with `asyncio.wait_for`. After direct plus bounded proxy leases fail, the client retains a sanitized terminal failure and immediately rejects later calls in that provider run. Closing the provider resets the state for the next run.

5. **Keep routing metrics at the provider boundary.** The DCE client emits credential-free events to the existing provider metric collector for challenges, leases, rotations, successful proxy sessions, failures, timeouts, and circuit-break hits. Existing calendar and sync summaries can carry these metrics without changing persistence schemas.

6. **Do not reuse the DCE client for other exchanges.** DCE `contractInfo` and DCE product pages use the validated DCE session. Generic product-page fallback uses direct HTTP/browser behavior scoped to its exchange and cannot call DCE readiness.

7. **Bound each recovery cycle separately from the whole run.** A proxy that has completed a business request proves that at least one route was usable, but that session can later stall or be challenged. The client retries one transient business failure in the same proxy session after refreshing the DCE page. If recovery still requires rotation, it receives a fresh per-recovery lease allowance while a larger run-wide cap prevents unbounded proxy acquisition. Successful business requests are paced from the previous completion time.

8. **Preserve existing strong calendar evidence on probe outages.** A rolling repair probe that fails cannot invalidate an existing `backfilled_verified` row supported by official market rows or an official closure notice. The backfill keeps that date verified, records that the current probe failed, and blocks only dates without strong evidence. Route exhaustion is classified before its nested timeout summary so outer retries cannot repeat an already-open circuit.

9. **Keep DCE timeout ownership inside the browser client.** The generic market-data source timeout cannot safely cancel a DCE request running in a single-worker executor; timing out the await leaves Chrome running and causes later dates to time out while queued behind it. DCE exchange payload calls therefore await the provider's bounded browser route lifecycle without the generic outer `wait_for`. Proxy HTTP 407/expired-authorization responses invalidate the current route immediately, rotate within the existing lease bounds, and are reduced to credential-free diagnostics before any log or result boundary.

## Risks / Trade-offs

- **[A proxy lease may be unavailable or also risk-controlled]** -> Rotate only a bounded number of leases, then keep dates unresolved and expose sanitized diagnostics.
- **[A hung browser operation may leave Chrome processes behind]** -> Stop the browser and loopback forwarder whenever an operation times out or a route is rotated.
- **[The upstream proxy may see credentials or traffic]** -> Credentials are sent only to the configured upstream proxy; logs and browser arguments contain only the local loopback endpoint.
- **[Circuit breaking may skip a later date that could succeed]** -> Scope the breaker to one provider instance/run; the next scheduled or manual run gets a fresh attempt.
- **[A previously healthy proxy can fail on the next date]** -> Retry once in the same session, then use a fresh bounded recovery allowance without exceeding the run-wide lease cap.
- **[A caller timeout cannot stop a running browser thread]** -> Do not apply the generic source timeout to DCE browser payload calls; retain hard browser operation bounds and the run-scoped circuit breaker inside the DCE client.
- **[An upstream proxy can return credentials in a 407 body]** -> Treat 407/expired authorization as a route failure and sanitize it before logging, classification evidence, or persisted result metadata.
