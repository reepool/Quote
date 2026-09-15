## 1. Mux Contract And Mode Switch

- [x] 1.1 Extend `attach_cninfo_access` into the only production mux. Resolve mode as attach argument, then non-empty `QUOTE_CNINFO_ACCESS_MODE`, then `research_config.sources.cninfo.access.preferred_mode`, then `headed_chrome`; raise on an invalid mode; accept explicit `preferred_mode` / headed / TLS / proxy injections so tests do not start Chrome.
- [x] 1.2 Route by host/path: allowlisted first-party www (including any `/data20/` path) uses the preferred mode; unallowlisted www, `static`, `webapi`, and `http` never enter headed Chrome and use the chrome_tls stack; exact `/` is not a prefix; AkShare `*_cninfo` and `AnnouncementAttachmentRetriever` stay untouched.
- [x] 1.3 Stop disabling Chrome TLS / headed policy when a caller injects a session; keep inner-session `__getattr__` forwarding; do not leave `wrap_cninfo_proxy_fallback` as a second production factory.

## 2. Split Fallback And Shared Runtime

- [x] 2.1 Make the headed hop report `success` / `chrome_blocked` / `chrome_unavailable` only. The mux applies production fallback; do not call the standalone headed-to-proxy `request()` as the production hop.
- [x] 2.2 When Chrome is connected but blocked, retry through `akshare_proxy_patch` only, sticky-prefer that choice on the shared runtime for later first-party www from every attach session, stop headed Chrome, and do not force static/webapi onto that sticky or follow with Chrome TLS / Python `requests` TLS.
- [x] 2.3 When Chrome never starts or is unusable after the one dead-session restart, use the full `chrome_tls` stack and sticky-prefer that stack on the same runtime. Treat HTTP 200 data20 logical 429 as transport success. Keep mux proxy independent of `supports_proxy_patch` / `allow_paid_proxy`. Accept JSON and `/new/disclosure/` HTML on www proxy fallback.
- [x] 2.4 Lazily share at most one headed Chrome runtime across attach sessions, serialize in-page requests, hop off a running loop before taking the lock, reuse an existing `DISPLAY`, and provide a test reset/inject hook that does not stop a DCE display.

## 3. Compatibility And Existing Callers

- [x] 3.1 Keep drop-in response fields current callers already read, including `reason`; `access_mode` is an added attribute, not a thinner replacement object.
- [x] 3.2 Leave announcement, shareholder, and official-filing parse/write owners and the `cninfo` adaptive throttle unchanged; do not import `DceOfficialBrowserClient`, rewrite announcement scan to `to_thread`, migrate operator raw-`requests` scripts, or grow `data_manager.py` / `scheduler/tasks.py` / `research/storage.py` / `api/routes.py`.
- [x] 3.3 Add `research_config.sources.cninfo.access` and document `chrome_tls` plus the env override as the rollback / standby switch.

## 4. Tests And Verification

- [x] 4.1 Add mux unit tests for preferred-mode selection, invalid mode, argument/env/config precedence, `/data20/` admission, unallowlisted www → TLS, static/webapi never headed, injected-session policy, blocked-Chrome → runtime-wide www proxy-only, unavailable-Chrome → runtime-wide TLS+proxy, logical 429 is not blocked, proxy HTML accept, shared runtime + serialization, loop-safe `request()`, `headless=true` refuse, response `reason`, and `access_mode`.
- [x] 4.2 Move `test_cninfo_http.py` off production `wrap()` if needed, invert or replace `test_attach_cninfo_access_does_not_construct_headed_chrome`, and keep headed-Chrome hop tests plus announcement 403 tests passing.
- [x] 4.3 Run the focused unit tests. If a display/Chrome environment is available, exercise `attach_cninfo_access()` against one data20 URL, one announcement POST, and one static URL without writing research snapshots.

## 5. Direct Hop Preferred; Backup Providers Stay

- [x] 5.1 Document on `attach_cninfo_access` that it is the preferred first-party hop and a reusable module, not a replacement for existing backup providers or parse/write owners.
- [x] 5.2 Add simulated shareholder incremental tests: cninfo success does not call akshare; cninfo raise or empty/incomplete scope continues to akshare on the existing resolver/registry.
- [x] 5.3 Add a simulated financial repair test: cninfo_data20 source/transport failure leaves the target unready and the existing router attempts THS/Sina. Do not add a second repair owner.
- [x] 5.4 Add a mux unit test that exhausted hops (headed blocked + proxy raise, and chrome_tls 403 + proxy raise) return HTTP 403 or raise instead of a covered HTTP 200 empty body.
- [x] 5.5 Re-run the write-free `attach_cninfo_access()` probe for one data20 GET, one announcement POST, and one static URL when a display/Chrome environment is available. Do not add a default-CI live network test. Observed 2026-09-15: data20 and announcement `headed_chrome` HTTP 200 JSON; static stayed on `chrome_tls` (sample PDF URL 404, hop correct).

## 6. Later: First Job And Official Data20 Gate

- [x] 6.1 Surface a per-job `cninfo_access` snapshot (preferred mode, seen hops, sticky, request count) on shareholder incremental and financial disclosure results and Telegram reports so an operator can observe the new hop when running those two daily jobs. Do not run the production jobs in this slice. Rollback remains `QUOTE_CNINFO_ACCESS_MODE=chrome_tls`.
- [ ] 6.2 Only after operator observation of the two daily jobs, decide whether to enable `official_structured_sources` / `sources.cninfo.financial_statements`. The 21:45 disclosure incremental already prefers `cninfo_data20` via the existing repair router and `attach_cninfo_access`; do not flip those flags just to test that job. Keep THS/Sina as backup.
- [ ] 6.3 Optional later job change: move announcement scan off the running asyncio loop. Not a mux or backup-provider task.
