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
