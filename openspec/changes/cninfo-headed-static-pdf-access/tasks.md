## 1. Headed Hop Admits Static And Returns Bytes

- [x] 1.1 Admit `https://static.cninfo.com.cn/` (any path) on the existing headed hop. Keep www path allowlist unchanged. Reject `http://` static and `webapi`. Do not import `DceOfficialBrowserClient`.
- [x] 1.2 Add a binary static fetch on the same hop module: raw bytes, no UTF-8 `r.text()` round-trip. Treat `%PDF-` or trusted historical HTML as a usable attachment; treat HTTP 404/410 as hop `success` with that status; treat HTTP 403 / Wangsu HTML (`403 Forbidden` or `ws-action`) and empty/unreadable HTTP 200/206 as `chrome_blocked`.
- [x] 1.3 If in-page `fetch` cannot read the static body (CORS / opaque / status `-1`), use a same-Chrome binary or Chrome-native read. A live Chrome that still cannot read the body reports `chrome_blocked`; a dead Chrome reports `chrome_unavailable`. Honor `allow_redirects=False` so the final host stays `static.cninfo.com.cn`. Cap the body at 50 MiB. Never run two headed Chrome processes at once.
- [x] 1.4 Keep `headless=true` as a construction error. Headed hop still reports only `success` / `chrome_blocked` / `chrome_unavailable`. Mux production path MUST NOT call standalone headed `request()`.

## 2. Mux Prefers Headed Chrome For Static

- [x] 2.1 Route HTTPS static through a loop-safe preferred headed path in `attach_cninfo_access` instead of TLS-first `_request_legacy`. Do not reuse `_headed_fetch` as-is (`www_sticky=proxy` early-return and `stop_headed()` are www-only). `chrome_tls` preferred mode still skips headed Chrome. Ignore `stream=True` on the headed hop and return full `content`.
- [x] 2.2 On static `chrome_blocked`, retry proxy only, set `static_sticky=proxy`, and do not probe Chrome TLS. Do not `stop_headed()` and do not set `www_sticky`. After a www block that stopped Chrome, static MAY start a new headed runtime. `www_sticky=proxy` must not skip static headed Chrome.
- [x] 2.3 On `chrome_unavailable` or runtime `www_sticky=chrome_tls`, use Chrome TLS then proxy for static and skip headed Chrome later on the same runtime. Keep the existing static proxy acceptor (PDF / historical HTML; reject Wangsu and JSON). Exhausted hops must return HTTP 403 or raise.
- [x] 2.4 Expose `static_sticky` on `snapshot_cninfo_access_runtime`. Treat static HTTP 404/410 as transport success, not `chrome_blocked`.

## 3. Callers Inherit Without New Transports

- [x] 3.1 Verify default CNInfo `AnnouncementAttachmentRetriever` sessions still call `attach_cninfo_access` and injected sessions stay unwrapped. Do not add a second PDF downloader.
- [x] 3.2 Do not rewrite XDXR / official-filing parse or write owners. Official-filing static `session.get` inherits the mux order. Do not grow `data_manager.py`, `scheduler/tasks.py`, `research/storage.py`, or `api/routes.py`.
- [x] 3.3 Update the still-active `unify-cninfo-access-modes` artifacts plus `cninfo_headed_chrome.py` / `official_announcement_acquisition.md` comments that still say static never enters headed Chrome.

## 4. Tests And Write-Free Probe

- [x] 4.1 Add injected-hop unit tests: static headed success with `%PDF-` bytes; `headless=true` refused; static `chrome_blocked` → proxy PDF and no TLS / no `stop_headed`; `chrome_unavailable` → TLS then proxy; www proxy sticky does not skip static headed; static blocked sticky does not force www; 404 is not proxied; exhausted hops stay HTTP 403; snapshot includes `static_sticky`. Rewrite current TLS-first static tests: `test_static_403_falls_back_to_proxy_pdf_not_headed`, `test_static_get_forwards_allow_redirects_to_chrome_tls`, and STATIC in `test_unallowlisted_static_webapi_http_never_use_headed` (webapi/http stay off headed).
- [x] 4.2 Keep existing retriever tests: default CNInfo session attaches; injected session is unwrapped; approved-host redirects and size limits still apply.
- [x] 4.3 Run the focused unit tests. If a display/Chrome environment is available, write-free `attach_cninfo_access()` GET of one known static PDF without writing research snapshots. Default CI MUST NOT hit the live CDN. Rollback remains `QUOTE_CNINFO_ACCESS_MODE=chrome_tls`.
