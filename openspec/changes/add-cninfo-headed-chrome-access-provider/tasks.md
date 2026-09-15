## 1. Access Contract

- [x] 1.1 Add a CNInfo headed-Chrome access module under `research/providers/` with a session-shaped `request` / `get` / `post` surface and an explicit factory that production `attach_cninfo_access` does not call. Do not treat this module as a wholesale replacement of `attach_cninfo_access`.
- [x] 1.2 Reject `http` / `webapi` / `static` / non-www hosts and disallowed www paths (`/` is exact homepage only, not a prefix); allow `https` www data20, announcement, top-search, and disclosure HTML; refuse `headless=true` without proxy fallback; forward `params` / `data` / `json` / `headers` / `timeout`; expose `url` / `raise_for_status` / `access_mode`; and keep `close()` for this instance's Chrome and only an Xvfb it started.
- [x] 1.3 Add unit tests with a fake page/browser that cover host/path rejection (including that `/` is not a prefix), instance-scoped reuse and `close()`, credentialed in-page request (no per-URL navigation), form `data` vs JSON body, request-argument forwarding, 403/null-evaluate/exception → proxy without a `requests` TLS probe, sticky proxy after successful fallback, proxy accept of JSON and disclosure HTML, Chrome-start failure → proxy, `headless=true` hard refuse, and `access_mode` reporting.

## 2. Headed Chrome Session

- [x] 2.1 Start one instance-owned headed Chrome (`headless=false`) using the existing Chrome binary path convention, without importing `DceOfficialBrowserClient` or a process-wide singleton. Reuse a pre-existing `DISPLAY`; start Xvfb only when unset; never overwrite DCE's display or stop an Xvfb this instance did not start.
- [x] 2.2 Bootstrap `https://www.cninfo.com.cn/` and do not issue in-page API calls from a Wangsu 403 block page; serialize requests; restart the browser at most once after a dead session; treat null evaluate as Chrome failure, not an empty success; if start/bootstrap still fails, or an in-page request 403s/times out, fall back to proxy and sticky-prefer after a successful proxy fallback.
- [x] 2.3 Issue allowed www GET/POST calls as credentialed same-origin in-page requests from the homepage document, returning status, headers, body, and `access_mode`. Do not wrap AkShare `*_cninfo` functions.

## 3. Operator Probe

- [x] 3.1 Add a write-free operator script under `scripts/dev_validation/` that constructs the new provider directly and exercises homepage bootstrap, one data20 endpoint, and one announcement query.
- [x] 3.2 Ensure production research providers and scheduler modules do not import that script, and that the script does not upsert shareholder snapshots or finish an ingestion run.

## 4. Production Isolation And Delivery

- [x] 4.1 Confirm `cninfo_announcements`, `cninfo_shareholders`, and official filings still use default `attach_cninfo_access` with no headed-Chrome construction.
- [x] 4.2 Run the new unit tests plus existing `tests/unit/test_research/test_cninfo_http.py` and announcement 403 tests.
- [x] 4.3 Run the operator probe on the scheduler host when a display/Chrome environment is available and record whether data20 and announcement JSON arrived via headed Chrome or proxy fallback.
