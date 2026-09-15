## Why

Wangsu CDN on `www.cninfo.com.cn` blocks Python `requests` and true headless Chrome with HTTP 403 (`Ws-Action: bot`). `curl_cffi` Chrome TLS only works when the egress is clean; the 2026-09-15 shareholder incremental run still fell back to `akshare_proxy_patch` after the first 403. Headed Chrome + Xvfb can open the homepage and then fetch data20 JSON on this host. Production still needs proxy as fallback, but current domain providers (shareholders, announcements, official filings) own both parsing and transport, so there is no isolated access provider to test or reuse.

## What Changes

- Add a standalone CNInfo **access** provider that owns one headed Chrome per instance (`headless=false`; start Xvfb only when `DISPLAY` is unset), bootstraps `https://www.cninfo.com.cn/`, and issues same-origin GET/POST for data20 and announcement endpoints.
- Keep the current domain providers and `attach_cninfo_access` Chrome-TLS-then-proxy path unchanged as the production default.
- Give the new provider a session-shaped surface (`request` / `get` / `post`, plus `params`, `data`, `json`, `headers`, `timeout`) for first-party `www.cninfo.com.cn` only. Later callers can inject it for those URLs without copying browser code. It is not a wholesale replacement of `attach_cninfo_access`: official filings still GET `static.cninfo.com.cn` PDFs on the same production session, and that pass-through must stay outside Chrome.
- Cover first-party `www.cninfo.com.cn` JSON, form, and disclosure HTML used by current research callers. This is not a transport for AkShare `stock_hold_*_cninfo` wrappers, `webapi.cninfo.com.cn`, or `static.cninfo.com.cn` PDFs.
- Leave `akshare_proxy_patch` as the access-layer fallback; sticky-prefer proxy after a successful proxy fallback, or after Chrome start/bootstrap is unusable. Do not remove proxy from production jobs in this change.
- Add an operator/dev test entry that proves homepage, one data20 JSON call, and one announcement query without writing research snapshots or changing scheduler jobs.
- Do not generalize this into a project-wide browser platform, and do not reuse `DceOfficialBrowserClient`.

## Capabilities

### New Capabilities

- `cninfo-headed-chrome-access`: Instance-owned headed Chrome access for first-party `www.cninfo.com.cn` APIs, isolated from domain providers, with sticky proxy fallback after 403 and an opt-in test path.

### Modified Capabilities

- None.

## Impact

- New research provider module under `research/providers/` plus focused unit tests and one operator live probe.
- Existing production callers stay on `research/providers/cninfo_http.py`:
  - `research/providers/cninfo_announcements.py`
  - `research/providers/cninfo_shareholders.py`
  - `research/providers/official_financial_filings.py`
- No scheduler job, API, CLI, Telegram, or canonical table change in this change.
- Reuses optional dependencies already required for DCE (`nodriver`, Xvfb/Chrome). No new paid proxy product.
- `webapi.cninfo.com.cn` and `static.cninfo.com.cn` PDF download stay on their current HTTP paths and MUST be rejected by this provider.
- Production jobs pick up nothing until a later change explicitly injects this access provider. That follow-up must keep domain parse/write owners and add the existing `cninfo` adaptive throttle around Chrome requests.
