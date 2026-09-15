## Why

Headed Chrome can fetch first-party `www.cninfo.com.cn` / `data20` JSON after Wangsu started blocking Python `requests` and true headless Chrome, but it is still an opt-in test factory. Production callers keep using `attach_cninfo_access`, and that factory cannot yet choose headed Chrome, distinguish “Chrome never started” from “Chrome connected but blocked”, or keep `static.cninfo.com.cn` / injected sessions on the old stack. Without one shared mux, every future CNInfo or data20 feature would copy browser, fallback, and job-loop rules again.

The new hop exists only to evade Wangsu. It is the preferred first-party HTTP path. It does **not** replace the existing research transport, parse/write owners, or backup providers (legacy cninfo TLS/proxy inside the same factory, AkShare `*_cninfo` and other AkShare hops, efinance, exchange announcements, THS/Sina financials, baostock, tdx, and later sources). Those stay as redundancy when the new direct hop cannot return usable data.

## What Changes

- Make `attach_cninfo_access()` the only production CNInfo HTTP access entry for current and future first-party www / data20 callers. `wrap_cninfo_proxy_fallback` SHALL NOT remain a second production entry.
- Route by host/path, not by domain provider: allowlisted first-party www APIs may use headed Chrome; unallowlisted www uses the old TLS stack; `static.cninfo.com.cn`, `webapi.cninfo.com.cn`, `http`, other hosts, AkShare `*_cninfo` wrappers, and announcement attachment downloads never go through Chrome.
- Expose two **access modes** (not two browsers). Preferred `headed_chrome` is headed Chrome (`headless=false` + existing display/Xvfb). Standby / rollback `chrome_tls` is the current `curl_cffi` Chrome-TLS fingerprint plus `akshare_proxy_patch`. True Chrome `headless=true` stays forbidden.
- Unify fallback on a **process runtime**, not per attach session: if headed Chrome connected but Wangsu blocked the call, go **direct to proxy** and do not probe Chrome TLS. If headed Chrome / Xvfb never started or is unusable, use the **full old stack** (Chrome TLS, then proxy).
- Keep a drop-in session surface (`request` / `get` / `post` plus the response fields current callers already read). If `request()` runs on a live asyncio loop, hop headed-Chrome work to a worker thread so announcement `session.post` does not raise. Jobs are not rewritten.
- Add `research_config.sources.cninfo.access.preferred_mode` so operators can prefer headed Chrome or flip back to `chrome_tls`. Tests inject mode and hops on the factory and do not start a real browser.
- Do not replace shareholder / announcement / filing parse or write owners. Do not wrap AkShare `stock_hold_*_cninfo`. Do not take over `AnnouncementAttachmentRetriever`. Do not reuse `DceOfficialBrowserClient`. Do not build a project-wide browser platform. Do not grow `data_manager.py`, `scheduler/tasks.py`, `research/storage.py`, or `api/routes.py`.
- Do not migrate old providers into the mux or treat leftover AkShare / THS / Sina / baostock / tdx / exchange paths as unfinished access work. Keep them as backup providers on the existing resolver and repair order.
- After the mux lands, prove the two-layer redundancy in simulation (direct hop exhausted → existing backup provider) and run a write-free connectivity probe. Do not require a full production job as a completion gate for those tests. Official `financial_statements` / `official_structured_sources` enablement and first-job observation stay later, post-deploy work.

## Capabilities

### New Capabilities

- `cninfo-unified-access`: One production CNInfo access mux that classifies first-party www vs other CNInfo hosts, applies headed-Chrome or Chrome-TLS mode, uses the agreed runtime-scoped fallback, and stays loop-safe for attach callers. The mux is a reusable HTTP module for current and future first-party callers. Existing source routing and backup providers remain the redundancy path when that module cannot return usable data.

### Modified Capabilities

- None. `cninfo-headed-chrome-access` is not yet an archived main spec. This change consumes that access hop; it does not reopen domain parse/write requirements.

## Impact

- Production owner: `research/providers/cninfo_http.py` (`attach_cninfo_access` is the public factory).
- Headed hop: `research/providers/cninfo_headed_chrome.py` (reports outcomes only; mux owns production fallback).
- Existing callers pick up the mux by continuing to call `attach_cninfo_access`:
  - `research/providers/cninfo_announcements.py`
  - `research/providers/cninfo_shareholders.py`
  - `research/providers/official_financial_filings.py`
- Config: `research_config.sources.cninfo.access` in `config/10_research.json`, with optional `QUOTE_CNINFO_ACCESS_MODE`.
- Tests: factory-level unit tests with injected hops, plus existing `test_cninfo_http.py`, headed-Chrome hop tests, and announcement 403 tests. Subsequent tasks add simulated provider-switch tests on the existing shareholder incremental resolver and financial repair router, plus a write-free `attach()` probe. Full 06:30 / 21:45 job observation is post-deploy, not a mux completion gate.
- No scheduler job id, write owner, canonical table, AkShare wrapper, or announcement-attachment download change.
- New www/data20 business logic MUST call this factory instead of starting Chrome or copying TLS/proxy code. Operator scripts that still use raw `requests` are not migrated in this change.
