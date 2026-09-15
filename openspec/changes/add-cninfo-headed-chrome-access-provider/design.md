## Context

CNInfo production access today is a session wrapper, not a reusable provider:

```text
shareholder_incremental_sync / financial filings / announcement scan
        |
        v
domain provider (parse + snapshot / announcement records)
        |
        v
attach_cninfo_access → Chrome TLS, then akshare_proxy_patch on HTTP 403
```

`CninfoShareholdersProvider`, `_CninfoTransport`, and official filings already call `attach_cninfo_access`. They own field mapping, pagination, hash, and writes. They must stay the production default.

Live probes on this host show:

- `requests` and true `headless=True` Chrome → Wangsu 403
- `curl_cffi` Chrome TLS → 403 when the egress is scored
- headed Chrome + Xvfb → homepage title `巨潮资讯网`, then data20 JSON `code=200`
- navigating each API URL works; `page.evaluate` of an async `fetch` currently returns `null` in nodriver and must be solved inside this provider, not by per-URL navigation

`DceOfficialBrowserClient` solves Dalian Ruishu tokens. CNInfo has no Ruishu challenge. Reusing that class would mix two WAFs and pull futures config into research HTTP.

## Goals / Non-Goals

**Goals:**

- Ship one CNInfo **access** provider that other domain providers can later inject like a session.
- Reuse one headed Chrome per provider instance: open the homepage once, then issue same-origin GET/POST for allowed `www.cninfo.com.cn` URLs.
- Keep current domain providers and `attach_cninfo_access` as the production path.
- Keep `akshare_proxy_patch` as the access-layer 403 fallback.
- Prove the provider with unit tests plus one operator live probe that does not write shareholder snapshots.

**Non-Goals:**

- Do not switch `shareholder_incremental_sync`, announcement routes, or official filings to Chrome in this change.
- Do not replace `CninfoShareholdersProvider`, `cninfo_announcements`, or official filing parsers.
- Do not build a generic browser, download, or anti-bot platform.
- Do not route `webapi.cninfo.com.cn` or `static.cninfo.com.cn` PDF bytes through Chrome.
- Do not add scheduler jobs, API routes, or `DataManager` / `scheduler/tasks.py` business loops.
- Do not import `scripts/` from production modules.

## Decisions

### Split access from domain providers

The new type is an access/session provider: `request(method, url, **kwargs)` plus `get` / `post`. It MUST forward `params`, `data`, `json`, `headers`, and `timeout`. Form `data` stays `application/x-www-form-urlencoded`; `json` stays a JSON body. The response looks like `requests.Response` (`status_code`, `url`, `text`, `content`, `headers`, `json()`, `raise_for_status()`) and also exposes `access_mode` of `headed_chrome` or `proxy_patch`. It does not parse top holders, announcement filters, or filings.

This object is not a drop-in for the whole official-filings session. That session also downloads `static.cninfo.com.cn` PDFs. A later change may inject this provider only for allowed www URLs, or wrap it so static/webapi still use the current TLS/proxy pass-through. Do not replace `attach_cninfo_access` wholesale.

Alternative considered: a second shareholder provider (`cninfo_chrome`) in `ShareholderProviderRegistry`. Rejected because announcements and filings would still need their own Chrome copies, and two shareholder writers would violate single-owner rules.

### One headed Chrome per provider instance, not per symbol and not a hidden process singleton

Create Chrome when the instance first needs it. If the process already has a `DISPLAY` (for example DCE Xvfb), reuse that display and do not start a second Xvfb that overwrites `os.environ["DISPLAY"]`. If no `DISPLAY` is set, start Xvfb for this instance and stop that Xvfb on `close()`. Do not share the Chrome process, cookies, or pages with DCE. Bootstrap `https://www.cninfo.com.cn/` and do not issue in-page API calls from a Wangsu 403 block page. If start or bootstrap fails after one restart, sticky-prefer proxy. Reuse that page for later Chrome requests. When the instance is already sticky-preferring proxy, later requests MUST NOT require a live homepage. Serialize requests with a lock. Tests construct a fresh instance and call `close()`, which MUST stop this instance's Chrome and only an Xvfb this instance started.

Do not install a process-wide singleton that pytest or a second factory would accidentally share.

Restart the browser once after a dead session. If Chrome/Xvfb cannot start, or bootstrap still fails after that restart, fall back to proxy for that instance and keep using proxy.

Alternative considered: start Chrome per instrument. Rejected; cold start is 5–8s and would dominate a 172-name incremental run.

### Same-origin in-page request, not `browser.get` per API URL

After bootstrap, issue GET/POST from the homepage document (relative or same-host URL) so Wangsu sees first-party Chrome TLS. In-page calls MUST include same-origin cookies. Solve nodriver promise/`evaluate` so the caller gets status + body. Do not navigate a new document for each data20 or announcement call.

Alternative considered: `browser.get(api_url)` for every call. It worked in a probe but costs 1–2s per URL and is the wrong contract for hundreds of candidate fetches.

### Implement a CNInfo-only client; do not reuse the DCE class

New module, for example `research/providers/cninfo_headed_chrome.py`. It may reuse the same Chrome binary path convention (`QUOTE_DCE_CHROME_PATH` or `/opt/google/chrome/chrome`) and Xvfb, but must not subclass or import `DceOfficialBrowserClient`.

`headless` MUST be `false`. True headless stays 403 on this WAF. Passing `headless=true` is a hard construction error: raise and do not start, and do not treat that as a proxy-fallback start failure.

Alternative considered: extract a shared “official browser runtime” now. Rejected; DCE and CNInfo share a binary, not a challenge or request contract. Two current callers with the same semantic are not present.

### Keep Chrome TLS + proxy as the default factory

`attach_cninfo_access()` stays Chrome TLS first, proxy on 403. The new provider is constructed only by an explicit factory used by tests and the operator probe.

A later change may add `backend=headed_chrome` to that factory. This change must not flip the default.

### Proxy remains the 403 fallback on the access layer

If the in-page request returns HTTP 403, a Wangsu block page, a null/failed evaluate result, or a Chrome/timeout exception, call `request_with_akshare_proxy`. Sticky-prefer proxy after a successful proxy fallback, or after Chrome start/bootstrap is unusable. Do not send Python `requests` TLS to `www.cninfo.com.cn` first; that can poison the same egress. A null evaluate MUST NOT be returned as a successful empty `headed_chrome` body.

Do not reuse `_accept_cninfo_proxy_response` as the only accept gate. That helper requires JSON and would reject official-filing disclosure HTML. Proxy fallback MUST accept JSON bodies and `text/html` for allowed `/new/disclosure/` pages.

A successful proxy fallback still counts as a successful request. The probe must report `access_mode`, not assume Chrome served every JSON body.

### Host and path allowlist

Allow only `https` on `www.cninfo.com.cn` (and the bare `cninfo.com.cn` alias if used) for:

- exact `/` (or empty path) for homepage bootstrap — `/` is not a prefix
- prefix `/data20/`
- exact `/new/hisAnnouncement/query`
- exact `/new/information/topSearch/query`
- prefix `/new/disclosure/` for official-filing HTML manifests

Reject `http://`, `webapi.cninfo.com.cn`, `static.cninfo.com.cn`, and every non-CNInfo host before Chrome starts. Current `is_cninfo_url()` is too wide to reuse as the only gate.

### Sync facade over a dedicated browser loop

Domain callers are synchronous. Follow the DCE pattern: own a worker thread / event loop, expose blocking `request()`. Do not require announcement or shareholder code to become async in this change. A later async caller MUST NOT invoke that blocking `request()` on the running asyncio loop; use a worker thread, same as DCE.

This provider does not own the existing `cninfo` adaptive throttle. Announcement transport already admits through that throttle. A follow-up that injects Chrome into daily jobs MUST keep that admission, or Chrome will burst.

### Operator probe is opt-in and write-free

Add `scripts/dev_validation/probe_cninfo_headed_chrome_access.py` (name may vary) that:

1. constructs the provider
2. GETs one data20 company or top-10 endpoint
3. POSTs one announcement page
4. prints status, JSON-ness, and access mode
5. shuts the browser down

It must not upsert `shareholder_snapshots` or finish an ingestion run. Production modules must not import this script.

## Risks / Trade-offs

- [Headed Chrome is heavier than HTTP] → Reuse one session and in-page fetch; do not navigate per URL. Accept extra memory for the test path.
- [nodriver async evaluate currently returns null] → Treat a working in-page request as an acceptance gate; unit-test the request adapter with a fake page.
- [Browser process leaks] → Explicit close on probe exit and a `close()` on the provider; restart-once policy on death.
- [Wangsu can still block headed Chrome later] → Keep proxy fallback; do not delete TLS+proxy.
- [Future callers copy browser code] → Publish only the session factory; domain providers must not start Chrome themselves.
- [Importing official_futures from research HTTP] → Forbidden. Share binary path via config/env only.
- [Scheduler process already has a display and Chrome for DCE] → Separate Chrome process; reuse existing `DISPLAY` if present. Do not share cookies or pages with DCE, do not overwrite `DISPLAY`, and do not stop an Xvfb this instance did not start.
- [AkShare functions named `*_cninfo` are a different hop] → This provider does not wrap `stock_hold_num_cninfo` / `stock_hold_control_cninfo`. Those stay on AkShare.
- [Blocking Chrome on the scheduler event loop] → Keep the sync facade off the running loop; document the DCE-style worker-thread rule for the follow-up injection change.
- [Config location] → Chrome binary path may read `QUOTE_DCE_CHROME_PATH` or `/opt/google/chrome/chrome`. Do not add a production `10_research.json` backend switch in this change.

## Migration Plan

1. Land the access provider, unit tests, and operator probe. Default production path unchanged.
2. Run the probe on the scheduler host. Success means homepage + data20 + announcement JSON without proxy, or explicit proxy fallback after 403.
3. A follow-up change may inject the session into one domain provider behind a config flag. That change must keep parse/write owners and the existing `cninfo` adaptive throttle. It is out of scope here.
4. Rollback is deleting or disabling the new module; no job or schema rollback is required.

## Open Questions

- Whether nodriver can await `fetch` directly, or the provider must use CDP `Runtime.evaluate` with `awaitPromise`, is an implementation detail to settle in the first vertical slice.
- Sharing one Chrome between a future CNInfo caller and DCE in the same process is not required and should stay closed unless both owners agree.
- Exact `access_mode` attachment (`response.access_mode` vs a small wrapper) can be chosen in implementation as long as the probe and tests can read it.
