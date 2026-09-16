## Context

CNInfo production HTTP is already centralized, but only for the old stack:

```text
announcements / shareholders / official filings
        |
        v
attach_cninfo_access()
        |
        v
Chrome TLS (curl_cffi impersonate=chrome), then akshare_proxy_patch on HTTP 403
```

A headed-Chrome access hop already exists (`create_cninfo_headed_chrome_access`). Live probes on the scheduler host can bootstrap `https://www.cninfo.com.cn/` and fetch data20 / announcement JSON. Production does not call that factory.

Facts that block a naive swap:

1. Official filings use one session for allowlisted www (disclosure HTML, data20, topSearch) **and** `static.cninfo.com.cn` PDFs. Headed-first static is owned by `cninfo-headed-static-pdf-access`.
2. `ShareholderIncrementalSyncService.sync()` and financial incremental scan call announcement `session.post` on the running asyncio loop. Headed Chrome currently raises if `request()` runs on that loop.
3. `attach_cninfo_access(session=...)` currently sets `impersonated_request=None`, so announcements and filings skip Chrome TLS and hit Wangsu with plain `requests`.
4. The same job constructs more than one attach session. Sticky fallback that lives on one session object will not stop the next session from probing headed Chrome again.
5. `CninfoAccessResponse` is missing `reason`, which `cninfo_shareholders` reads on HTTP >= 400. A thin wrapper would break that caller.
6. `wrap_cninfo_proxy_fallback` is a second public helper. If only `attach()` becomes the mux, `wrap()` remains a production bypass.
7. `AnnouncementAttachmentRetriever` used to download `static.cninfo.com.cn` with bare `request_get` (`stream=True`, its own redirects). That bypass is a live Wangsu 403 hole (XDXR path-B PDFs). Default CNInfo sessions now attach the mux. Injected test sessions stay unwrapped. Headed-first static PDF is owned by `cninfo-headed-static-pdf-access`.
8. data20 HTTP 200 + `resultCode=429` is a domain retry in `cninfo_shareholders`, not a Wangsu block.

Wangsu behavior this design must preserve:

- Python `requests` TLS first can poison the same egress.
- True Chrome `headless=true` stays 403.
- `curl_cffi` Chrome TLS works only on a clean egress; after a bot 403 it is the wrong next hop.
- Headed Chrome (`headless=false`) is the current first hop that can open the homepage.

This is CNInfo first-party HTTP only. It is not a DCE / Ruishu / project-wide browser platform. Static PDF bytes reuse the same mux; headed-first static is owned by `cninfo-headed-static-pdf-access`.

## Goals / Non-Goals

**Goals:**

- One production factory (`attach_cninfo_access`) that current and future first-party CNInfo / data20 attach callers use.
- Classify by host/path so new `/data20/` endpoints work without a new access stack, unallowlisted www still get the old stack, and webapi/http never enter Chrome. HTTPS static headed-first is owned by `cninfo-headed-static-pdf-access`.
- Two operator-selectable access modes: `headed_chrome` (preferred while Wangsu lasts) and `chrome_tls` (standby / rollback). Proxy is a fallback hop, not a third preferred mode.
- Runtime-scoped fallback: Chrome connected but blocked → proxy only for later first-party www. Chrome never started / unusable → full old stack for later first-party www.
- Loop-safe headed calls so existing announcement code does not raise on a running asyncio loop.
- Drop-in response fields current callers already read.
- Keep parse/write owners and the existing `cninfo` adaptive throttle outside the access layer.

**Non-Goals:**

- Do not replace `CninfoShareholdersProvider`, `cninfo_announcements`, or official-filing parsers / writers.
- Do not wrap AkShare `stock_hold_num_cninfo` / `stock_hold_control_cninfo` / `stock_hold_change_cninfo`.
- Default CNInfo `AnnouncementAttachmentRetriever` sessions MUST call `attach_cninfo_access`; injected sessions stay unwrapped. Headed-first static PDF is owned by `cninfo-headed-static-pdf-access`.
- Do not migrate operator scripts that still use raw `requests` (for example `validate_cninfo_top10_shareholders_live.py`).
- Do not enable true `headless=true` Chrome, and do not describe `chrome_tls` as a headless browser.
- Do not reuse or subclass `DceOfficialBrowserClient`.
- Do not build a generic anti-bot, download, or browser runtime for other vendors.
- Do not add scheduler jobs, API routes, or new loops in `data_manager.py` / `scheduler/tasks.py` / `research/storage.py` / `api/routes.py`.
- Do not import `scripts/` from production modules.
- Do not change canonical tables, job ids, or financial time semantics.
- Static-PDF proxy accept MUST allow PDF and trusted historical HTML and MUST reject Wangsu block pages and JSON API payloads. webapi and other non-www CNInfo hosts keep the JSON-only helper.
- Do not rewrite announcement scan to `asyncio.to_thread` in this change.
- Do not delete, wrap, or migrate backup providers (AkShare `*_cninfo`, efinance, exchange announcement routes, THS/Sina, baostock, tdx, or the in-mux `chrome_tls` / proxy hops) into a second access stack.
- Do not treat leftover use of those backup providers as unfinished mux work.
- Do not enable `official_structured_sources` or `sources.cninfo.financial_statements` in this change. That is a later, bounded production switch after the mux is live.

## Decisions

### 1. One mux at `attach_cninfo_access`; `wrap()` is not a second production entry

`research/providers/cninfo_http.py` remains the only production owner. Domain providers keep calling `attach_cninfo_access(...)` and keep owning field mapping, pagination, hash, and writes.

`attach_cninfo_access` is the only production factory. `wrap_cninfo_proxy_fallback` either becomes an internal helper of that mux or a test-only seam that requires explicit fake hops (`impersonated_request`, `proxy_request`, headed hop). Callers MUST NOT use `wrap()` as “TLS+proxy without headed policy”.

The mux is session-shaped (`request` / `get` / `post`) and forwards `params`, `data`, `json`, `headers`, and `timeout`. New research features that need www or data20 MUST attach this factory. They MUST NOT start nodriver, copy TLS impersonation, or add a second proxy wrapper.

`attach_cninfo_access` MUST accept explicit test/operator injections: `preferred_mode`, headed hop, TLS function, and `proxy_request`. Unit tests MUST use those injections and MUST NOT start a real Chrome/Xvfb.

Alternative considered: inject headed Chrome only into `cninfo_shareholders`. Rejected; announcements and filings would still need a second copy.

### 2. Two access modes; proxy is fallback; headless Chrome is not a mode

| Mode | Meaning | When to use |
|---|---|---|
| `headed_chrome` | Headed Chrome (`headless=false`), homepage bootstrap, same-origin in-page fetch | Preferred while Wangsu blocks TLS / `requests` |
| `chrome_tls` | `curl_cffi` Chrome TLS fingerprint, then `akshare_proxy_patch` on HTTP 403 | Standby / rollback when headed Chrome is unnecessary or broken |
| `proxy_patch` | `akshare_proxy_patch` hop | Automatic fallback only |

True Chrome `headless=true` remains a hard construction error on the headed hop. The mux never passes that flag. It is not a rollback target and is not treated as `chrome_unavailable`.

Config lives at `config/10_research.json` → `research_config.sources.cninfo.access.preferred_mode` (`headed_chrome` \| `chrome_tls`). That is `ResearchConfig.sources["cninfo"]["access"]["preferred_mode"]`. Mode resolution is: explicit `attach_cninfo_access(preferred_mode=...)` argument, then non-empty `QUOTE_CNINFO_ACCESS_MODE`, then config, then `headed_chrome`. An explicit invalid argument/env/config value raises. Empty env is unset.

`sources.cninfo.supports_proxy_patch` is currently `false`, and job `allow_paid_proxy` only feeds source policy. The mux 403 fallback MUST stay independent of both flags. Aligning the mux to those flags would remove the existing 403 escape hatch.

Alternative considered: default `chrome_tls` until a job is flipped. Rejected; the user asked for one generic path that future attach callers get automatically, with the old stack as standby.

### 3. Host/path classification is the reuse surface

Reuse one allowlist (`is_allowed_cninfo_headed_chrome_url` or a renamed shared helper):

- `https` + `www.cninfo.com.cn` / `cninfo.com.cn`
- exact `/` for homepage bootstrap only (`/` is not a prefix)
- prefix `/data20/` — future data20 endpoints attach without a new browser stack
- exact `/new/hisAnnouncement/query`
- exact `/new/information/topSearch/query`
- prefix `/new/disclosure/` for official-filing HTML

Routing:

```text
non-CNInfo host                         → inner session only
webapi / http                           → never headed; chrome_tls (TLS then proxy)
unallowlisted https www path            → never headed; chrome_tls (TLS then proxy)
allowlisted first-party www             → preferred mode, then the fallback below
https://static.cninfo.com.cn/           → attach the mux; headed-first is
                                          cninfo-headed-static-pdf-access
```

A new `/data20/...` path is automatically allowlisted. A new www path outside the list still works through `attach()` on the old stack; headed Chrome requires one allowlist edit plus tests.

`is_cninfo_url()` stays too wide to be the headed-Chrome gate.

Official-filing PDFs and announcement attachments on `static.cninfo.com.cn` attach the mux, not bare `requests`. This change lands TLS-then-proxy and PDF proxy accept. Headed-first static PDF is owned by `cninfo-headed-static-pdf-access`. Static proxy accept MUST allow PDF and trusted historical HTML and MUST reject Wangsu block pages.

Homepage `/` is bootstrap-only. Mux MUST NOT require homepage success through a JSON-only proxy acceptor.

### 4. Mux owns fallback; headed hop only reports an outcome

The standalone headed factory today jumps **headed → proxy** on both start failure and Wangsu 403. Production mux MUST NOT call that `request()` as a black box.

The headed hop MUST report `success`, `chrome_blocked`, or `chrome_unavailable`. The mux applies production fallback from those signals.

- **`chrome_blocked`**: headed Chrome has a live bootstrapped session, and the allowlisted www call returned HTTP 403, a Wangsu block page, a null/failed evaluate, or an in-page timeout. Retry **proxy only**. Do **not** send Chrome TLS or Python `requests` TLS. Sticky-prefer proxy on the **access-layer runtime** for later `https` `www.cninfo.com.cn` / `cninfo.com.cn` requests (any path) from every attach session in the process. Then stop that headed Chrome (do not stop a DCE display). Do **not** force later `static.cninfo.com.cn` or `webapi.cninfo.com.cn` downloads onto that www proxy sticky.
- **`chrome_unavailable`**: no binary, Xvfb/DISPLAY cannot be created, nodriver missing, start/bootstrap still unusable after the hop's one restart, or a dead session remains unusable after one restart. Use the **full old stack** (Chrome TLS, then proxy on HTTP 403). Sticky-prefer that legacy stack on the **same runtime** so later www requests do not restart Chrome on every call.
- **`success`**: return the headed body.

Standalone `create_cninfo_headed_chrome_access()` may keep probe-oriented proxy fallback for hop unit tests. Production jobs and the mux-level probe MUST go through `attach_cninfo_access`.

Never probe `www.cninfo.com.cn` with plain `requests` TLS first.

HTTP 200 JSON with data20 `resultCode=429` / `resultMsg != success` is a successful transport response. The mux MUST NOT treat it as `chrome_blocked`. Domain providers keep that retry.

Proxy fallback for allowlisted www MUST accept JSON bodies and `text/html` for `/new/disclosure/` pages. Do not reuse `_accept_cninfo_proxy_response` as the only accept gate.

### 5. Injected sessions keep the policy

`attach_cninfo_access(session=...)` MUST still apply headed Chrome / Chrome TLS / proxy to CNInfo URLs. The injected session is the inner pass-through for non-CNInfo hosts and the carrier for headers/cookies, not a way to disable the access policy.

Tests that need a fake inner hop inject explicit fakes, not “any session implies no TLS”.

This fixes the current announcement/filing path that disables `impersonated_request`. Static PDFs on that same session then use TLS+proxy instead of bare `requests`.

### 6. Loop-safe headed calls without rewriting jobs

If mux `request()` sees a running asyncio loop, it MUST hop the headed-Chrome work (and only that blocking hop) to a worker thread, **then** take the shared Chrome lock in that thread. Taking the lock on the running loop and hopping while holding it will deadlock.

This hop prevents nodriver `RuntimeError`. It does **not** stop announcement `_scan_announcements()` from blocking the scheduler loop. That scan already blocks today with TLS. Chrome cold start adds about 5–8s on the first call. This change does not move that scan into `asyncio.to_thread`.

Chrome TLS and proxy hops that are already thread-safe stay in-process.

The existing `cninfo` adaptive throttle stays around announcement `_post`, outside the mux.

### 7. One shared headed Chrome runtime, serialized, resettable

The previous headed-Chrome change forbade a hidden process singleton for the **standalone** factory so pytest instances would not share cookies. This change adds a mux-owned shared runtime because one scheduler worker constructs announcement + shareholder + filing sessions.

Rules:

- Standalone `create_cninfo_headed_chrome_access()` stays instance-owned.
- `attach_cninfo_access` lazily starts **at most one** headed Chrome runtime per process and shares it among mux sessions.
- In-page requests on that runtime are serialized with one lock.
- Dead session: restart the shared runtime at most once; if it is still unusable, report `chrome_unavailable`.
- Tests reset or inject the mux runtime. One test MUST NOT observe another test's Chrome, cookies, or pages.
- Reuse an existing `DISPLAY`; start Xvfb only when unset. Do not overwrite or stop a display started by DCE.
- Do not share cookies or pages with `DceOfficialBrowserClient`.
- After runtime sticky-prefers proxy, stop headed Chrome to free memory.

### 8. Drop-in responses; observability is an attribute

Mux responses MUST keep the fields current callers already read:

- `status_code`, `url`, `text`, `content`
- `headers.get(...)` (case-insensitive Content-Type is enough)
- `json()`, `raise_for_status()`, `reason`

`access_mode` (`headed_chrome` / `chrome_tls` / `proxy_patch`) is an added attribute or equivalent, not a reason to replace those fields with `CninfoAccessResponse` as it exists today (it lacks `reason`). No new metrics framework.

Current `CninfoProxyFallbackSession.__getattr__` forwards unknown attributes to the inner session. The mux MUST keep that so callers can still touch inner session state.

## Risks / Trade-offs

- [User reads this as “headed vs headless browsers”] → Names stay `headed_chrome` / `chrome_tls`. Comments MUST say `chrome_tls` is curl_cffi TLS, not `headless=true`.
- [Headed Chrome start fails on CI] → `chrome_unavailable` → full TLS+proxy; unit tests inject fakes.
- [Wangsu later blocks headed Chrome] → Runtime sticky proxy for first-party www; no TLS probe. Operators can set `preferred_mode=chrome_tls`.
- [Per-session sticky leaks a headed probe] → Sticky and availability live on the shared runtime.
- [Mux wraps headed.request() and skips TLS] → Headed hop reports outcomes only.
- [Thin `CninfoAccessResponse` drops `reason`] → Preserve current caller fields.
- [Announcement scan blocks the scheduler loop] → Accepted; hop only prevents RuntimeError.
- [Two threads evaluate at once] → Shared runtime lock; hop before lock.
- [JSON-only proxy rejects disclosure HTML] → Www proxy accept is JSON + disclosure HTML.
- [Headed-blocked sticky applied to static PDFs] → Sticky proxy is first-party www only; static/webapi keep their own TLS pass-through.
- [Someone gates proxy on `supports_proxy_patch`] → Mux fallback stays independent.
- [GET `/` via JSON-only proxy] → Homepage is bootstrap-only.
- [Shared runtime leaks across pytest] → Reset/inject helper is mandatory in mux tests.
- [AkShare `*_cninfo` still 403] → Out of scope; those remain backup providers.
- [Attachment downloads still 403] → Default CNInfo retriever sessions attach the mux. Static never headed. If Chrome TLS and proxy both fail, surface the HTTP error.
- [Default `headed_chrome` starts Chrome on the first nightly job] → Rollback is config/env. Verify mux on the scheduler host for data20 + announcement + one static URL before relying on the default.
- [Someone reads leftover AkShare/THS as “mux incomplete”] → Those are backup providers. Subsequent work tests the switch; it does not migrate them into the mux.
- [Mux returns HTTP 200 empty JSON after hops fail] → Domain provider may mark the instrument covered and skip akshare/THS. Exhausted hops must surface as HTTP error, raise, or uncovered scope.

## Migration Plan

1. Land mux + classification + runtime fallback + loop hop + injectable factory behind `attach_cninfo_access`. Existing three callers inherit it; no job rewrite.
2. Add `research_config.sources.cninfo.access.preferred_mode` default `headed_chrome`. Document `chrome_tls` and `QUOTE_CNINFO_ACCESS_MODE`.
3. Stop using `wrap_cninfo_proxy_fallback` as a production entry. Update `test_cninfo_http.py` to the mux/test seam.
4. Invert `test_attach_cninfo_access_does_not_construct_headed_chrome`. Keep headed-Chrome hop tests for the hop.
5. Run focused unit tests plus announcement 403 tests. If a display/Chrome environment is available, call `attach_cninfo_access()` for one data20 URL, one announcement POST, and one static URL without writing research snapshots.
6. Rollback: set preferred mode or env to `chrome_tls`. That restores TLS-then-proxy without deleting headed code.
7. If headed Chrome later becomes unnecessary, keep the hop in-tree and prefer `chrome_tls`.

## Open Questions

- `access_mode` may be a wrapper attribute or a small adapter, as long as current caller fields stay readable.
- Sharing one Chrome with DCE in the same process stays closed.
- Config-driven extra www prefixes stay deferred until a real caller needs a path outside the current allowlist and `/data20/`.
- Moving announcement scan off the scheduler loop is a later job change, not this mux.

## Subsequent Work

The mux slice (sections 1–4 in `tasks.md`) is the new preferred first-party hop. Later work on this same change keeps the three-layer model:

```text
new attach() hop  (headed → chrome_tls → proxy)
        |
        v
existing domain provider  (parse / write owner unchanged)
        |
        v
existing backup providers  (akshare / efinance / THS / Sina / exchange / …)
```

Do now, without a production job:

1. Simulated shareholder incremental switch: `cninfo` raise or incomplete scope → `akshare` (and optionally `efinance`) fills remaining instruments. Primary `cninfo` success MUST NOT call the backup.
2. Simulated financial repair switch: `cninfo_data20` transport/source failure → THS/Sina fallback on the existing repair router. Do not invent a second repair owner.
3. Mux exhausted hops MUST return the HTTP error (or raise) so the domain provider can fail that candidate and the resolver can continue. Do not convert a blocked empty body into a covered success.
4. Write-free `attach_cninfo_access()` probe for one data20 GET, one announcement POST, and one static URL. This is operator/dev verification, not a default CI live test.

Do later, after the hop is on the scheduler host:

5. Operator runs `shareholder_incremental_sync` and `financial_disclosure_incremental_sync` separately (dry-run or bounded `/run`) and reads `cninfo_access`, uncovered instruments, and backup-source fields. Rollback remains `QUOTE_CNINFO_ACCESS_MODE=chrome_tls`.
6. Bounded official data20 enablement for `official_structured_sources` / `sources.cninfo.financial_statements` only after that observation. The 21:45 incremental already uses `cninfo_data20` first through the existing repair router; those flags are for official L1/shadow write, not a prerequisite for testing the daily disclosure job.
7. Optional later job change: move announcement scan off the running asyncio loop. Not a mux or backup-provider task.
