## Context

`attach_cninfo_access` is already the only production CNInfo factory. Allowlisted `https://www.cninfo.com.cn` / `cninfo.com.cn` paths (homepage, `/data20/`, announcement query, topSearch, `/new/disclosure/`) prefer headed Chrome, then apply runtime-scoped fallback:

```text
chrome_blocked      → proxy only (skip Chrome TLS)
chrome_unavailable  → chrome_tls, then proxy
```

`https://static.cninfo.com.cn/` is a CNInfo URL, so it enters the mux, but it is **not** on the headed allowlist. The mux sends it through `_request_legacy`: Chrome TLS first, then proxy. Default `AnnouncementAttachmentRetriever` CNInfo sessions already call `attach()`, so XDXR / announcement-asset / broker-risk / official-filing static GETs inherit that TLS-first order.

Observed 2026-09-16 on `1225566315.PDF`:

- bare `requests` → HTTP 403, `Ws-Action: bot`
- Chrome TLS (`curl_cffi`) → HTTP 403
- mux proxy → HTTP 200, valid `%PDF-1.7`

The headed hop cannot serve this URL today:

1. `is_allowed_cninfo_headed_chrome_url` rejects `static.cninfo.com.cn`.
2. In-page `fetch` reads `r.text()` and UTF-8-encodes it, which corrupts PDF bytes.
3. In-page `fetch` from the www homepage to static is cross-origin; CORS may fail.

The user requirement is the same three-layer model as shareholder / financial www access: **headed Chrome preferred, Chrome TLS and proxy as backup, one factory, many business callers**.

## Goals / Non-Goals

**Goals:**

- One preferred static-attachment hop: headed Chrome on the existing mux/hop modules.
- Binary-safe PDF and trusted historical HTML through that hop.
- Same factory (`attach_cninfo_access`) and same mode switch (`headed_chrome` / `chrome_tls`) as www / data20.
- Same blocked-versus-unavailable backup: Chrome TLS and proxy remain fallback hops, not a second PDF stack.
- Current attach callers inherit the new order without new job loops.

**Non-Goals:**

- Do not rewrite XDXR triage, LLM path B, or canonical factor writes.
- Do not wrap AkShare `*_cninfo`.
- Do not reuse or subclass `DceOfficialBrowserClient`.
- Do not build a project-wide browser or download platform.
- Do not send `webapi.cninfo.com.cn` or `http://` URLs through headed Chrome.
- Do not grow `data_manager.py`, `scheduler/tasks.py`, `research/storage.py`, or `api/routes.py`.
- Do not enable `headless=true`.
- Do not make proxy the preferred PDF hop.
- Do not require a production XDXR write as the completion gate.

## Decisions

### 1. Same factory; static joins the preferred headed path

`attach_cninfo_access` remains the only production entry. HTTPS `static.cninfo.com.cn` SHALL use the preferred mode (default `headed_chrome`) instead of going straight to `_request_legacy`.

```text
non-CNInfo                         → inner session
webapi / http / unallowlisted www  → chrome_tls, then proxy (unchanged)
allowlisted www                    → preferred headed, then www fallback
https://static.cninfo.com.cn/      → preferred headed, then static fallback
```

Alternative considered: keep static on TLS-first because proxy already works. Rejected; the user required headed Chrome first, same framework as www / data20.

### 2. Extend the existing hop; do not start a second Chrome

Reuse the mux-owned headed runtime (homepage bootstrap on `https://www.cninfo.com.cn/`, one process Chrome, serialized in-page work, loop-safe worker thread).

Admission: `https` + host `static.cninfo.com.cn` (any path). That is the reuse surface for `/finalpage/` PDFs and later static artifacts, analogous to `/data20/` on www.

`fetch_allowlisted` MUST accept these URLs and still report only `success` / `chrome_blocked` / `chrome_unavailable`. Production mux MUST NOT call standalone headed `request()` (that helper still owns its own proxy jump).

Alternative considered: a new `CninfoPdfBrowserClient`. Rejected; that copies Chrome/Xvfb/bootstrap and violates “one hop module”.

### 3. Binary-safe fetch inside the hop, with a Chrome-native fallback

The current `_async_fetch` path (`window.fetch` + `r.text()` + UTF-8) MUST NOT be used as the PDF body path.

The hop SHALL:

1. After the existing www homepage bootstrap, try a same-Chrome fetch that returns **raw bytes** (for example `arrayBuffer` + base64, decoded in Python). Treat HTTP 403 / Wangsu HTML (`403 Forbidden` or `ws-action`) as `chrome_blocked`. Treat an empty or UTF-8-mojibake HTTP 200/206 body that is not `%PDF-` / trusted HTML as `chrome_blocked` so the mux can fail over.
2. If in-page `fetch` cannot read the body (CORS, opaque response, status `-1`), use the **same** headed Chrome to obtain bytes without leaving the hop: Chrome-native navigation or CDP response-body read. Do not open a second browser product.
3. Honor `allow_redirects=False` so `AnnouncementAttachmentRetriever` can keep host checks. If redirects are followed inside the hop, the final host MUST remain `static.cninfo.com.cn`.
4. Cap the headed body at the existing attachment limit (50 MiB). Oversized or unreadable bodies are not success; the mux may then try backup hops.
5. HTTP 404 / 410 is a successful transport response with that status, not `chrome_blocked`. Missing files MUST NOT trigger TLS or proxy. Retriever keeps terminal `attachment_http_404` / `410`.
6. Callers may pass `stream=True`. The headed hop MAY ignore streaming and return the full `content` bytes. `CninfoAccessResponse` already falls back to `.content` when `iter_content` is absent.
7. Static headed work MUST use the existing mux worker-thread hop and shared Chrome lock. Do not call Chrome on the running asyncio loop.

Alternative considered: only CDP, skip in-page fetch. Rejected as the first attempt; in-page fetch already works for www JSON and may work for static if the CDN allows it. Alternative considered: navigate away from the www homepage for every PDF and re-bootstrap. Rejected unless required; losing the www session would harm announcement/data20 callers sharing the runtime.

### 4. Fallback matches www semantics; sticky is host-class scoped

When preferred mode is `headed_chrome`:

| Headed outcome | Next hop | Later static in this process | Later www |
|---|---|---|---|
| `success` | return bytes | keep trying headed | unchanged |
| `chrome_blocked` | **proxy only** | sticky-prefer proxy; do not probe TLS | do **not** inherit this sticky |
| `chrome_unavailable` | **chrome_tls then proxy** | skip headed (share the existing “Chrome unusable → TLS stack” runtime choice) | already skips headed |

Rationale: after Wangsu blocks a live headed session, Chrome TLS on the same egress is the wrong next hop (same rule as www). If Chrome never started, TLS then proxy is the full old stack.

`chrome_tls` preferred mode (config or `QUOTE_CNINFO_ACCESS_MODE`) still skips headed for static, as it does for www.

Static proxy accept already allows PDF / trusted HTML and rejects Wangsu pages and JSON APIs. Keep that. Do not reuse the JSON-only webapi acceptor for PDF.

`www_sticky=proxy` MUST NOT skip headed Chrome for static. Do **not** reuse `_headed_fetch` as-is: that helper returns early on `www_sticky=proxy` and calls `stop_headed()` on www `chrome_blocked`. Static needs its own headed entry that:

- ignores `www_sticky=proxy`;
- treats `www_sticky=chrome_tls` / hop `headed is None` after **unavailable** as skip-headed;
- after www has stopped Chrome for a www block, MAY construct a **replacement** headed runtime for static only (never two Chrome processes at once);
- on static `chrome_blocked`, sets `static_sticky=proxy` and MUST NOT `stop_headed()` or set `www_sticky`.

Snapshot SHALL expose `static_sticky` next to `www_sticky`.

Exhausted hops (headed blocked + proxy fail, or TLS 403 + proxy fail) MUST return HTTP 403 or raise. Do not convert that into HTTP 200 empty/HTML.

Alternative considered: headed fail → always TLS then proxy. Rejected for `chrome_blocked`; it contradicts the www mux and the 2026-09-16 evidence that TLS is also 403.

This change MODIFIES the still-active `unify-cninfo-access-modes` `cninfo-unified-access` contract. Implementers MUST NOT follow that change's older "static never headed" scenarios.

### 5. Callers stay attach-only; policy stays outside the hop

- Default CNInfo `AnnouncementAttachmentRetriever` sessions already call `attach()`. This change does not add a second download transport.
- Official-filing `session.get(static)` inherits headed-first automatically.
- Retriever keeps approved hosts, redirect checks, size, pacing, and PDF/HTML signature.
- Injected retriever sessions stay unwrapped (tests).
- Injected mux sessions still apply headed / TLS / proxy to CNInfo URLs.

### 6. Tests inject hops; live probe is not CI

Unit tests MUST inject `headed_hop` / TLS / proxy and MUST NOT start Chrome/Xvfb.

Required cases: static headed success with `%PDF-` bytes; static headed never uses `headless=true`; `chrome_blocked` → proxy PDF and no TLS / no `stop_headed`; `chrome_unavailable` → TLS then proxy; www proxy sticky does not skip static headed; Chrome-unavailable sticky does skip static headed; static blocked sticky does not force www; 404 is not proxied; exhausted hops stay HTTP 403; retriever default session still attaches; injected retriever session still unwrapped; snapshot includes `static_sticky`. Rewrite the current TLS-first static tests (`test_static_403_falls_back_to_proxy_pdf_not_headed`, `test_static_get_forwards_allow_redirects_to_chrome_tls`, and STATIC in `test_unallowlisted_static_webapi_http_never_use_headed`).

Write-free `attach().get` of one known static PDF on the scheduler host is operator/dev verification. Default CI MUST NOT hit the live CDN.

## Risks / Trade-offs

- [In-page fetch CORS-blocks static] → Same Chrome CDP/navigation fallback inside the hop; if that also fails, report `chrome_blocked` or `chrome_unavailable` and use mux backup.
- [UTF-8 `r.text()` corrupts PDF] → Binary path is mandatory; tests MUST assert `%PDF-` bytes, not decoded text.
- [Large PDF blows evaluate JSON] → 50 MiB cap; prefer CDP body read for large binaries; fail over to TLS/proxy rather than hang Chrome.
- [Navigating to a PDF destroys the www page] → Prefer fetch/CDP that keeps the homepage tab; if a tab must open, close it and keep the www session.
- [Headed blocked sticky applied to www] → `static_sticky` is independent of `www_sticky`. Static block MUST NOT `stop_headed()`.
- [Copying `_headed_fetch` for static] → www `www_sticky=proxy` early-return and `stop_headed()` would skip or kill headed PDF. Static MUST have its own headed entry.
- [TLS after headed 403 poisons egress] → `chrome_blocked` skips TLS, same as www.
- [Proxy-only preferred by operators] → Not a mode. Rollback is `chrome_tls` (TLS then proxy).
- [XDXR still shows `active_pending` until next job] → Code path fix is this change; clearing 600629 is a later job run, not a mux rewrite.
- [Homepage bootstrap 403 prevents headed PDF even if a direct PDF tab would work] → Accepted. Report `chrome_unavailable` and use TLS then proxy. Do not start a second Chrome only for PDFs.
- [Two active OpenSpec changes contradict each other] → This change ships a `cninfo-unified-access` delta and updates the sibling artifacts so static is headed-first.
- [404 retried through proxy looks like a flaky success] → 404/410 is transport success, not `chrome_blocked`.

## Migration Plan

1. Extend headed-hop admission and binary static fetch. Keep reporting outcomes only.
2. Route HTTPS static through the mux preferred-headed path with static-scoped sticky.
3. Keep retriever / official-filing attach callers unchanged except for hop behavior they already inherit.
4. Add injected-hop unit tests listed above.
5. On a display/Chrome host, write-free probe one static PDF via `attach_cninfo_access()`. Do not write research snapshots.
6. Rollback: `QUOTE_CNINFO_ACCESS_MODE=chrome_tls` or `research_config.sources.cninfo.access.preferred_mode=chrome_tls`.

## Open Questions

- Whether the first successful headed static fetch on this host is in-page `arrayBuffer` or CDP/navigation is an implementation spike inside the hop, not a second architecture. The mux contract does not change.
- Surfacing `cninfo_access` on the XDXR Telegram report is optional later work and MUST NOT grow domain logic in `scheduler/tasks.py` beyond a thin display line if added.
