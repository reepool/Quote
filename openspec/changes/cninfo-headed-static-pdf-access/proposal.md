## Why

Wangsu now blocks ordinary Python HTTP and often Chrome TLS on `static.cninfo.com.cn` announcement PDFs. XDXR path B, official-filing downloads, announcement-asset archival, and broker-risk retrieval all need those bytes. The production mux already prefers headed Chrome for first-party www / data20 and keeps Chrome TLS plus proxy as backup, but static PDFs were kept off headed Chrome. That split is the wrong framework: PDF access must use the same preferred hop and the same backup stack, through the same factory, so every current and future CNInfo caller inherits it.

## What Changes

- Prefer headed Chrome (`headless=false`) for `https://static.cninfo.com.cn/` attachment GET through `attach_cninfo_access`, the same factory www / data20 already use.
- Extend the existing headed-Chrome hop so it can return binary PDF and trusted historical HTML. Today's in-page `fetch` + `r.text()` is www-only and corrupts binary bodies; if that path cannot fetch static bytes, add the minimum binary/cross-origin capability **inside the same hop module**, not a second browser stack.
- Keep `chrome_tls` (`curl_cffi`) and `akshare_proxy_patch` as backups when headed Chrome is blocked or unusable. HTTP 404 / 410 is a finished transport result, not a reason to fail over. Do not treat proxy as the preferred PDF hop.
- Keep `AnnouncementAttachmentRetriever` as an `attach()` caller for default CNInfo sessions. Injected test sessions stay unwrapped. Official-filing `session.get` on static inherits the same order.
- Supersede `unify-cninfo-access-modes` rules that static never enters headed Chrome and that attachment downloads stay on TLS/proxy only.
- No new scheduler job, no XDXR parse/write rewrite, no AkShare `*_cninfo` wrap, no `DceOfficialBrowserClient` reuse, no project-wide download platform, and no domain logic in `data_manager.py` / `scheduler/tasks.py` / `research/storage.py` / `api/routes.py`.

## Capabilities

### New Capabilities

- `cninfo-headed-static-pdf`: Headed Chrome is the preferred hop for HTTPS static CNInfo attachments (PDF and trusted historical HTML). The mux applies the same factory, mode switch, and blocked-versus-unavailable backup as www / data20. Chrome TLS and proxy remain fallback hops. Domain retrievers and filing providers keep URL trust, size limits, and parse/write ownership.

### Modified Capabilities

- `cninfo-unified-access`: HTTPS `static.cninfo.com.cn` SHALL use the preferred headed hop, then the blocked-versus-unavailable backup. Remove the rules that static and announcement attachments stay off headed Chrome. Www / data20 routing, AkShare backups, and parse/write owners stay unchanged.

## Impact

- Production factory: `research/providers/cninfo_http.py` (`attach_cninfo_access`).
- Headed hop: `research/providers/cninfo_headed_chrome.py` (binary static fetch; still reports `success` / `chrome_blocked` / `chrome_unavailable`).
- Default CNInfo attachment sessions: `research/announcements/retrieval.py`.
- Inherited callers (no new loops): XDXR document ingest, announcement-asset archival, broker-risk retrieval, official-filing static GET.
- Tests: mux and hop injections must cover static headed success, blocked → backup, unavailable → TLS then proxy, 404 not proxied, exhausted hops, binary body integrity, sticky isolation, snapshot `static_sticky`, and `headless=true` refused.
- Operator rollback remains `QUOTE_CNINFO_ACCESS_MODE=chrome_tls` (TLS then proxy, no headed Chrome).
- Read-only live probe of one known static PDF is operator/dev verification, not a default CI network test.
- Not a W1–W9 core-file workstream. Does not change job ids, canonical tables, or financial time semantics.
