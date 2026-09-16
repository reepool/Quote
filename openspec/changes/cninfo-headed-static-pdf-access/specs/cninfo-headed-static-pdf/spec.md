## ADDED Requirements

### Requirement: Headed Chrome Is The Preferred Static Attachment Hop
The system SHALL use headed Chrome (`headless=false`) as the preferred hop for `https://static.cninfo.com.cn/` GET through `attach_cninfo_access` when preferred mode is `headed_chrome` and the access-layer runtime is not already sticky-preferring a fallback. Chrome TLS and `akshare_proxy_patch` SHALL remain backup hops. The mux SHALL NOT send `webapi.cninfo.com.cn` or non-https URLs through headed Chrome.

#### Scenario: Attach factory serves static through headed Chrome first
- **WHEN** a caller GETs `https://static.cninfo.com.cn/finalpage/...PDF` on an attached session
- **AND** preferred mode is `headed_chrome`
- **AND** the runtime is not sticky-preferring a static or Chrome-unavailable fallback
- **AND** headed Chrome can start and return a usable body
- **THEN** the mux SHALL issue that request through headed Chrome
- **AND** a successful response SHALL expose `access_mode` as `headed_chrome`
- **AND** the mux SHALL NOT call Chrome TLS or proxy for that request

#### Scenario: Preferred Chrome TLS still skips headed Chrome for static
- **WHEN** preferred mode or non-empty `QUOTE_CNINFO_ACCESS_MODE` is `chrome_tls`
- **AND** a caller GETs an HTTPS static CNInfo URL
- **THEN** the mux SHALL use Chrome TLS first and `akshare_proxy_patch` on HTTP 403
- **AND** it SHALL NOT start headed Chrome for that request

#### Scenario: Webapi and http stay off headed Chrome
- **WHEN** a caller requests `https://webapi.cninfo.com.cn/...` or `http://static.cninfo.com.cn/...`
- **THEN** the mux SHALL NOT start or use headed Chrome for that URL

### Requirement: Headed Hop Returns Binary-Safe Attachment Bytes
The headed-Chrome hop SHALL fetch HTTPS static CNInfo attachments using the same hop module that serves www / data20. It SHALL return raw response bytes without a UTF-8 text round-trip. Hop `success` means a definitive HTTP response that is not a Wangsu block: a usable attachment body (`%PDF-` or trusted historical HTML) **or** HTTP 404 / 410. A Wangsu block page (`403`, title/body `403 Forbidden`, or `ws-action`) SHALL be `chrome_blocked`. An HTTP 200/206 body that is empty or neither PDF nor trusted HTML SHALL be `chrome_blocked` so the mux applies backup hops. If the existing in-page text `fetch` cannot return those bytes, the hop SHALL add the minimum same-Chrome binary or Chrome-native read. A live Chrome that still cannot read the static body SHALL report `chrome_blocked`. A dead or unusable Chrome SHALL report `chrome_unavailable`. It SHALL NOT start a second browser product or import `DceOfficialBrowserClient`. It SHALL NOT run two headed Chrome processes at once; a new static runtime MAY replace a Chrome that www already stopped.

#### Scenario: PDF bytes survive the headed hop
- **WHEN** headed Chrome retrieves a static CNInfo PDF
- **THEN** the response `content` SHALL start with `%PDF-`
- **AND** the hop SHALL NOT decode the body as text and re-encode it as UTF-8

#### Scenario: Wangsu HTML is not a successful PDF
- **WHEN** headed Chrome receives HTTP 403 or a Wangsu block page for a static URL
- **THEN** the hop SHALL report `chrome_blocked`
- **AND** the mux SHALL NOT treat that page as a successful attachment

#### Scenario: Cross-origin in-page fetch failure stays in the hop
- **WHEN** in-page `fetch` from the www homepage cannot read a static body because of CORS or an opaque response
- **THEN** the hop SHALL try a same-Chrome binary or Chrome-native read
- **AND** if that also fails on a live Chrome session it SHALL report `chrome_blocked`
- **AND** if Chrome is dead or unusable it SHALL report `chrome_unavailable`
- **AND** it SHALL NOT open a second Chrome implementation or a second concurrent Chrome process

#### Scenario: Redirect policy stays host-safe
- **WHEN** a caller GETs a static URL with `allow_redirects=False`
- **THEN** the hop SHALL NOT follow a redirect off `static.cninfo.com.cn`
- **AND** `AnnouncementAttachmentRetriever` SHALL keep its approved-host check

#### Scenario: Missing document is not a Wangsu block
- **WHEN** headed Chrome receives HTTP 404 or 410 for a static URL
- **THEN** the hop SHALL report `success` with that status code
- **AND** the mux SHALL NOT treat it as `chrome_blocked` and SHALL NOT retry Chrome TLS or proxy
- **AND** `AnnouncementAttachmentRetriever` SHALL keep classifying it as a terminal `attachment_http_404` or `attachment_http_410`

#### Scenario: Trusted HTML matches the retriever signature
- **WHEN** headed Chrome returns historical announcement HTML from static CNInfo
- **THEN** a usable HTML body SHALL match the retriever signature (`<!doctype html`, `<html`, `<head`, or `<body`, or `text/html`)
- **AND** a Wangsu "403 Forbidden" HTML page SHALL NOT be treated as trusted historical HTML

### Requirement: Blocked Versus Unavailable Fallback Matches The Www Mux
When preferred mode is `headed_chrome`, a live headed session that Wangsu blocked on static SHALL fall back to `akshare_proxy_patch` only. Headed Chrome that never started or is unusable SHALL fall back to Chrome TLS then proxy. Those sticky choices SHALL NOT force first-party www onto the static proxy sticky, and a www proxy sticky SHALL NOT force static to skip headed Chrome except when Chrome is unusable for the whole runtime.

#### Scenario: Headed hop reports outcomes only
- **WHEN** the mux asks the headed hop for an HTTPS static URL
- **THEN** that hop SHALL report `success`, `chrome_blocked`, or `chrome_unavailable`
- **AND** the mux SHALL apply the production fallback
- **AND** the mux SHALL NOT use the standalone headed factory's headed-to-proxy `request()` as the production hop

#### Scenario: Static headed block goes to proxy and skips TLS
- **WHEN** headed Chrome has started and bootstrapped
- **AND** a static request is `chrome_blocked`
- **THEN** the mux SHALL retry that request through `akshare_proxy_patch`
- **AND** it SHALL NOT send Chrome TLS for that retry
- **AND** later HTTPS `static.cninfo.com.cn` requests in the same process SHALL use proxy without another headed or TLS probe first
- **AND** later first-party www requests SHALL NOT be forced onto that static proxy sticky
- **AND** a successful proxy response SHALL expose `access_mode` as `proxy_patch`

#### Scenario: Unavailable Chrome uses the TLS stack for static
- **WHEN** headed Chrome cannot start, cannot bootstrap the www homepage, or is unusable after its one dead-session restart
- **AND** a caller GETs an HTTPS static URL
- **THEN** the mux SHALL use Chrome TLS first and `akshare_proxy_patch` on HTTP 403
- **AND** the hop SHALL NOT start a second Chrome only for that PDF
- **AND** later www and static requests in that process SHALL NOT restart headed Chrome

#### Scenario: Static proxy still accepts PDF
- **WHEN** static fallback uses `akshare_proxy_patch`
- **THEN** the acceptor SHALL accept a PDF or trusted historical HTML body
- **AND** it SHALL reject a Wangsu block page and a JSON API payload

#### Scenario: Www proxy sticky does not skip static headed Chrome
- **WHEN** the runtime is already sticky-preferring www proxy after a first-party www block
- **AND** headed Chrome is not marked unusable
- **AND** a caller GETs an HTTPS static URL with preferred mode `headed_chrome`
- **THEN** the mux SHALL still try headed Chrome for that static URL
- **AND** it SHALL NOT reuse the www `_headed_fetch` early-return that skips Chrome whenever `www_sticky=proxy`
- **AND** if www fallback already stopped the shared Chrome process, static MAY start a replacement headed runtime
- **AND** it SHALL NOT keep the stopped www Chrome running in parallel
- **AND** later www requests SHALL stay on the www proxy sticky

#### Scenario: Static block does not stop www Chrome
- **WHEN** a static request is `chrome_blocked`
- **THEN** the mux SHALL set static sticky to proxy
- **AND** it SHALL NOT call `stop_headed()` and SHALL NOT set `www_sticky=proxy`
- **AND** later first-party www requests MAY still use the live headed Chrome

#### Scenario: Chrome-unavailable sticky skips headed Chrome for static
- **WHEN** the runtime has already marked headed Chrome unusable (`chrome_tls` stack)
- **AND** a caller GETs an HTTPS static URL
- **THEN** the mux SHALL skip headed Chrome
- **AND** it SHALL use Chrome TLS first and `akshare_proxy_patch` on HTTP 403

#### Scenario: Exhausted static hops surface the HTTP error
- **WHEN** headed Chrome is `chrome_blocked` and proxy raises or is rejected
- **OR** headed Chrome is unusable and Chrome TLS returns HTTP 403 and proxy raises or is rejected
- **THEN** the mux SHALL return that HTTP 403 or raise
- **AND** it SHALL NOT convert the failure into HTTP 200 with an empty or HTML-error body

#### Scenario: Static headed work stays loop-safe
- **WHEN** mux `request()` for a static URL runs on a live asyncio loop
- **THEN** the mux SHALL hop only the headed-Chrome work to a worker thread before taking the shared Chrome lock
- **AND** it SHALL use the same loop-safe path as www / data20

### Requirement: Existing Attach Callers Inherit Static Headed Access
Default CNInfo `AnnouncementAttachmentRetriever` sessions SHALL obtain HTTP access through `attach_cninfo_access`. Official-filing static GET on an attached session SHALL use the same mux order. Domain modules SHALL keep URL trust, size limits, parse, and write ownership. Injected retriever sessions SHALL remain unwrapped.

#### Scenario: Retriever default session uses the mux
- **WHEN** `AnnouncementAttachmentRetriever` downloads a `static.cninfo.com.cn` attachment and the caller did not inject a session
- **THEN** that download SHALL obtain HTTP access through `attach_cninfo_access`
- **AND** it SHALL receive headed Chrome first when preferred mode is `headed_chrome` and headed Chrome can return a usable body

#### Scenario: Official filing static GET uses the same factory
- **WHEN** official-filing code GETs `https://static.cninfo.com.cn/` through its attached session
- **THEN** that GET SHALL use the same preferred headed then backup order
- **AND** the filing parser SHALL remain the write/parse owner

#### Scenario: Injected retriever sessions stay unwrapped
- **WHEN** a test injects a fake session into `AnnouncementAttachmentRetriever`
- **THEN** the retriever SHALL use that session without wrapping it in `attach_cninfo_access`

#### Scenario: AkShare wrappers stay outside the mux
- **WHEN** research code calls AkShare `stock_hold_num_cninfo` or another `*_cninfo` wrapper
- **THEN** those calls SHALL remain on the existing AkShare hop

#### Scenario: Future static callers reuse the same factory
- **WHEN** a new research feature needs a `https://static.cninfo.com.cn/` PDF or historical HTML
- **THEN** it SHALL obtain HTTP access only through `attach_cninfo_access`
- **AND** it SHALL NOT add a second headed-Chrome factory, TLS impersonation wrapper, or proxy wrapper for that path

#### Scenario: Snapshot exposes static sticky
- **WHEN** a job calls `snapshot_cninfo_access_runtime` after static access
- **THEN** the snapshot SHALL include the static sticky choice
- **AND** it SHALL still include preferred mode, seen hops, and www sticky

### Requirement: True Headless Chrome Stays Forbidden
The headed-Chrome hop SHALL reject `headless=true`. The mux SHALL NOT treat that misconfiguration as a successful static download.

#### Scenario: headless=true is refused for static as well as www
- **WHEN** the headed-Chrome hop is constructed with `headless=true`
- **THEN** that hop SHALL raise and refuse to start
- **AND** the mux SHALL NOT send a `headless=true` Chrome session to `static.cninfo.com.cn`
