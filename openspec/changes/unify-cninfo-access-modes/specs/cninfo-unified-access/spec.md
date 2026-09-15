## ADDED Requirements

### Requirement: Single Production Factory For CNInfo And Data20 HTTP
The system SHALL expose one production CNInfo HTTP access factory, `attach_cninfo_access`, that current and future first-party www / data20 attach callers use for session-shaped `request` / `get` / `post`. Domain providers SHALL keep parse and write ownership. `wrap_cninfo_proxy_fallback` SHALL NOT be a second production entry.

#### Scenario: Existing domain providers inherit the mux
- **WHEN** announcement, shareholder, or official-filing code constructs CNInfo access through `attach_cninfo_access`
- **THEN** that call SHALL receive the unified access mux
- **AND** those modules SHALL NOT start headed Chrome or copy TLS/proxy fallback themselves

#### Scenario: Future first-party callers reuse the same factory
- **WHEN** a new research feature needs `https://www.cninfo.com.cn/data20/` or another first-party www API
- **THEN** it SHALL obtain HTTP access only through `attach_cninfo_access`
- **AND** it SHALL NOT add a second headed-Chrome factory, TLS impersonation wrapper, or proxy wrapper for that path

#### Scenario: Wrap is not a production bypass
- **WHEN** production research code needs CNInfo HTTP access
- **THEN** it SHALL call `attach_cninfo_access`
- **AND** it SHALL NOT call `wrap_cninfo_proxy_fallback` as a standalone production factory

#### Scenario: Session arguments stay drop-in
- **WHEN** a caller invokes `get` or `post` on the attached session with `params`, `data`, `json`, `headers`, or `timeout`
- **THEN** the mux SHALL forward those arguments to the chosen hop
- **AND** form `data` SHALL be sent as `application/x-www-form-urlencoded` and `json` SHALL be sent as a JSON body

#### Scenario: Response fields stay drop-in
- **WHEN** the mux returns a response to announcement, shareholder, or official-filing code
- **THEN** that response SHALL expose `status_code`, `url`, `text`, `content`, `headers.get`, `json()`, `raise_for_status()`, and `reason`
- **AND** it MAY also expose `access_mode`
- **AND** it SHALL NOT omit `reason` solely because the body was wrapped to carry `access_mode`

#### Scenario: Factory injections keep tests off a real browser
- **WHEN** a unit test constructs `attach_cninfo_access` with an explicit `preferred_mode` and fake headed, TLS, or proxy hops
- **THEN** the mux SHALL honor those injections
- **AND** the explicit `preferred_mode` argument SHALL override env and config
- **AND** it SHALL NOT start a real Chrome process or Xvfb for that test

### Requirement: Access Modes Are Headed Chrome And Chrome TLS
The system SHALL provide exactly two preferred CNInfo access modes: `headed_chrome` and `chrome_tls`. `headed_chrome` SHALL mean headed Chrome with `headless=false`. `chrome_tls` SHALL mean `curl_cffi` Chrome TLS fingerprint plus `akshare_proxy_patch` on HTTP 403. True Chrome `headless=true` SHALL NOT be a preferred mode or rollback target.

#### Scenario: Preferred headed Chrome uses the headed hop first
- **WHEN** `research_config.sources.cninfo.access.preferred_mode` is `headed_chrome` or is unset
- **AND** `QUOTE_CNINFO_ACCESS_MODE` is unset or empty
- **AND** the caller requests an allowlisted first-party www URL
- **AND** the access-layer runtime is not sticky-preferring a fallback
- **AND** headed Chrome can start
- **THEN** the mux SHALL issue that request through headed Chrome
- **AND** a successful response SHALL expose `access_mode` as `headed_chrome`

#### Scenario: Preferred Chrome TLS skips headed Chrome
- **WHEN** `research_config.sources.cninfo.access.preferred_mode` or non-empty `QUOTE_CNINFO_ACCESS_MODE` is `chrome_tls`
- **AND** the caller requests an allowlisted first-party www URL
- **THEN** the mux SHALL use Chrome TLS first and `akshare_proxy_patch` on HTTP 403
- **AND** it SHALL NOT start headed Chrome for that request

#### Scenario: Environment override rolls back without a second stack
- **WHEN** an operator sets `QUOTE_CNINFO_ACCESS_MODE=chrome_tls`
- **THEN** the mux SHALL prefer `chrome_tls` even if config says `headed_chrome`
- **AND** it SHALL still use the same factory and domain callers

#### Scenario: Invalid preferred mode is refused
- **WHEN** the explicit `preferred_mode` argument, non-empty `QUOTE_CNINFO_ACCESS_MODE`, or config is set to a value other than `headed_chrome` or `chrome_tls`
- **THEN** `attach_cninfo_access` SHALL raise
- **AND** it SHALL NOT start headed Chrome
- **AND** it SHALL NOT silently treat the value as `headed_chrome` or `chrome_tls`

#### Scenario: True headless Chrome is refused
- **WHEN** the headed-Chrome hop is constructed with `headless=true`
- **THEN** that hop SHALL raise and refuse to start
- **AND** the mux SHALL NOT treat that misconfiguration as `chrome_unavailable`, a successful `chrome_tls` session, or a successful `headed_chrome` session
- **AND** it SHALL NOT send a `headless=true` Chrome session to `www.cninfo.com.cn`

### Requirement: Host And Path Routing Without Per-Domain Copies
The mux SHALL route by URL host and path. Allowlisted first-party www URLs MAY use headed Chrome. Unallowlisted https www paths, `static.cninfo.com.cn`, `webapi.cninfo.com.cn`, non-https URLs, and non-CNInfo hosts SHALL never use headed Chrome. Prefix `/data20/` SHALL admit future data20 endpoints to the allowlisted www hop without a new access implementation.

#### Scenario: Data20 uses the first-party www hop
- **WHEN** a caller GETs `https://www.cninfo.com.cn/data20/` on any path under that prefix
- **THEN** the mux SHALL treat the URL as allowlisted first-party www
- **AND** it SHALL apply the preferred access mode and unified fallback

#### Scenario: Static PDF stays off Chrome and uses TLS
- **WHEN** official-filing code GETs `https://static.cninfo.com.cn/` for a PDF through an attached session
- **THEN** the mux SHALL NOT send that URL to headed Chrome
- **AND** it SHALL use the Chrome-TLS pass-through and HTTP 403 proxy fallback used for other non-allowlisted CNInfo hosts
- **AND** it SHALL NOT use bare Python `requests` TLS solely because a session was injected

#### Scenario: Unallowlisted www uses the old stack
- **WHEN** a caller requests `https://www.cninfo.com.cn/new/js/app.js` or another https www path that is not exact `/`, not a `/data20/` prefix, not exact `/new/hisAnnouncement/query`, not exact `/new/information/topSearch/query`, and not a `/new/disclosure/` prefix
- **AND** the access-layer runtime is not already sticky-preferring www proxy
- **THEN** the mux SHALL NOT send that URL to headed Chrome
- **AND** it SHALL use Chrome TLS first and `akshare_proxy_patch` on HTTP 403
- **AND** exact `/` SHALL NOT be treated as a prefix that allows that path through headed Chrome

#### Scenario: Webapi and http are not headed Chrome
- **WHEN** a caller requests `http://www.cninfo.com.cn/...` or `https://webapi.cninfo.com.cn/...`
- **THEN** the mux SHALL NOT start or use headed Chrome for that URL
- **AND** it SHALL use the Chrome-TLS pass-through instead of headed Chrome

#### Scenario: AkShare CNInfo wrappers stay outside the mux
- **WHEN** shareholder valuation code calls AkShare `stock_hold_num_cninfo`, `stock_hold_control_cninfo`, or `stock_hold_change_cninfo`
- **THEN** those calls SHALL remain on the existing AkShare hop
- **AND** the mux SHALL NOT wrap those functions

#### Scenario: Announcement attachment downloads stay outside the mux
- **WHEN** `AnnouncementAttachmentRetriever` downloads a `static.cninfo.com.cn` attachment
- **THEN** that download SHALL keep using its existing `request_get` path
- **AND** this change SHALL NOT route those bytes through headed Chrome or `attach_cninfo_access`

### Requirement: Split Fallback For Blocked Versus Unavailable Chrome
When preferred mode is `headed_chrome`, the mux SHALL distinguish a live headed-Chrome session that Wangsu blocked from headed Chrome that never started or is unusable. The headed hop SHALL report `success`, `chrome_blocked`, or `chrome_unavailable` and SHALL NOT choose the production fallback. A blocked live session SHALL fall back to `akshare_proxy_patch` only. An unavailable Chrome SHALL fall back to the full `chrome_tls` stack. Those sticky choices SHALL live on the access-layer runtime and apply to later first-party www requests from every attach session in the process. The mux SHALL NOT probe `www.cninfo.com.cn` with Python `requests` TLS first.

#### Scenario: Headed hop reports outcomes only
- **WHEN** the mux asks the headed hop for an allowlisted www URL
- **THEN** that hop SHALL report `success`, `chrome_blocked`, or `chrome_unavailable`
- **AND** the mux SHALL apply the production fallback from that outcome
- **AND** the mux SHALL NOT use the standalone headed factory's headed-to-proxy `request()` as the production hop

#### Scenario: Chrome connected but Wangsu blocked goes to proxy
- **WHEN** headed Chrome has started and bootstrapped
- **AND** an allowlisted www request returns HTTP 403, a Wangsu block page, a null/failed evaluate, or an in-page timeout
- **THEN** the mux SHALL retry that request through `akshare_proxy_patch`
- **AND** it SHALL NOT send Chrome TLS or Python `requests` TLS for that retry
- **AND** after a successful proxy fallback, later `https` `www.cninfo.com.cn` / `cninfo.com.cn` requests from any attach session in the same process SHALL use proxy without another headed-Chrome or TLS probe first
- **AND** later `static.cninfo.com.cn` or `webapi.cninfo.com.cn` requests SHALL NOT be forced onto that www proxy sticky
- **AND** the runtime SHALL stop the headed Chrome process after that sticky choice
- **AND** a successful proxy response SHALL expose `access_mode` as `proxy_patch`

#### Scenario: Chrome never started uses the old TLS stack
- **WHEN** headed Chrome or Xvfb cannot start, nodriver is unavailable, or homepage bootstrap remains unusable after the headed hop's one restart
- **THEN** the mux SHALL use Chrome TLS first and `akshare_proxy_patch` on HTTP 403 for that allowlisted www request
- **AND** later first-party www requests from any attach session in the same process SHALL keep using that legacy stack without restarting headed Chrome on every call
- **AND** a successful TLS response SHALL expose `access_mode` as `chrome_tls`

#### Scenario: Chrome TLS 403 still uses proxy
- **WHEN** preferred mode is `chrome_tls`, or the runtime has already fallen back to the legacy stack
- **AND** Chrome TLS returns HTTP 403 for a first-party www CNInfo URL
- **THEN** the mux SHALL retry through `akshare_proxy_patch`
- **AND** it SHALL sticky-prefer proxy for later first-party www requests after a successful proxy fallback
- **AND** it SHALL NOT force later `static.cninfo.com.cn` PDFs onto that JSON/HTML www proxy sticky

#### Scenario: Proxy fallback accepts JSON and disclosure HTML
- **WHEN** the mux falls back to `akshare_proxy_patch` for an allowlisted JSON API or `/new/disclosure/` HTML page
- **THEN** it SHALL accept a JSON body or `text/html` respectively
- **AND** it SHALL NOT reject disclosure HTML solely because `_accept_cninfo_proxy_response` requires JSON

#### Scenario: Data20 logical 429 stays a transport success
- **WHEN** an allowlisted data20 response is HTTP 200 JSON whose body has `resultCode=429` or a non-success `resultMsg`
- **THEN** the mux SHALL treat that response as a successful transport result
- **AND** it SHALL NOT classify it as `chrome_blocked`
- **AND** it SHALL NOT sticky-prefer proxy because of that body

#### Scenario: Source-policy flags do not disable mux proxy
- **WHEN** `sources.cninfo.supports_proxy_patch` is false or a job sets `allow_paid_proxy` false
- **AND** the mux needs `akshare_proxy_patch` after HTTP 403 or `chrome_blocked`
- **THEN** the mux SHALL still perform that proxy fallback
- **AND** it SHALL NOT disable proxy solely because those source-policy flags are false

### Requirement: Injected Sessions Do Not Disable Access Policy
Attaching an existing session SHALL keep the unified CNInfo access policy for CNInfo URLs. An injected session SHALL only be the inner pass-through for non-CNInfo hosts and the carrier for caller-supplied session state.

#### Scenario: Announcement transport with an injected session still uses the mux
- **WHEN** announcement or official-filing code calls `attach_cninfo_access(session, ...)`
- **AND** the caller requests an allowlisted first-party www URL
- **THEN** the mux SHALL still apply preferred `headed_chrome` or `chrome_tls` policy
- **AND** it SHALL NOT disable Chrome TLS solely because a session object was injected

#### Scenario: Non-CNInfo URLs stay on the inner session
- **WHEN** the attached session is asked for a host that is not `cninfo.com.cn` or a `*.cninfo.com.cn` host
- **THEN** the mux SHALL delegate to the inner session
- **AND** it SHALL NOT start headed Chrome or Chrome TLS impersonation for that host
- **AND** unknown attributes SHALL still forward to that inner session

### Requirement: Headed Chrome Calls Are Safe On A Running Asyncio Loop
The mux SHALL allow synchronous `request` / `get` / `post` from a running asyncio event loop by hopping blocking headed-Chrome work to a worker thread before taking the shared Chrome lock. Domain announcement and filing loops SHALL NOT be rewritten to become async in this change.

#### Scenario: Announcement POST from a running loop does not raise
- **WHEN** a scheduler job has a running asyncio loop
- **AND** announcement transport calls `session.post` for `https://www.cninfo.com.cn/new/hisAnnouncement/query`
- **AND** preferred mode is `headed_chrome`
- **THEN** the mux SHALL complete the request without raising a running-loop error
- **AND** it SHALL hop headed-Chrome work off the running loop before acquiring the shared Chrome lock

#### Scenario: Adaptive throttle stays outside the mux
- **WHEN** announcement transport admits a POST through the existing `cninfo` adaptive throttle
- **THEN** the mux SHALL not replace or bypass that throttle
- **AND** the throttle SHALL still wrap the `session.post` call

### Requirement: Shared Headed Chrome Runtime For All Attach Callers
While preferred mode is `headed_chrome` and Chrome is usable, the access layer SHALL lazily start at most one live headed Chrome runtime per process, reuse it across `attach_cninfo_access` sessions, and serialize in-page requests on that runtime. The standalone headed factory SHALL remain instance-owned. Tests SHALL be able to reset or inject the mux runtime.

#### Scenario: Two attach sessions reuse one Chrome
- **WHEN** announcement transport and the shareholder provider each call `attach_cninfo_access` in the same process
- **AND** preferred mode is `headed_chrome` and Chrome can start
- **THEN** the access layer SHALL NOT start a second headed Chrome process for the second attach
- **AND** both sessions SHALL be able to issue allowlisted www requests

#### Scenario: In-page requests are serialized
- **WHEN** two attach sessions in the same process issue allowlisted www requests at the same time
- **AND** the shared headed Chrome runtime is in use
- **THEN** the access layer SHALL serialize those in-page requests
- **AND** it SHALL NOT evaluate two in-page fetches concurrently on the same Chrome page

#### Scenario: Dead session restarts once
- **WHEN** the shared headed Chrome runtime dies after a successful bootstrap
- **THEN** the access layer SHALL restart that runtime at most once
- **AND** if the restart still cannot serve in-page requests, it SHALL report `chrome_unavailable` and use the legacy stack

#### Scenario: Tests can isolate the runtime
- **WHEN** a unit test resets or injects the headed-Chrome mux runtime
- **THEN** that test SHALL not observe another test's live Chrome process, cookies, or pages

#### Scenario: Display ownership stays local
- **WHEN** the access layer starts headed Chrome
- **AND** `DISPLAY` is already set
- **THEN** it SHALL reuse that display and SHALL NOT overwrite a display started by DCE
- **AND** if `DISPLAY` is unset, it SHALL start Xvfb only for this access-layer runtime
- **AND** it SHALL NOT stop an Xvfb this runtime did not start

### Requirement: Domain Owners And Core Files Stay Unchanged
This change SHALL not move shareholder, announcement, or official-filing parse/write ownership, and SHALL not add business loops to `data_manager.py`, `scheduler/tasks.py`, `research/storage.py`, or `api/routes.py`.

#### Scenario: Parse and write owners stay in domain providers
- **WHEN** a mux request returns data20 JSON, announcement JSON, or disclosure HTML
- **THEN** the existing domain provider SHALL keep mapping fields and writing snapshots or filing records
- **AND** the mux SHALL NOT upsert `shareholder_snapshots` or finish an ingestion run

#### Scenario: No DCE browser reuse
- **WHEN** the access layer starts headed Chrome for CNInfo
- **THEN** it SHALL NOT import or subclass `DceOfficialBrowserClient`
- **AND** it SHALL NOT share CNInfo cookies or pages with DCE

#### Scenario: Operator scripts are not migrated
- **WHEN** an operator script still calls CNInfo with raw `requests`
- **THEN** this change SHALL NOT rewrite that script onto the mux

### Requirement: Direct Hop Is Preferred And Existing Providers Stay As Backup
The new `attach_cninfo_access` hop SHALL be the preferred first-party www / data20 HTTP path. Existing backup providers SHALL remain registered on the current source-routing and repair frameworks. The mux SHALL NOT replace AkShare, efinance, exchange announcement routes, THS/Sina, baostock, tdx, or the in-mux `chrome_tls` / proxy hops.

#### Scenario: Shareholder incremental keeps cninfo primary
- **WHEN** the shareholder incremental resolver has `cninfo:direct` as the first candidate and a backup `akshare` candidate
- **AND** the cninfo provider returns snapshots that cover the required scope
- **THEN** the job SHALL write those cninfo snapshots
- **AND** it SHALL NOT call the backup provider for those instruments

#### Scenario: Shareholder incremental switches after cninfo fails
- **WHEN** the shareholder incremental resolver has `cninfo:direct` first and `akshare:direct` as backup
- **AND** the cninfo provider raises or returns no covering snapshot for an instrument
- **THEN** the job SHALL attempt the backup provider for the remaining instruments
- **AND** a covering backup snapshot SHALL be accepted without rewriting the resolver or registry

#### Scenario: Shareholder incremental switches after incomplete cninfo scope
- **WHEN** the cninfo provider returns a snapshot that does not cover the required scope
- **AND** a backup provider can cover the missing scope
- **THEN** the job SHALL keep the uncovered instrument in the remaining set
- **AND** it SHALL call the backup provider for that remaining set

#### Scenario: Financial repair keeps official data20 first
- **WHEN** financial maintenance repair uses `repair_source_order` of `cninfo_data20`, then `ths_report`, then `sina_report`
- **AND** cninfo data20 leaves the target unready
- **THEN** the existing repair router SHALL attempt the THS/Sina fallback sources
- **AND** it SHALL NOT add a second repair owner or migrate THS/Sina into the mux

#### Scenario: Backup providers are not mux work
- **WHEN** shareholder valuation, holder-count, or control code calls AkShare `*_cninfo`
- **OR** financial fallback uses THS/Sina
- **OR** announcement acquisition falls through to an exchange source
- **THEN** those calls SHALL stay on their existing providers
- **AND** this change SHALL NOT treat that leftover usage as an incomplete mux migration

### Requirement: Exhausted Direct Hops Surface Failure To Existing Routing
When every hop inside `attach_cninfo_access` fails to return usable first-party www / data20 data, the mux SHALL surface an HTTP error or raise. Domain providers SHALL treat that as an uncovered candidate so the existing resolver or repair router can continue to a backup provider. The mux SHALL NOT convert a blocked or empty failure into a successful covered result.

#### Scenario: Headed blocked and proxy failure keeps the HTTP error
- **WHEN** preferred mode is `headed_chrome`
- **AND** the headed hop reports `chrome_blocked` with an HTTP 403 response
- **AND** the www proxy hop raises
- **THEN** the mux SHALL return that HTTP 403 response (or raise)
- **AND** it SHALL NOT return HTTP 200 empty JSON solely to hide the failure

#### Scenario: Legacy stack exhaustion keeps the HTTP error
- **WHEN** the runtime is using the `chrome_tls` stack
- **AND** Chrome TLS returns HTTP 403
- **AND** the www proxy hop raises
- **THEN** the mux SHALL return that HTTP 403 response
- **AND** a domain provider that treats HTTP >= 400 as failure SHALL be able to fail the candidate and let routing continue

#### Scenario: Daily jobs expose a hop snapshot
- **WHEN** shareholder incremental or financial disclosure incremental finishes a run
- **THEN** the result SHALL include a `cninfo_access` snapshot with preferred mode, seen hops, sticky, and request count
- **AND** starting that run SHALL reset those counters without stopping headed Chrome or clearing www sticky
