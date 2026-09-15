## ADDED Requirements

### Requirement: CNInfo Access Provider Is Independent Of Domain Providers
The system SHALL provide a CNInfo headed-Chrome access provider that issues HTTP-like GET and POST requests and does not parse shareholder snapshots, announcement records, or financial filings.

#### Scenario: Access provider returns a session-shaped response
- **WHEN** a caller invokes `request`, `get`, or `post` on the headed-Chrome access provider for an allowed CNInfo URL
- **THEN** the provider SHALL forward `params`, `data`, `json`, `headers`, and `timeout`
- **AND** form `data` SHALL be sent as `application/x-www-form-urlencoded` and `json` SHALL be sent as a JSON body
- **AND** it SHALL return a response object exposing `status_code`, `url`, `headers`, `text`, `content`, `json()`, `raise_for_status()`, and `access_mode`
- **AND** it SHALL NOT write shareholder snapshots, announcement audit rows, or financial filings

#### Scenario: Domain providers stay on the current access path
- **WHEN** production shareholder, announcement, or official-filing code constructs CNInfo access through the existing default factory with no injected session
- **THEN** that factory SHALL continue to use Chrome TLS first with `akshare_proxy_patch` on HTTP 403
- **AND** it SHALL NOT construct the headed-Chrome access provider

### Requirement: One Headed Chrome Session Per Provider Instance
The headed-Chrome access provider SHALL reuse a single headed Chrome browser and homepage document for subsequent allowed CNInfo API requests on that instance while headed Chrome is still in use. After the instance sticky-prefers proxy, later requests SHALL use proxy and SHALL NOT require a live homepage.

#### Scenario: First request bootstraps the homepage
- **WHEN** the provider handles its first allowed CNInfo request and Chrome can start
- **THEN** it SHALL start headed Chrome with `headless=false`
- **AND** if `DISPLAY` is already set, it SHALL reuse that display and SHALL NOT start a second Xvfb that overwrites it
- **AND** if `DISPLAY` is unset, it SHALL start Xvfb for this instance
- **AND** it SHALL open `https://www.cninfo.com.cn/`
- **AND** it SHALL NOT issue in-page API calls from a Wangsu 403 block page
- **AND** if bootstrap remains a block page after one restart, it SHALL sticky-prefer proxy instead of calling in-page APIs

#### Scenario: Later requests reuse the same browser
- **WHEN** the same provider instance handles a second allowed CNInfo request and is not sticky-preferring proxy
- **THEN** it SHALL reuse the existing browser and homepage document
- **AND** it SHALL NOT start a new Chrome process for that request

#### Scenario: Later requests stay on proxy after fallback
- **WHEN** the same provider instance is already sticky-preferring proxy
- **THEN** later allowed requests SHALL use proxy
- **AND** they SHALL NOT require a live homepage document

#### Scenario: Separate instances do not share a browser
- **WHEN** two headed-Chrome access provider instances are constructed
- **THEN** each instance SHALL own its own browser lifecycle
- **AND** closing one instance SHALL NOT close the other instance's browser

#### Scenario: True headless mode is rejected
- **WHEN** configuration asks the CNInfo headed-Chrome access provider to use `headless=true`
- **THEN** the provider SHALL raise and refuse to start
- **AND** it SHALL NOT send that headless session to `www.cninfo.com.cn`
- **AND** it SHALL NOT treat that misconfiguration as a Chrome-start failure that falls back to proxy

#### Scenario: Close releases this instance only
- **WHEN** a caller invokes `close()` on one headed-Chrome access provider instance
- **THEN** that instance SHALL stop its Chrome process
- **AND** it SHALL stop an Xvfb only if this instance started it
- **AND** it SHALL NOT stop another instance's browser or a display started by DCE

### Requirement: Same-Origin In-Page Requests
After a successful homepage bootstrap, the provider SHALL perform allowed CNInfo API calls as same-origin requests from that document instead of navigating a new page per URL.

#### Scenario: Data20 is fetched from the homepage context
- **WHEN** the provider is asked to GET an allowed `/data20/` URL after bootstrap and headed Chrome is not sticky-preferring proxy
- **THEN** it SHALL issue the request from the existing homepage document
- **AND** that in-page request SHALL include same-origin cookies

#### Scenario: Announcement query is posted from the homepage context
- **WHEN** the provider is asked to POST `https://www.cninfo.com.cn/new/hisAnnouncement/query` after bootstrap and headed Chrome is not sticky-preferring proxy
- **THEN** it SHALL issue the form POST from the existing homepage document
- **AND** that in-page request SHALL include same-origin cookies

#### Scenario: Per-URL navigation is not used for API calls
- **WHEN** the provider issues a data20 or announcement API request after bootstrap
- **THEN** it SHALL NOT replace the homepage document by navigating the browser to that API URL

### Requirement: Host Allowlist And Proxy Fallback
The provider SHALL only accept first-party `www.cninfo.com.cn` paths used by current research callers, and SHALL fall back to `akshare_proxy_patch` after a headed-Chrome 403, block page, or Chrome start/bootstrap failure.

#### Scenario: Non-allowed URL is rejected
- **WHEN** a caller requests `http://www.cninfo.com.cn`, `webapi.cninfo.com.cn`, `static.cninfo.com.cn`, or any host that is not `www.cninfo.com.cn` / `cninfo.com.cn`
- **THEN** the provider SHALL raise an error before starting or using Chrome

#### Scenario: Disallowed www path is rejected
- **WHEN** a caller requests `https://www.cninfo.com.cn/` on a path that is not exact `/`, not a `/data20/` prefix, not exact `/new/hisAnnouncement/query`, not exact `/new/information/topSearch/query`, and not a `/new/disclosure/` prefix
- **THEN** the provider SHALL raise an error before starting or using Chrome
- **AND** exact `/` SHALL NOT be treated as a prefix that allows every www path

#### Scenario: Disclosure HTML on www is allowed
- **WHEN** a caller GETs an allowed `https://www.cninfo.com.cn/new/disclosure/` URL
- **THEN** the provider SHALL accept the URL
- **AND** it SHALL NOT treat that page as a `static.cninfo.com.cn` PDF download

#### Scenario: Chrome 403 or Chrome request failure falls back to proxy and stays on proxy
- **WHEN** a headed-Chrome request to an allowed CNInfo URL returns HTTP 403, a Wangsu block page, a null/failed evaluate result, or a Chrome/timeout exception
- **THEN** the provider SHALL retry that request through `akshare_proxy_patch`
- **AND** it SHALL NOT first probe the same URL with Python `requests` TLS
- **AND** it SHALL NOT return a null evaluate as a successful empty `headed_chrome` body
- **AND** after a successful proxy fallback, later requests on that instance SHALL use proxy without sending another headed-Chrome probe first

#### Scenario: Proxy fallback accepts JSON and disclosure HTML
- **WHEN** the provider falls back to `akshare_proxy_patch` for an allowed JSON API or `/new/disclosure/` HTML page
- **THEN** it SHALL accept a JSON body or `text/html` respectively
- **AND** it SHALL NOT reject disclosure HTML solely because the current `_accept_cninfo_proxy_response` helper requires JSON

#### Scenario: Chrome cannot start
- **WHEN** Xvfb or headed Chrome cannot start, or homepage bootstrap remains a block page after one restart
- **THEN** the provider SHALL fall back to `akshare_proxy_patch` for that instance
- **AND** it SHALL expose `access_mode` as proxy fallback

#### Scenario: Access mode is visible
- **WHEN** a request succeeds from headed Chrome or from proxy fallback
- **THEN** the provider SHALL expose `access_mode` as `headed_chrome` or `proxy_patch` respectively

### Requirement: Opt-In Live Probe Does Not Write Production Research Data
The change SHALL include an operator-triggered live probe that exercises the access provider without becoming a production scheduler job and without writing canonical research snapshots.

#### Scenario: Operator probe exercises three surfaces
- **WHEN** an operator runs the headed-Chrome CNInfo access probe
- **THEN** the probe SHALL construct the headed-Chrome access provider directly
- **AND** it SHALL request the homepage bootstrap path, one data20 endpoint, and one announcement query
- **AND** it SHALL report status, whether the body is JSON, and the access mode
- **AND** it SHALL NOT upsert `shareholder_snapshots` or finish a shareholder ingestion run

#### Scenario: Production modules do not import the probe
- **WHEN** research providers or scheduler task modules are imported
- **THEN** they SHALL NOT import the operator probe script
