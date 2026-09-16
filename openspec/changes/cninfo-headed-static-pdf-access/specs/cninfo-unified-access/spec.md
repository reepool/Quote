## MODIFIED Requirements

### Requirement: Host And Path Routing Without Per-Domain Copies
The mux SHALL route by URL host and path. Allowlisted first-party www URLs MAY use headed Chrome. HTTPS `static.cninfo.com.cn` SHALL use the same preferred headed hop and blocked-versus-unavailable backup as www, through `attach_cninfo_access`. Unallowlisted https www paths, `webapi.cninfo.com.cn`, non-https URLs, and non-CNInfo hosts SHALL never use headed Chrome. Prefix `/data20/` SHALL admit future data20 endpoints to the allowlisted www hop without a new access implementation.

#### Scenario: Data20 uses the first-party www hop
- **WHEN** a caller GETs `https://www.cninfo.com.cn/data20/` on any path under that prefix
- **THEN** the mux SHALL treat the URL as allowlisted first-party www
- **AND** it SHALL apply the preferred access mode and unified fallback

#### Scenario: Static PDF uses preferred headed then backup
- **WHEN** official-filing or announcement-attachment code GETs `https://static.cninfo.com.cn/` for a PDF through an attached session
- **AND** preferred mode is `headed_chrome`
- **AND** the runtime is not sticky-preferring a static-proxy or Chrome-unavailable fallback
- **THEN** the mux SHALL send that URL to headed Chrome first
- **AND** it SHALL use Chrome TLS then `akshare_proxy_patch` only as backup
- **AND** it SHALL NOT use bare Python `requests` TLS solely because a session was injected
- **AND** proxy fallback SHALL accept a PDF or trusted historical HTML body
- **AND** proxy fallback SHALL reject a Wangsu block page and a JSON API payload

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

#### Scenario: Announcement attachments use the mux with headed Chrome first
- **WHEN** `AnnouncementAttachmentRetriever` downloads a `static.cninfo.com.cn` attachment and the caller did not inject a session
- **THEN** that download SHALL obtain HTTP access through `attach_cninfo_access`
- **AND** the mux SHALL send that URL to headed Chrome first when preferred mode is `headed_chrome` and headed Chrome can return a usable body
- **AND** Chrome TLS and `akshare_proxy_patch` SHALL remain backups
- **AND** an injected session SHALL remain unwrapped so tests keep their fake transport
