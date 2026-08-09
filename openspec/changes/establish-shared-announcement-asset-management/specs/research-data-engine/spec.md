## ADDED Requirements

### Requirement: Research Consumers Use Shared Annual-Report Assets
The Research Data Engine SHALL provide one stable annual-report asset dependency for business-profile, broker risk-control, and future consumers.

#### Scenario: Business-profile requires an annual report
- **WHEN** a business-profile acquire or parse stage needs a formal annual report
- **THEN** it SHALL request the effective shared asset by instrument and fiscal year or by source filing identity
- **AND** it SHALL not write a business-profile-owned copy of the original attachment

#### Scenario: Consumer requires an exact filing
- **WHEN** a business work item is already bound to a source and announcement id
- **THEN** the shared service SHALL ensure that exact filing or return an explicit unavailable/integrity status
- **AND** it SHALL not substitute a different legal filing solely because its content or title is similar

#### Scenario: Consumer pins an exact filing observation
- **WHEN** a business work item supplies attachment id plus expected content hash or observation version for an exact filing
- **THEN** the shared service SHALL preserve that evidence identity and SHALL NOT substitute a later attachment observation under the same filing id
- **AND** a deleted pinned predecessor SHALL return metadata with local content unavailable rather than current bytes

#### Scenario: Consumer requests a superseded exact filing
- **WHEN** a business work item requests a known predecessor whose bytes were deleted under version 1 retention
- **THEN** the shared service SHALL return historical metadata with local content unavailable
- **AND** ordinary consumer acquisition SHALL NOT redownload the superseded attachment

#### Scenario: Consumer needs only local data
- **WHEN** a research workflow runs with network calls disabled
- **THEN** annual-report lookup SHALL be local-only and SHALL return an explicit missing status when unavailable

#### Scenario: Historical knowledge cutoff is requested
- **WHEN** a consumer requests annual-report evidence with a knowledge cutoff
- **THEN** selection SHALL exclude announcements, attachment observations, and withdrawal states whose publication or `version_available_at` is after that cutoff
- **AND** if the eligible predecessor bytes were deleted under version 1 retention, the service SHALL report historical metadata with local content unavailable rather than substitute a later correction

#### Scenario: Structured financial facts are available
- **WHEN** an existing official JSON, XBRL, or canonical numeric-fact source satisfies a financial-data request
- **THEN** the annual-report asset capability SHALL NOT replace that structured source or trigger PDF parsing during valuation reads

#### Scenario: Consumer migration gate is enabled
- **WHEN** business-profile or broker annual-report acquisition has cut over to shared assets
- **THEN** a shared miss SHALL use only the shared ensure path
- **AND** the consumer SHALL NOT fall back to its legacy provider downloader or business-owned original archive writer

#### Scenario: Adopted asset path changes during migration
- **WHEN** a verified source asset is moved, linked, or projected from a legacy path to the canonical blob pool without changing source identity or content hash
- **THEN** business-profile and broker consumer-processing identity SHALL remain stable
- **AND** the path change alone SHALL NOT force redownload or alter derived business facts

### Requirement: DataManager Exposes Annual-Report Asset Operations
DataManager SHALL expose business-neutral annual-report asset lookup, ensure, status, and bounded-operation methods that do not depend on business-profile configuration.

#### Scenario: Caller lists effective reports
- **WHEN** a caller lists annual reports by instrument, fiscal year, source, integrity, or acquisition status
- **THEN** DataManager SHALL query the shared asset repository and return stable source and asset identities

#### Scenario: Caller ensures a report
- **WHEN** a caller requests ensure with permitted network and storage policy
- **THEN** DataManager SHALL invoke the shared service and return `local_hit`, `local_miss`, `operation_created`, or `operation_reused` disposition
- **AND** asset availability, durable operation status/stage, and final `adopted|downloaded|repaired` origin SHALL be represented separately
- **AND** a front-facing caller SHALL receive its authorized `asset_request_id` opaque subscription handle rather than the internal shared operation identity
- **AND** the handle SHALL be present only when asynchronous acquisition is created or reused; an immediate `local_hit` or network-disabled `local_miss` SHALL return no handle (or an explicit `null`)

#### Scenario: Legacy annual-report catalog is called during migration
- **WHEN** an existing caller uses `get_annual_report_assets` or `get_annual_report_asset`
- **THEN** the compatibility method SHALL read through the shared repository
- **AND** it SHALL not remain permanently filtered to business-profile manifests

#### Scenario: Legacy compatibility response is produced
- **WHEN** an existing internal caller uses a legacy catalog signature or identifier during migration
- **THEN** DataManager SHALL preserve compatible call parameters and key legacy identities while adding the shared asset id
- **AND** any controlled local handle SHALL remain internal and SHALL be removed by external API serializers

#### Scenario: Caller inspects operations and readiness
- **WHEN** a caller requests annual-report operation status or service readiness
- **THEN** DataManager SHALL require a trusted request context and expose caller-owned `asset_request_id` and `consumer_request_id` projections plus redacted readiness without constructing business-profile services
- **AND** raw internal operation, provider, filesystem, actor, and detailed failure state SHALL require operator or service scope

### Requirement: Annual-Report Asset API Is Additive And Safe
The Research API SHALL provide additive endpoints for effective annual-report metadata, readiness, acquisition requests, caller-scoped asset-request and consumer-request status, and controlled file delivery.

#### Scenario: Front-facing client queries status
- **WHEN** a client requests annual-report asset status for an instrument
- **THEN** the response SHALL include fiscal year, report period, source, filing id, published time, correction flag, content hash, content length, local availability, integrity, acquisition status, effective status, and last checked time
- **AND** the GET request SHALL perform zero provider calls and zero attachment writes

#### Scenario: Client requests acquisition
- **WHEN** an authorized client requests ensure for an annual report
- **THEN** the API SHALL accept only one instrument/fiscal-year or exact source-filing scope and create or reuse a bounded operation
- **AND** it SHALL return HTTP 200 with `local_hit` for an immediately valid asset, HTTP 200 with `local_miss` when network acquisition is explicitly disabled, or HTTP 202 with principal-scoped `asset_request_id`, a `Location` containing only that opaque handle, and `Retry-After` for created or reused asynchronous work
- **AND** it SHALL not keep the request open for an unbounded market or attachment fetch

#### Scenario: A business command starts consumer processing
- **WHEN** a protected business-profile or broker command requires an annual-report-backed processing result
- **THEN** that business command SHALL accept a consumer processing fingerprint and caller idempotency key and SHALL execute the shared local-first lookup before processing
- **AND** a local hit SHALL create or reuse exactly one caller-owned `consumer_request_id` without creating an asset acquisition subscription
- **AND** the command SHALL create or reuse the caller-owned `consumer_request_id` immediately, with `pending_asset` status when the shared asset is not yet locally valid
- **AND** a local miss with acquisition permitted SHALL create or reuse one `asset_request_id`, persist one consumer-specific continuation linked to that already-created consumer request, and advance the same consumer request to `queued|processing` only after the asset is valid
- **AND** the command SHALL return HTTP 200 only when an existing consumer result is bound to the current effective asset id/content hash and the same processing fingerprint; a local asset hit that still requires consumer processing or whose prior result is stale SHALL return HTTP 202 with the `consumer_request_id`, and an asset miss with acquisition permitted SHALL return HTTP 202 with both opaque request handles
- **AND** a business-command `Location` SHALL always identify the overall consumer-request resource, while an applicable asset-request URL SHALL appear in a response-body link; generic asset ensure alone SHALL use an asset-request `Location`
- **AND** a normal local/confirmed miss or network-disabled request SHALL return HTTP 200 with a terminal `missing` consumer projection, a pre-work temporary provider/storage blocker SHALL return HTTP 503, and an ambiguity/current-state conflict SHALL return HTTP 409, each with stable reason codes (`annual_report_not_found`, `network_disabled`, `provider_unavailable`, `archive_mount_unavailable`, `storage_reserve_exceeded`, `backup_gate_blocked`, `candidate_ambiguous`, `effective_state_conflict`, or the applicable authorization/idempotency code) and no unintended provider work
- **AND** generic asset ensure SHALL NOT start this consumer processing implicitly

#### Scenario: Business command adapters have explicit ownership
- **WHEN** the business-profile or broker risk-control front end starts annual-report-backed processing
- **THEN** it SHALL call its own protected command adapter (for example `POST /api/v1/research/company/{instrument_id}/business-profile/annual-report-process` or `POST /api/v1/research/company/{instrument_id}/broker-risk-control/annual-report-process`)
- **AND** the adapter SHALL own the consumer processing fingerprint, caller idempotency contract, and `consumer_request_id` lifecycle while delegating source discovery and attachment acquisition to the shared asset service
- **AND** generic asset ensure SHALL remain a source-asset-only entry point and SHALL NOT be treated as the business processing entry point

#### Scenario: Business command authorization is composed safely
- **WHEN** a caller invokes a business-profile or broker risk-control processing command
- **THEN** the adapter SHALL require its domain processing scope before creating a consumer request
- **AND** a verified local asset MAY be processed with the domain scope alone, but a missing asset SHALL contact a provider only when the caller also has `annual_report_assets:acquire`
- **AND** insufficient domain or acquire scope SHALL return HTTP 403 or the configured HTTP 404 non-disclosure response before creating unauthorized consumer/asset work

#### Scenario: Business command idempotency is repeated or conflicts
- **WHEN** one principal repeats a business command with the same idempotency key, consumer, selector, processing fingerprint, and normalized body
- **THEN** the adapter SHALL return the same `consumer_request_id` and continuation and SHALL NOT create a second consumer operation
- **AND** reuse of that key with a different consumer, selector, processing fingerprint, or body SHALL return HTTP 409 with a stable idempotency-conflict code and no new work

#### Scenario: Client submits an invalid selector combination
- **WHEN** an ensure request mixes effective-period and exact-filing selectors, omits one member of `source + filing_id`, supplies an attachment/hash/observation pin without exact-filing identity, supplies inconsistent fiscal-year and report-period values, binds an exact filing to a different path instrument, or supplies a provider URL or filesystem path
- **THEN** the API SHALL reject the request with HTTP 422 and a stable validation code before creating an operation or contacting a provider
- **AND** the two supported selector forms SHALL remain mutually exclusive and all-or-none

#### Scenario: Acquisition request is repeated
- **WHEN** the same normalized scope and policy is submitted again with the same idempotency identity while work is active
- **THEN** the API SHALL return the same principal-scoped request subscription and SHALL NOT issue a second provider request or physical write
- **AND** the subscription MAY reference a globally shared internal asset operation without exposing another trigger's ownership or diagnostics

#### Scenario: Idempotency identity is reused for a different request
- **WHEN** one principal reuses an `Idempotency-Key` with a different normalized selector, policy, or request body
- **THEN** the API SHALL return HTTP 409 with a stable idempotency-conflict code and SHALL neither reuse the old result nor create new work
- **AND** request idempotency, response visibility, and consumer continuation SHALL be scoped to the authenticated principal
- **AND** another principal MAY attach its own subscription to the same globally single-flight asset operation but SHALL NOT inherit the first caller's request handle, idempotency record, continuation, or privileged diagnostics

#### Scenario: Authorization boundary is unavailable
- **WHEN** the deployment has no configured trusted identity and scoped permissions
- **THEN** registered acquisition, content delivery, cancellation, repair, and operator endpoints SHALL return HTTP 503 with `authorization_boundary_unavailable`
- **AND** the response SHALL not contact a provider, reveal filesystem paths, or create a durable mutation operation
- **AND** this fail-closed gate SHALL run before selector validation, resource lookup, or ownership checks so response differences cannot be used to enumerate assets or operations

#### Scenario: Caller exceeds an API scope or rate bound
- **WHEN** a client requests a market-wide scope, an unsafe parameter, or exceeds configured request/rate limits
- **THEN** the API SHALL return HTTP 422 with a stable validation code for an invalid/over-broad scope, or HTTP 429 with `Retry-After` for a rate-limit violation
- **AND** it SHALL NOT create an unbounded operation or contact a provider

#### Scenario: Provider or storage is temporarily unavailable
- **WHEN** an authorized bounded ensure cannot proceed because the provider, archive mount, or storage reserve is unavailable
- **THEN** the API SHALL return HTTP 503 with a stable retryable or blocked error code and bounded diagnostics
- **AND** it SHALL preserve any durable operation and never publish partial bytes

#### Scenario: Client follows operation status
- **WHEN** a client polls its `asset_request_id`
- **THEN** the API SHALL return `queued|running|completed|missing|failed|blocked|cancelled|expired` status, current stage, retry metadata, timestamps, progress, result asset id, stable reason codes, and bounded diagnostics
- **AND** it SHALL separately return asset availability, ensure disposition, and downstream consumer-processing state where applicable
- **AND** the API SHALL project the underlying shared asset operation through that caller's subscription rather than expose the internal operation directly

#### Scenario: Client follows downstream consumer status
- **WHEN** a business command returns a `consumer_request_id` and the owner polls `GET /api/v1/research/annual-report-consumer-requests/{consumer_request_id}`
- **THEN** the API SHALL return consumer identity, processing fingerprint, `pending_asset|not_started|queued|processing|current|stale|failed|missing|blocked|cancelled` request/processing status, result identity, retry metadata, timestamps, stable reason codes, and bounded diagnostics
- **AND** it SHALL include the linked caller-visible `asset_request_id` only when acquisition was required; a local-hit consumer request SHALL not invent one
- **AND** it SHALL NOT expose internal asset or consumer operation ids, other principals, or filesystem paths
- **AND** an unknown or cross-owner consumer request SHALL follow the same configured 404 non-disclosure and common error-envelope policy as an asset request

#### Scenario: Client cancels one shared acquisition request
- **WHEN** an authorized client cancels its `asset_request_id` while another principal or scheduler still depends on the underlying acquisition
- **THEN** only that request subscription and its pending consumer continuation SHALL become cancelled
- **AND** the internal asset operation SHALL remain active or checkpoint according to remaining subscribers and SHALL NOT expose their identities
- **AND** DELETE SHALL return HTTP 200 with a durable `cancelled` projection, repeated DELETE SHALL return the same outcome, and a later owner GET SHALL remain queryable for audit

#### Scenario: Client cancels the last shared acquisition request
- **WHEN** the last principal cancels its `asset_request_id` after bounded internal acquisition work exists
- **THEN** version 1 SHALL cancel only that subscription and any not-yet-started continuation while the internal acquisition continues to a bounded terminal state
- **AND** consumer processing that already started SHALL not be cancelled through the asset request; its own domain stop contract SHALL apply or the stop SHALL be explicitly rejected
- **AND** an unknown or cross-owner request SHALL follow the configured HTTP 404 non-disclosure policy

#### Scenario: Client cancels a consumer request
- **WHEN** an authorized owner deletes a `consumer_request_id`
- **THEN** a not-yet-started continuation SHALL be cancelled idempotently with HTTP 200, remain queryable as `cancelled`, and return the same projection on repeated DELETE
- **AND** already-started consumer processing SHALL use that consumer domain's authorized cooperative-stop contract and return HTTP 202 when accepted, or HTTP 409 when stopping is unsupported/current-state invalid; it SHALL NOT be force-cancelled through request-subscription deletion
- **AND** cancelling a completed or current request SHALL NOT rewrite its result, while unknown or cross-owner identifiers SHALL follow the configured HTTP 404 non-disclosure policy

#### Scenario: Client polls another caller's operation
- **WHEN** a caller lacks ownership/read scope for the requested asset-request subscription or consumer-request projection
- **THEN** the API SHALL deny access through the common error envelope without disclosing the underlying operation scope, diagnostics, subscribers, or existence beyond the configured authorization policy

#### Scenario: Effective report is not locally available
- **WHEN** a metadata GET cannot resolve a local valid effective report
- **THEN** the API SHALL return structured missing, metadata-only, corrupt, ambiguous, or blocked availability with last-checked evidence
- **AND** it SHALL NOT convert a normal absence into an internal-server error or trigger acquisition

#### Scenario: A served predecessor is provisional
- **WHEN** a newer complete correction is known but has not passed acquisition and validation
- **THEN** the API SHALL expose the predecessor's local availability separately from a provisional effective-decision state, pending correction identity, and stable reason code
- **AND** business-facing clients SHALL NOT label the predecessor as an unqualified final latest-effective report

#### Scenario: Client downloads an asset
- **WHEN** a client requests an available asset by id
- **THEN** the API SHALL validate the effective record and file integrity before streaming with a safe filename, `application/pdf`, and verified Content-Length
- **AND** it SHALL not accept a caller-supplied filesystem path

#### Scenario: Client requests a superseded or corrupt asset
- **WHEN** a content request resolves to a superseded, missing, or integrity-failed file
- **THEN** the API SHALL return HTTP 410 for a known superseded/deleted asset and HTTP 409 for a current asset whose bytes fail integrity, using stable error codes
- **AND** it SHALL never return HTTP 409 for a known superseded/deleted asset
- **AND** it SHALL NOT stream stale bytes

#### Scenario: API outcomes are mapped deterministically
- **WHEN** a client receives a normal missing metadata result, an unknown resource, an authentication or permission failure, a malformed selector, a rate limit, or a temporary provider/storage blocker
- **THEN** a metadata query with no local report SHALL return HTTP 200 with structured availability, while an unknown asset or operation SHALL return HTTP 404
- **AND** a configured trusted boundary SHALL use HTTP 401 for missing authentication, HTTP 403 for insufficient scope, or the documented HTTP 404 non-disclosure policy for cross-owner operation lookup
- **AND** selector validation SHALL use HTTP 422, rate limits HTTP 429 with `Retry-After`, and temporary infrastructure blockers HTTP 503 with a stable retryable or blocked code
- **AND** every non-success response SHALL use the versioned common error envelope rather than provider exception text

### Requirement: Consumer Outputs Surface Asset Lineage
Business-facing results that depend on annual reports SHALL expose sufficient shared asset lineage for audit without leaking internal archive paths.

#### Scenario: A business action has an immediately valid asset
- **WHEN** a front-facing business-profile or broker command resolves a verified local annual-report asset
- **THEN** that business orchestration SHALL create or reuse exactly one consumer operation for the requested consumer and processing fingerprint without creating an asset-acquisition operation
- **AND** the response SHALL expose a caller-owned `consumer_request_id` separately from asset availability without exposing the internal consumer operation identity

#### Scenario: A business action must wait for asset acquisition
- **WHEN** a front-facing business-profile or broker command creates or reuses an asset ensure operation
- **THEN** that business orchestration SHALL persist one idempotent continuation tied to the requested consumer, processing fingerprint, and asset operation
- **AND** it SHALL enqueue exactly one consumer operation after the asset becomes locally valid, or terminally fail/block the continuation when the asset operation cannot produce a valid asset
- **AND** generic asset ensure SHALL NOT implicitly start every consumer
- **AND** the response SHALL expose caller-owned `asset_request_id` and `consumer_request_id`, never internal asset or consumer operation ids, so asset completion and business-result completion remain independently queryable

#### Scenario: Business-profile result uses annual-report evidence
- **WHEN** a business-profile result is returned
- **THEN** its evidence lineage SHALL identify the shared asset id, source filing, report period, content hash, and effective correction status
- **AND** its consumer-processing status SHALL distinguish current results from stale or reprocessing results after a source change

#### Scenario: Broker fact uses annual-report evidence
- **WHEN** a broker regulatory fact is returned or inspected
- **THEN** its source lineage SHALL identify the shared asset id and broker processing manifest
