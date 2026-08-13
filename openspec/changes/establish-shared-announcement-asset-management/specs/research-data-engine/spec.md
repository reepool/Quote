## ADDED Requirements

### Requirement: Research Consumers Use Shared Annual-Report Assets
The Research Data Engine SHALL provide one stable annual-report asset dependency for business-profile, broker risk-control, and future consumers.

Every consumer of a document family managed by the shared capability SHALL perform shared local lookup/ensure before provider work and, once its consumer cutover gate is enabled, SHALL NOT call provider retrieval directly or persist a private original-attachment archive. Parser outputs, derived files, facts, and consumer processing state remain owned by the consumer.

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
- **THEN** annual-report lookup SHALL be local-only
- **AND** the lookup SHALL return an explicit missing status when unavailable

#### Scenario: Historical knowledge cutoff is requested
- **WHEN** a consumer requests annual-report evidence with a knowledge cutoff
- **THEN** selection SHALL exclude announcements, attachment observations, and withdrawal states whose publication or `version_available_at` is after that cutoff
- **AND** if the eligible predecessor bytes were deleted under version 1 retention, the service SHALL report historical metadata with local content unavailable rather than substitute a later correction

#### Scenario: Historical or pinned consumer result remains valid
- **WHEN** a consumer result is bound to the immutable observation resolved by an exact-filing pin or knowledge cutoff and a later correction appears outside that selector/cutoff
- **THEN** result validity SHALL continue to be evaluated against the selector-resolved observation and processing fingerprint rather than the present-day effective asset
- **AND** the later observation SHALL NOT mark that historically correct result stale
- **AND** an unpinned default effective-period result SHALL still become stale when its current effective asset changes

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

### Requirement: Business-Profile Migration Preserves Domain Semantics
Business-profile SHALL consume shared annual-report source assets without transferring its parser, derived artifacts, review, promotion, or business-result ownership into the shared asset capability.

#### Scenario: Business-profile switches to shared source assets
- **WHEN** the business-profile consumer migration gate is enabled
- **THEN** `BusinessProfileDocumentArchiveService` SHALL stop owning formal annual-report discovery, original-attachment download, and the final source-archive root
- **AND** page artifacts, sections, extracted text/text hashes, semantic and LLM results, review evidence, promoted facts, parser versions/parameters, and business processing state SHALL remain business-profile-owned and preserve their existing contracts
- **AND** knowledge-cutoff and exact-observation behavior SHALL remain unchanged except for explicit metadata-only unavailability when version 1 no longer retains predecessor bytes

#### Scenario: Legacy annual-report catalog is used after migration
- **WHEN** a business-profile or compatibility caller uses `AnnualReportAssetCatalog` or its existing DataManager methods
- **THEN** the catalog SHALL act as a read-through compatibility projection over the shared repository rather than a business-profile-owned source manifest or downloader
- **AND** shutdown of legacy writes SHALL remain gated until dual-read identity/hash reconciliation passes; legacy writes remain available during the migration comparison window

#### Scenario: Only the source-asset path changes
- **WHEN** migration resolves the same legal filing, observation, and content hash through a different controlled shared path
- **THEN** business-profile processing identity and equivalent parser output SHALL remain stable
- **AND** path relocation alone SHALL NOT trigger a new business identity, alter derived hashes, or change promoted facts

### Requirement: DataManager Exposes Annual-Report Asset Operations
DataManager SHALL expose business-neutral annual-report asset lookup, ensure, status, and bounded-operation methods that do not depend on business-profile configuration.

#### Scenario: Caller lists annual-report asset records
- **WHEN** a caller lists annual reports by instrument, fiscal year, source, canonical `source_announcement_id` or compatible `filing_id`, integrity, acquisition status, effective state, or local availability
- **THEN** DataManager SHALL query the shared asset repository and return stable source and asset identities for the bounded requested projection
- **AND** the versioned list contract SHALL state whether current-effective, metadata-only, superseded, withdrawn, and historical filing records are included
- **AND** an explicit effective-state filter SHALL allow callers to select those records without conflating record existence with current effectiveness
- **AND** the single source/filing fields SHALL be the versioned canonical projection
- **AND** equivalent cross-source evidence SHALL include a stable `equivalent_source_filings` set, projection-policy version, and evidence-set hash
- **AND** equivalent cross-source evidence SHALL preserve distinct legal identities rather than merge them
- **AND** the filters SHALL be composable only within a bounded single-consumer query, with deterministic pagination, ordering, and page-size limits
- **AND** listing SHALL remain local-only and perform zero provider calls, attachment writes, or implicit ensure work

#### Scenario: Caller ensures a report
- **WHEN** a caller requests ensure with permitted network and storage policy
- **THEN** DataManager SHALL invoke the shared service and return `local_hit`, `local_miss`, `operation_created`, or `operation_reused` disposition
- **AND** asset availability, durable operation status/stage, and final `adopted|downloaded|repaired` origin SHALL be represented separately
- **AND** a front-facing caller SHALL receive its authorized `asset_request_id` opaque subscription handle rather than the internal shared operation identity
- **AND** the handle SHALL be present only when asynchronous acquisition is created or reused; an immediate `local_hit` or network-disabled `local_miss` SHALL return no handle (or an explicit `null`)

#### Scenario: Caller opens a controlled asset stream
- **WHEN** DataManager is asked to stream a report by asset id under an authorized request context
- **THEN** it SHALL reject caller-supplied paths, acquire a bounded read lease, revalidate effective state, mount identity, length, and content hash before yielding bytes, and release the lease when the stream closes
- **AND** the public asset-id stream SHALL fail closed with `410` for superseded/deleted assets, while an authorized internal DataManager/service exact-filing observation pin MAY read a retained non-effective byte through a non-public controlled handle that passes its integrity policy; missing, corrupt, or path-unsafe assets remain unavailable without exposing the canonical filesystem path

#### Scenario: Legacy annual-report catalog is called during migration
- **WHEN** an existing caller uses `get_annual_report_assets` or `get_annual_report_asset`
- **THEN** the compatibility method SHALL read through the shared repository
- **AND** it SHALL not remain permanently filtered to business-profile manifests

#### Scenario: Existing business-profile GET exposes optional shared lineage
- **WHEN** an existing company-profile GET is served after the shared asset contract is registered
- **THEN** its response SHALL preserve all existing required fields
- **AND** its response SHALL add schema-defined optional/nullable `source_assets.annual_report_asset` and `consumer_processing_status` projections
- **AND** those properties SHALL remain registered in OpenAPI even when their values are null
- **AND** the annual-report projection SHALL use the stable shared asset identity and expose `asset_availability=local_valid|metadata_only|missing|ambiguous|corrupt|superseded|blocked`, effective/correction decision state, source filing, report period, content hash, and local availability without a filesystem path
- **AND** acquisition `operation_status`, `operation_stage`, and ensure `disposition` SHALL remain separate projections and SHALL NOT add `queued`, `running`, or `failed` to the asset-availability vocabulary
- **AND** per-instrument bootstrap reports SHALL use the separate `bootstrap_asset_status=available|confirmed_missing|retryable|blocked` vocabulary and SHALL NOT serialize those values into the API `asset_availability` field
- **AND** the consumer projection SHALL expose `current|stale|reprocessing` independently from shared asset validity, with a null/omitted value for callers that have no annual-report-backed processing
- **AND** a GET SHALL remain zero-network
- **AND** a GET SHALL NOT infer annual-report availability from whether company-profile processing exists
- **AND** the exact response schema, nullability, and legacy-field compatibility SHALL be captured in the OpenAPI snapshot and a response fixture

#### Scenario: Legacy compatibility response is produced
- **WHEN** an existing internal caller uses a legacy catalog signature or identifier during migration
- **THEN** DataManager SHALL preserve compatible call parameters and key legacy identities while adding the shared asset id
- **AND** any controlled local handle SHALL remain internal and SHALL be removed by external API serializers

#### Scenario: Caller inspects operations and readiness
- **WHEN** a caller requests annual-report operation status or service readiness
- **THEN** DataManager SHALL require a trusted request context for caller-owned `asset_request_id` and `consumer_request_id` projections
- **AND** redacted zero-network readiness SHALL remain available through the ordinary read-only policy without constructing business-profile services
- **AND** raw internal operation, provider, filesystem, actor, and detailed failure state SHALL require operator or service scope

### Requirement: Annual-Report Asset API Is Additive And Safe
The Research API SHALL provide additive endpoints for effective annual-report metadata, readiness, acquisition requests, caller-scoped asset-request and consumer-request status, and controlled file delivery.

The OpenAPI contract SHALL fix the final resource paths and schemas (rather than leaving them as implementation-specific aliases): instrument report list and effective lookup, bounded ensure, asset-request status/DELETE, consumer-request status/DELETE, readiness, asset-id content streaming, and one protected annual-report processing command each for business-profile and broker risk-control.
Dynamic instrument and asset/request paths SHALL be registered without route shadowing.
The OpenAPI snapshot SHALL be updated whenever a path or response field changes.

Protected operations SHALL use stable scopes equivalent to `annual_report_assets:acquire`, `annual_report_assets:read_content`, `business_profile:process`, `broker_risk_control:process`, and an operator scope.
A domain-processing scope SHALL never imply asset acquisition or operator authority.

#### Scenario: OpenAPI resource paths are registered
- **WHEN** the API contract is generated for version 1
- **THEN** it SHALL include stable resources equivalent to `/api/v1/research/company/{instrument_id}/annual-reports`, `/effective`, `/ensure`, `/api/v1/research/annual-report-asset-requests/{asset_request_id}`, `/api/v1/research/annual-report-consumer-requests/{consumer_request_id}`, `/api/v1/research/annual-report-assets/readiness`, and `/api/v1/research/annual-report-assets/{asset_id}/content`
- **AND** it SHALL include protected POST operations at `/api/v1/research/company/{instrument_id}/business-profile/annual-report-process` and `/api/v1/research/company/{instrument_id}/broker-risk-control/annual-report-process`, or exact equivalent existing paths explicitly bound to those consumer owners and frozen in the snapshot
- **AND** the existing company-profile read contract SHALL be frozen at `GET /api/v1/research/company/{instrument_id}/business-profile`
- **AND** version 1 broker frontend status SHALL be supplied by the broker processing command plus the caller-owned consumer-request resource unless a separate broker fact GET is explicitly registered and snapshotted
- **AND** backend completion SHALL NOT imply an unregistered broker UI/read surface
- **AND** list/effective/ensure selectors, status projections, links, and error envelopes SHALL be present in the OpenAPI snapshot
- **AND** path precedence tests SHALL prove that `{asset_id}`, `{asset_request_id}`, and `{consumer_request_id}` cannot capture reserved static segments such as `readiness` or `effective`

#### Scenario: Front-facing client queries status
- **WHEN** a client requests annual-report asset status for an instrument
- **THEN** the response SHALL include fiscal year, report period, source, filing id, published time, correction flag, content hash, content length, local availability, integrity, aggregate asset acquisition state, effective status, and last checked time
- **AND** aggregate acquisition state SHALL NOT expose an internal operation, another principal's subscription, retry schedule, or privileged diagnostics; those details require the caller's own `asset_request_id` projection
- **AND** when equivalent cross-source filings support the effective blob, source/filing SHALL identify the versioned canonical projection
- **AND** the response SHALL expose a stable equivalent-filing identity set, projection-policy version, and evidence-set hash
- **AND** the GET request SHALL perform zero provider calls and zero attachment writes

#### Scenario: Client requests acquisition
- **WHEN** an authorized client requests ensure for an annual report
- **THEN** the API SHALL accept only one instrument/fiscal-year or exact source-filing scope and create or reuse a bounded operation
- **AND** the versioned request schema SHALL include `allow_network`, `integrity_level`, bounded `wait_seconds`, optional `knowledge_cutoff`, and exact-filing-only `attachment_id`, `expected_content_hash`, or `observation_version` pins
- **AND** exact filing SHALL use canonical `source_announcement_id`, with `filing_id` only as an explicit compatibility alias
- **AND** the idempotency key SHALL be caller-scoped
- **AND** generic ensure SHALL NOT use a consumer field to start domain processing
- **AND** configuration and OpenAPI SHALL fix `wait_seconds` default and maximum; `0` returns immediately, a positive value waits only to the bounded deadline, completion within the bound returns the immediate HTTP 200 projection, and expiry of the wait returns HTTP 202 with the same durable handle rather than cancelling or duplicating work
- **AND** it SHALL return HTTP 200 with `local_hit` for an immediately valid asset, HTTP 200 with `local_miss` when network acquisition is explicitly disabled, or HTTP 202 with principal-scoped `asset_request_id`, a `Location` containing only that opaque handle, and `Retry-After` for created or reused asynchronous work
- **AND** an exact-filing or knowledge-cutoff response SHALL expose the resolved observation version, content hash, `version_available_at`, and `local_content_unavailable` when retained metadata exists but eligible bytes do not
- **AND** it SHALL not keep the request open for an unbounded market or attachment fetch
- **AND** a local-only generic ensure MAY use the ordinary authenticated read policy
- **AND** any `allow_network=true` branch that would create discovery/acquisition work SHALL require `annual_report_assets:acquire`
- **AND** absence of `annual_report_assets:acquire` SHALL return HTTP 403 or configured non-disclosure HTTP 404 with zero durable/provider work

#### Scenario: A business command starts consumer processing
- **WHEN** a protected business-profile or broker command requires an annual-report-backed processing result
- **THEN** the versioned `BusinessAnnualReportProcessRequest` SHALL use the same mutually exclusive effective-period/exact-filing selector, identity consistency, `allow_network`, `integrity_level`, bounded `wait_seconds`, optional `knowledge_cutoff`, and exact attachment/hash/observation pin rules as generic ensure, plus a registered consumer processing profile or optional expected processing fingerprint and caller idempotency key
- **AND** the server SHALL compute the canonical processing fingerprint from the registered consumer/parser version, governed parameters, and effective configuration
- **AND** an unknown profile SHALL return HTTP 422 without creating work
- **AND** an expected-fingerprint mismatch SHALL return HTTP 409 without creating work
- **AND** an arbitrary caller string SHALL never define a new processing identity
- **AND** the command SHALL execute the shared local-first lookup before processing
- **AND** a local hit SHALL create or reuse exactly one caller-owned `consumer_request_id` without creating an asset acquisition subscription
- **AND** the command SHALL create or reuse the caller-owned `consumer_request_id` immediately, with `pending_asset` status when the shared asset is not yet locally valid
- **AND** a local miss with acquisition permitted SHALL create or reuse one `asset_request_id`, persist one consumer-specific continuation linked to that already-created consumer request, and advance the same consumer request to `queued|processing` only after the asset is valid
- **AND** the command SHALL return HTTP 200 only when `consumer_request_status=completed` and `consumer_result_state=current` for the observation resolved by the normalized selector, knowledge cutoff, retention policy, and processing fingerprint; for the default effective-period selector that observation is the present effective asset, while exact-filing and historical selectors use their pinned/cutoff-visible observation
- **AND** a local asset hit that still requires consumer processing or whose prior result is stale SHALL return HTTP 202 with the `consumer_request_id`
- **AND** an asset miss with acquisition permitted SHALL return HTTP 202 with both opaque request handles
- **AND** every asynchronous `202` business-command response for `pending_asset`, parser `queued|processing`, or stale-result reprocessing SHALL include `Location` for the consumer request and a bounded `Retry-After`
- **AND** terminal `missing` projections SHALL not include `Retry-After`
- **AND** a business-command `Location` SHALL always identify the overall consumer-request resource
- **AND** an applicable asset-request URL SHALL appear in a response-body link
- **AND** generic asset ensure alone SHALL use an asset-request `Location`
- **AND** a normal local/confirmed miss or a missing asset with `allow_network=false` SHALL return HTTP 200 with a persisted terminal `missing` consumer projection and `consumer_request_id`, a `Location` for that queryable terminal resource, and no `Retry-After`
- **AND** a missing asset with `allow_network=true` SHALL require `annual_report_assets:acquire`
- **AND** a missing asset with `allow_network=true` and no `annual_report_assets:acquire` scope SHALL return HTTP 403 or configured non-disclosure HTTP 404 without creating work
- **AND** a pre-work temporary provider/storage blocker SHALL return HTTP 503 with an applicable stable reason code and no unintended provider work
- **AND** an ambiguity or current-state conflict SHALL return HTTP 409 with an applicable stable reason code and no unintended provider work
- **AND** every immediate outcome SHALL use an applicable stable reason code from `annual_report_not_found`, `network_disabled`, `provider_unavailable`, `archive_mount_unavailable`, `storage_reserve_exceeded`, `backup_gate_blocked`, `domain_scope_required`, `asset_acquire_scope_required`, `candidate_ambiguous`, `effective_state_conflict`, `idempotency_conflict`, or `consumer_processing_stale`
- **AND** generic asset ensure SHALL NOT start this consumer processing implicitly

#### Scenario: Business command adapters have explicit ownership
- **WHEN** the business-profile or broker risk-control front end starts annual-report-backed processing
- **THEN** it SHALL call its own protected command adapter registered in the OpenAPI snapshot as `POST /api/v1/research/company/{instrument_id}/business-profile/annual-report-process` or `POST /api/v1/research/company/{instrument_id}/broker-risk-control/annual-report-process`; an exact equivalent existing route is allowed only when the snapshot binds it to the corresponding consumer owner
- **AND** the adapter SHALL own canonical processing-fingerprint calculation from registered server-side parser/configuration state, caller idempotency contract, and `consumer_request_id` lifecycle while delegating source discovery and attachment acquisition to the shared asset service
- **AND** generic asset ensure SHALL remain a source-asset-only entry point and SHALL NOT be treated as the business processing entry point

#### Scenario: Business command outcomes are mapped without ambiguity
- **WHEN** a business-profile or broker command reaches an immediate outcome
- **THEN** terminal normal missing or network-disabled SHALL return HTTP 200 with a persisted `consumer_request_id`, consumer-request `Location`, stable reason, and no `Retry-After`
- **AND** accepted asynchronous work SHALL return HTTP 202 with consumer-request `Location` and bounded `Retry-After`
- **AND** missing authentication SHALL return HTTP 401
- **AND** insufficient domain/acquire scope SHALL return HTTP 403 or the configured non-disclosure HTTP 404
- **AND** candidate, effective-state, or idempotency conflict SHALL return HTTP 409
- **AND** rate limiting SHALL return HTTP 429
- **AND** a provider/mount/storage blocker known before work creation SHALL return HTTP 503
- **AND** authentication, authorization, unknown-resource, and cross-owner non-disclosure responses SHALL NOT create a consumer request, asset subscription, or provider work

#### Scenario: Business command authorization is composed safely
- **WHEN** a caller invokes a business-profile or broker risk-control processing command
- **THEN** the adapter SHALL require its domain processing scope before creating a consumer request
- **AND** a verified local asset MAY be processed with the domain scope alone, but a missing asset SHALL contact a provider only when the caller also has `annual_report_assets:acquire`
- **AND** a missing asset with `allow_network=false` MAY create only the caller-owned terminal `missing` consumer projection under the domain scope, without provider or asset work
- **AND** a missing asset with `allow_network=true` and no acquire scope, or any request with no domain scope, SHALL return HTTP 403 or the configured HTTP 404 non-disclosure response before creating unauthorized consumer/asset work

#### Scenario: Business command idempotency is repeated or conflicts
- **WHEN** one principal repeats a business command with the same idempotency key, consumer, selector, processing fingerprint, and normalized body
- **THEN** the adapter SHALL return the same `consumer_request_id` and continuation and SHALL NOT create a second consumer operation
- **AND** reuse of that key with a different consumer, selector, processing fingerprint, or body SHALL return HTTP 409 with a stable idempotency-conflict code and no new work

#### Scenario: A consumer request idempotency record expires
- **WHEN** a consumer-request projection reaches its versioned retention expiry while its principal/idempotency tombstone remains governed
- **THEN** owner polling SHALL return the original `consumer_request_id` with `consumer_request_status=expired` without changing any completed consumer result or its freshness
- **AND** replay with the old key SHALL NOT create consumer or asset work, and a new request SHALL require a new caller idempotency key

#### Scenario: Client submits an invalid selector combination
- **WHEN** an ensure request mixes effective-period and exact-filing selectors, omits one member of `source + source_announcement_id`, supplies both canonical `source_announcement_id` and compatibility alias `filing_id` with different values, supplies an attachment/hash/observation pin without exact-filing identity, supplies inconsistent fiscal-year and report-period values, binds an exact filing to a different path instrument, or supplies a provider URL or filesystem path
- **THEN** the API SHALL reject the request with HTTP 422 and a stable validation code before creating an operation or contacting a provider
- **AND** `source_announcement_id` SHALL be the canonical exact-filing field
- **AND** `filing_id` MAY be accepted only as a versioned compatibility alias
- **AND** an accepted `filing_id` compatibility alias SHALL normalize to `source_announcement_id`
- **AND** the API SHALL reject both fields unless an explicit compatibility policy proves they are identical
- **AND** the two supported selector forms SHALL remain mutually exclusive and all-or-none

#### Scenario: Acquisition request is repeated
- **WHEN** the same normalized scope and policy is submitted again with the same idempotency identity while work is active or its request projection remains within the configured retention period
- **THEN** the API SHALL return the same principal-scoped request subscription and SHALL NOT issue a second provider request or physical write
- **AND** the subscription MAY reference a globally shared internal asset operation without exposing another trigger's ownership or diagnostics

#### Scenario: An idempotency record expires
- **WHEN** a retained request projection reaches its configured expiry while its principal/idempotency fingerprint tombstone remains governed
- **THEN** owner polling SHALL return the same opaque handle with `expired` status and bounded diagnostics, and replay with that key SHALL NOT silently create a second request
- **AND** starting new work SHALL require a new caller idempotency key
- **AND** retention and tombstone durations SHALL be versioned and tested

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
- **AND** the zero-network redacted readiness summary MAY remain available under the ordinary read-only API policy, but provider, filesystem, actor, repair, and operator diagnostics SHALL remain unavailable until the trusted operator boundary is configured

#### Scenario: Caller exceeds an API scope or rate bound
- **WHEN** a client requests a market-wide scope, an unsafe parameter, or exceeds configured request/rate limits
- **THEN** the API SHALL return HTTP 422 with a stable validation code for an invalid/over-broad scope, or HTTP 429 with `Retry-After` for a rate-limit violation
- **AND** it SHALL NOT create an unbounded operation or contact a provider

#### Scenario: Provider or storage is temporarily unavailable
- **WHEN** an authorized bounded ensure cannot be accepted or create durable work because the provider, archive mount, or storage reserve is known to be unavailable before request acceptance
- **THEN** the API SHALL return HTTP 503 with a stable retryable or blocked error code and bounded diagnostics
- **AND** it SHALL create no request subscription or operation
- **AND** it SHALL never publish partial bytes
- **AND** if the request was already accepted before the blocker was detected, the original response SHALL remain HTTP 202 and subsequent owner polling SHALL return HTTP 200 with operation status `blocked`; it SHALL not be retroactively projected as a 503 POST

#### Scenario: Public ensure selects a non-effective exact observation
- **WHEN** a public ensure request resolves to a known non-winning, superseded, withdrawn, or historical exact observation
- **THEN** it SHALL return HTTP 200 with `disposition=local_miss`, no `asset_request_id`, `asset_availability=superseded` for a formerly effective filing or `metadata_only` for a never-effective filing, and `exact_content_state=retained_internal_only|local_content_unavailable` as applicable
- **AND** the response SHALL include the exact filing/observation/hash metadata while creating zero provider, operation, blob, or file work
- **AND** any public asset-id content request for that non-effective asset SHALL remain HTTP 410; retained bytes MAY be read only through the separately authorized internal exact-observation handle

#### Scenario: Client follows operation status
- **WHEN** a client polls its `asset_request_id`
- **THEN** the API SHALL return independent `asset_request_status=active|cancelled|expired`, internal operation status `queued|running|completed|missing|failed|blocked|cancelled`, current stage, disposition, retry metadata, timestamps, progress, result asset id, stable reason codes, and bounded diagnostics
- **AND** DELETE or expiry SHALL change only the caller-owned asset-request projection; it SHALL NOT rewrite the internal operation status, asset result, or another subscription
- **AND** it SHALL separately return asset availability, ensure disposition, and downstream consumer-processing state where applicable
- **AND** the API SHALL project the underlying shared asset operation through that caller's subscription rather than expose the internal operation directly

#### Scenario: Client follows downstream consumer status
- **WHEN** a business command returns a `consumer_request_id` and the owner polls `GET /api/v1/research/annual-report-consumer-requests/{consumer_request_id}`
- **THEN** the API SHALL return consumer identity, processing fingerprint, `consumer_request_status=pending_asset|not_started|queued|processing|completed|failed|missing|blocked|cancelled|expired`, independent `consumer_result_state=unavailable|current|stale|reprocessing`, result identity, retry metadata, timestamps, stable reason codes, and bounded diagnostics
- **AND** `expired` SHALL describe only the caller handle/idempotency projection and SHALL NOT erase or make stale a completed consumer result
- **AND** it SHALL include the linked caller-visible `asset_request_id` only when acquisition was required; a local-hit consumer request SHALL not invent one
- **AND** it SHALL NOT expose internal asset or consumer operation ids, other principals, or filesystem paths
- **AND** an unknown or cross-owner consumer request SHALL follow the same configured 404 non-disclosure and common error-envelope policy as an asset request

#### Scenario: Client cancels one shared acquisition request
- **WHEN** an authorized client cancels its `asset_request_id` while another principal or scheduler still depends on the underlying acquisition
- **THEN** only that request subscription SHALL become cancelled; the linked `consumer_request_id` and pending continuation SHALL remain unchanged and independently queryable/cancellable
- **AND** the internal asset operation SHALL remain active or checkpoint according to remaining subscribers
- **AND** the internal asset operation SHALL NOT expose remaining subscriber identities
- **AND** DELETE SHALL return HTTP 200 with a durable `cancelled` projection
- **AND** repeated DELETE SHALL return the same outcome
- **AND** a later owner GET SHALL remain queryable for audit

#### Scenario: Client cancels the last shared acquisition request
- **WHEN** the last principal cancels its `asset_request_id` after bounded internal acquisition work exists
- **THEN** version 1 SHALL cancel only that subscription while the internal acquisition continues to a bounded terminal state and any linked consumer continuation remains governed by its own request
- **AND** consumer processing that already started SHALL not be cancelled through the asset request
- **AND** its own domain stop contract SHALL apply
- **AND** the stop SHALL be explicitly rejected when that contract cannot stop the processing
- **AND** an unknown or cross-owner request SHALL follow the configured HTTP 404 non-disclosure policy

#### Scenario: Client cancels a consumer request
- **WHEN** an authorized owner deletes a `consumer_request_id`
- **THEN** a not-yet-started continuation SHALL be cancelled idempotently with HTTP 200, remain queryable as `cancelled`, and return the same projection on repeated DELETE
- **AND** already-started consumer processing SHALL use that consumer domain's authorized cooperative-stop contract
- **AND** an accepted cooperative stop SHALL return HTTP 202
- **AND** an unsupported or current-state-invalid stop SHALL return HTTP 409
- **AND** already-started consumer processing SHALL NOT be force-cancelled through request-subscription deletion
- **AND** terminal `completed|missing|failed|expired` requests, any request with a current result, and blocked requests whose consumer processing has already started and cannot cooperatively stop SHALL return HTTP 409 `request_not_cancellable`; a blocked continuation that has not started MAY be cancelled with HTTP 200 while preserving blocker/retry/audit evidence
- **AND** deleting a consumer request SHALL NOT detach, cancel, or rewrite its linked `asset_request_id` or internal acquisition; the caller must delete that asset request separately when desired
- **AND** repeated DELETE of an already-cancelled request SHALL return HTTP 200 with the same cancelled projection
- **AND** unknown or cross-owner identifiers SHALL follow the configured HTTP 404 non-disclosure policy

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
- **AND** local byte validity SHALL NOT start new default-effective business processing
- **AND** the command SHALL keep the result stale or pending correction
- **AND** exact-observation or knowledge-cutoff processing MAY proceed only when its evidence scope remains valid

#### Scenario: Client downloads an asset
- **WHEN** a client requests an available asset by id
- **THEN** the API SHALL validate the effective record and file integrity before streaming with a safe filename, `application/pdf`, and verified Content-Length
- **AND** it SHALL not accept a caller-supplied filesystem path

#### Scenario: Client requests a superseded or corrupt asset
- **WHEN** a content request resolves to a superseded, missing, or integrity-failed file
- **THEN** the public asset-id content request SHALL return HTTP 410 for a known superseded/deleted asset; any retained non-effective byte remains accessible only through the internal DataManager/service exact-observation controlled-handle contract, not this API route
- **AND** a current asset whose bytes fail integrity SHALL return HTTP 409, using stable error codes
- **AND** it SHALL never return HTTP 409 for a known superseded/deleted asset
- **AND** the public route SHALL NOT stream stale bytes

#### Scenario: API outcomes are mapped deterministically
- **WHEN** a client receives a normal missing metadata result, an unknown resource, an authentication or permission failure, a malformed selector, a rate limit, or a temporary provider/storage blocker
- **THEN** a metadata query with no local report SHALL return HTTP 200 with structured availability
- **AND** an unknown asset or operation SHALL return HTTP 404
- **AND** a configured trusted boundary SHALL use HTTP 401 for missing authentication, HTTP 403 for insufficient scope, or the documented HTTP 404 non-disclosure policy for cross-owner operation lookup
- **AND** selector validation SHALL use HTTP 422, rate limits HTTP 429 with `Retry-After`, and temporary infrastructure blockers HTTP 503 with a stable retryable or blocked code
- **AND** an asynchronous request accepted before a later provider, mount, or storage blocker SHALL remain queryable through HTTP 200 status GET with `status=blocked`; the original POST SHALL NOT be retroactively reclassified as HTTP 503
- **AND** every non-success response SHALL use the versioned common error envelope rather than provider exception text

#### Scenario: AI/API caller integration is accepted
- **WHEN** rollout evaluates the project's API-only consumer integration
- **THEN** it SHALL register `client_mode=ai_api_only`, the frozen backend candidate, bound OpenAPI contract version, reproducible state/polling/content test evidence, and gate status
- **AND** the acceptance evidence SHALL prove an authorized caller can explicitly request acquisition while an unauthorized caller is denied before lookup, provider, operation, or file work
- **AND** the acceptance evidence SHALL prove an active request idempotently suppresses duplicate submission for the same normalized selector
- **AND** the acceptance evidence SHALL prove polling honors the server's bounded `Retry-After` cadence and follows the returned caller-owned `Location` resource
- **AND** asset responses SHALL expose a non-null `content_url` only for `asset_availability=local_valid` and `null` for metadata-only, missing, ambiguous, corrupt, superseded, blocked, or retained-internal-only content
- **AND** the API-client gate SHALL be independent from production rollout and consumer-cutover gates; no external Web UI repository or deployment is required
- **AND** the acceptance evidence SHALL prove the asset-id content endpoint validates authorization, current state, integrity, and safe response headers without exposing a server path

### Requirement: Consumer Outputs Surface Asset Lineage
Business-facing results that depend on annual reports SHALL expose sufficient shared asset lineage for audit without leaking internal archive paths.

#### Scenario: A business action has an immediately valid asset
- **WHEN** a front-facing business-profile or broker command resolves a verified local annual-report asset
- **THEN** that business orchestration SHALL create or reuse exactly one consumer operation for the requested consumer and processing fingerprint without creating an asset-acquisition operation
- **AND** it SHALL pass the resolved shared asset id as the source-asset input to that consumer operation
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
- **THEN** its source lineage SHALL identify the shared asset id, source filing, report period, content hash, effective-correction state, broker processing manifest, and `current|stale|reprocessing` consumer-processing status
- **AND** a stale or reprocessing broker fact SHALL NOT be returned as unqualified current data or admitted into a DCF input contract that requires current facts
