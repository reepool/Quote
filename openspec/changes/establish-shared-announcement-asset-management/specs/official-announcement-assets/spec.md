## ADDED Requirements

### Requirement: Announcement Asset Management Is Business-Neutral
The system SHALL provide an official announcement asset capability that is independently configured, persisted, scheduled, and callable without enabling business-profile, broker risk-control, or any other consuming business module.

#### Scenario: Business-profile is disabled
- **WHEN** business-profile acquisition and daily processing are disabled
- **THEN** annual-report backfill, daily discovery, attachment download, integrity management, and consumer access SHALL remain available

#### Scenario: A new consumer needs annual reports
- **WHEN** a future business module needs a formal annual-report attachment
- **THEN** version 1 architecture-contract tests SHALL prove a registered test consumer can use the shared identity/blob/acquisition interfaces without importing a provider scanner, attachment downloader, archive layout, or revision store; future production consumers require their own separately approved change and cutover evidence
- **AND** after that consumer's cutover gate is enabled, it SHALL perform shared local-first lookup/ensure before any source work and SHALL NOT call provider retrieval directly or persist a private original-attachment archive for a document family managed by the shared capability

#### Scenario: Application process starts
- **WHEN** the application, API, DataManager, asset service, or scheduler process initializes
- **THEN** startup SHALL perform zero provider scans, attachment downloads, migration moves/links/quarantine, durable work dispatch, or physical deletions
- **AND** initialization SHALL be limited to dependency registration, schema/configuration validation, and other local non-destructive setup

### Requirement: Existing Announcement Acquisition Infrastructure Is Reused
The asset capability SHALL use the existing source-neutral announcement providers, route configuration, normalized records, conservative cursors, and governed attachment retrieval rather than introducing parallel source transports.

#### Scenario: CNInfo annual-report discovery runs
- **WHEN** the asset service searches A-share annual reports
- **THEN** it SHALL construct a normalized announcement query and use configured provider capabilities and routes
- **AND** it SHALL NOT hard-code CNInfo `column`, `plate`, `orgId`, artifact hosts, headers, TLS behavior, or fallback logic in the asset service

#### Scenario: Provider fallback is attempted
- **WHEN** the primary source fails and the configured route permits fallback
- **THEN** all attempts and diagnostics SHALL be retained while the selected source record remains source-qualified

### Requirement: Canonical Announcement And Attachment Identities Are Preserved
The system SHALL distinguish legal announcement identity, attachment observation identity, physical content identity, and consumer-processing identity.

The canonical schema SHALL retain provider diagnostics/status on announcements and immutable retrieval evidence on attachment versions, including `version_available_at`, time source/precision, lease owner/generation, attempt/max-attempt, `next_retry_at`, error classification, and temporary/quarantine evidence.
Temporary paths SHALL never be treated as effective-asset identity.
Mutable parser state SHALL never be treated as effective-asset identity.

#### Scenario: Canonical schema is created or migrated
- **WHEN** the announcement-asset schema is created on a clean database or upgraded in place
- **THEN** announcement records SHALL include stable internal and source-qualified ids, title, instrument/exchange/source category, raw and normalized publication times, first/last observed times, raw-payload hash, provider diagnostics/status, and schema timestamps
- **AND** attachment records SHALL include source attachment or normalized-URL identity, original URL, filename, media-type and content-length hints, first/last observation, and current metadata without parser state
- **AND** blob and attachment-version records SHALL include content hash/length/PDF/integrity state, controlled path, adoption/acquisition/verification/backup evidence, immutable observation/retrieval evidence, retry/lease state, and transactional retention-pin relations
- **AND** effective-report records SHALL include instrument/fiscal-year uniqueness, report period, selected legal filing/attachment/observation/blob, variant/full-report/classifier evidence, predecessor/activation/last-checked state, equivalent-filing evidence-set hash, and canonical-projection policy version
- **AND** every effective transition SHALL append an immutable scope-qualified decision/replacement record containing predecessor and replacement legal filing, attachment observation, asset/blob identity, decision and policy versions, reason, activation time, and matching outbox event key; overwriting the current effective projection SHALL NOT be the only record of predecessor/replacement lineage
- **AND** operation, subscription, deletion-intent/audit, recovery-manifest, recovery-pair closure, backup recovery-journal, change-event, and consumer-checkpoint records SHALL retain their normalized bounds, progress/checkpoint, ownership, idempotency, retry, watermark, lineage, and audit fields required by their versioned contracts, including recovery-retention-pin `blocks_primary_unlink` and `required_set_hold` transition state
- **AND** migration tests SHALL verify these fields and constraints without altering existing business-profile or financial-fact table contracts

#### Scenario: Operational evidence survives repeated observation and retry
- **WHEN** an announcement or attachment is rediscovered, retried, quarantined, or adopted
- **THEN** first/last observation, provider diagnostics/status, attempt history, lease generation, retry schedule, error classification, and temporary evidence SHALL remain queryable without overwriting immutable prior observations
- **AND** a `.part` path SHALL never satisfy local availability or effective selection

#### Scenario: Announcement has a stable provider id
- **WHEN** an official source returns a stable announcement id
- **THEN** the announcement key SHALL be `source + source_announcement_id`

#### Scenario: One announcement has multiple attachments
- **WHEN** an announcement exposes multiple attachments
- **THEN** each attachment SHALL have a stable source-qualified identity using attachment id when available or a deterministic normalized source URL identity otherwise

#### Scenario: Two filings contain identical bytes
- **WHEN** two source-qualified attachments have the same SHA-256 content
- **THEN** they SHALL share one physical blob after canonical migration or acquisition converges
- **AND** their legal announcement and attachment identities SHALL remain distinct

#### Scenario: A known announcement is observed again
- **WHEN** the same source-qualified announcement or attachment is rediscovered
- **THEN** the canonical record SHALL be updated idempotently with first/last-observed evidence and raw metadata identity
- **AND** it SHALL NOT create a duplicate legal announcement or overwrite prior attachment-version evidence

#### Scenario: Effective decisions change repeatedly
- **WHEN** an original is replaced by one or more corrections, a same-hash distinct legal filing is selected, or a withdrawal changes the effective decision
- **THEN** each transition SHALL append exactly one immutable decision/replacement record in the same transaction as the current projection and outbox event
- **AND** the complete ordered lineage SHALL remain reconstructable after later transitions, predecessor unlink, migration, and paired restore without inferring it only from mutable current rows, deletion intents, or backup manifests
- **AND** decision-history records SHALL be append-only and uniqueness-constrained by their scope, transition identity, and outbox event key

#### Scenario: Attachment bytes or withdrawal state appear after the announcement
- **WHEN** a provider silently changes an attachment or first exposes a withdrawal/cancellation state after the parent announcement publication time
- **THEN** the system SHALL create an immutable observation with `version_available_at`, original time value, time source, and time precision
- **AND** `version_available_at` SHALL use an official effective time when available or otherwise the first-observed time
- **AND** a knowledge-cutoff query SHALL NOT expose the new bytes or withdrawal state before that observation was available

#### Scenario: One asset is parsed by two consumers
- **WHEN** business-profile and broker risk-control process the same annual-report asset
- **THEN** they SHALL share the source asset and SHALL retain separate consumer, parser-version, parameter-hash, status, and derived-output records

### Requirement: Formal Full Annual Reports Are Classified Centrally
Version 1 SHALL centrally classify A-share formal full annual reports, annual-report corrections, summaries, translations, correction notices, and related non-report announcements using a versioned deterministic policy.

#### Scenario: Canonical classification vocabulary crosses providers and consumers
- **WHEN** a provider category, migrated manifest, shared asset, or consumer adapter represents an annual or future semiannual report
- **THEN** it SHALL map through a versioned policy to orthogonal `document_family=annual_report|semiannual_report|...`, `variant=original|correction`, and `is_full_report` fields
- **AND** `correction` SHALL NOT be treated as a peer document family
- **AND** a notice without a complete replacement body SHALL remain correction evidence
- **AND** a notice without a complete replacement body SHALL NOT become an eligible corrected report

#### Scenario: Formal annual report is found
- **WHEN** a title denotes a complete `年度报告` or accepted `年报` abbreviation and a PDF attachment is present
- **THEN** the record SHALL be classified as a formal full annual report with its fiscal year and report period

#### Scenario: Annual-report summary is found
- **WHEN** the title denotes an annual-report summary
- **THEN** it SHALL NOT be eligible as the effective full-report asset

#### Scenario: Translation or visual edition is found
- **WHEN** the title denotes an English translation, illustrated edition, or another non-primary rendition
- **THEN** it SHALL NOT replace the primary Chinese full report

#### Scenario: Related material or unresolvable identity is found
- **WHEN** an attachment is an audit or assurance report, inquiry or reply, briefing material, semiannual or quarterly report, or lacks a reliable instrument or fiscal-year identity
- **THEN** it SHALL NOT be eligible as the version 1 effective annual-report asset

#### Scenario: Correction notice has no full replacement report
- **WHEN** a correction announcement contains only a notice or changed pages and not a complete corrected annual-report attachment
- **THEN** it SHALL remain correction evidence and SHALL NOT replace the effective full-report attachment

#### Scenario: Complete corrected report is found
- **WHEN** a later announcement contains a complete corrected or revised annual-report PDF for the same instrument and fiscal year
- **THEN** it SHALL be eligible to supersede the prior full report

#### Scenario: One announcement has mixed attachments
- **WHEN** one announcement contains a complete primary Chinese report, summary, translation, appendix, and correction notice attachments
- **THEN** classification SHALL occur at attachment level
- **AND** only the complete primary Chinese report SHALL be eligible

#### Scenario: Cross-source candidates conflict
- **WHEN** official-source candidates for the same instrument and fiscal year have different verified content and no governed mirror or legal-precedence evidence
- **THEN** selection SHALL fail closed as ambiguous
- **AND** lexical source or filing-id order SHALL NOT choose the effective report

#### Scenario: A conflicting candidate appears after an asset is already serving
- **WHEN** a newly verified candidate conflicts with the currently serving winner and no governed precedence can resolve it
- **THEN** the effective decision SHALL transition to `ambiguous`
- **AND** the service SHALL retain the prior bytes, references, and consumer lineage
- **AND** no predecessor deletion intent SHALL be created
- **AND** the prior file MAY remain locally available only as explicitly provisional and SHALL NOT be presented as an unqualified final winner or trigger new consumer processing
- **AND** version 1 SHALL suppress new default-effective consumer processing
- **AND** version 1 SHALL project existing default-effective results as `stale` with reason `pending_correction` and exclude them from DCF contracts requiring current facts
- **AND** exact-observation and knowledge-cutoff results SHALL remain governed by their own evidence scope
- **AND** this provisional policy/version SHALL be configuration-fingerprinted
- **AND** any later policy change SHALL require explicit migration evidence
- **AND** conflict resolution SHALL converge through one transactional decision and replayable outbox event

#### Scenario: Cross-source candidates have equivalent content
- **WHEN** multiple source-qualified annual-report candidates are proven equivalent by identical verified content or governed mirror evidence
- **THEN** the effective decision SHALL retain every distinct legal filing in a stable `equivalent_source_filings` evidence set rather than merge their identities
- **AND** any required single `canonical_source_filing` projection SHALL be selected by a versioned discovery-order-independent policy, with its policy version and evidence-set hash retained in asset and consumer lineage
- **AND** observing an additional equivalent mirror SHALL NOT silently change an existing consumer-processing identity

#### Scenario: Cross-source equivalence needs bounded byte verification
- **WHEN** a route exposes multiple legal candidates but does not provide a trusted content hash or mirror/legal-precedence proof
- **THEN** the service MAY perform a bounded, explicitly policy-authorized temporary candidate verification to establish equivalence or conflict
- **AND** only the selected winner SHALL be published, retained for coverage, or counted as an effective asset
- **AND** non-winner temporary bytes SHALL never create a canonical blob/effective asset or coverage credit
- **AND** non-winner temporary bytes SHALL obey size, reservation, and mount gates
- **AND** the service SHALL persist their hash, length, retrieval evidence, verification-policy version, and cleanup outcome as immutable attachment-observation/version evidence before cleanup
- **AND** the service SHALL remove or governed-quarantine non-winner temporary bytes after that immutable evidence is persisted

#### Scenario: Same-source corrections have tied publication time and different bytes
- **WHEN** two complete corrections from one source have the same normalized publication time but different verified content
- **THEN** only an explicit provider replacement edge, official revision sequence, or other versioned legal-precedence evidence SHALL choose the winner
- **AND** absent that evidence selection SHALL fail closed as ambiguous and SHALL NOT infer precedence from filing or attachment id order

#### Scenario: Same-source equivalent corrections have tied publication time
- **WHEN** complete corrections in the same proven legal chain have the same normalized publication time and identical verified content
- **THEN** a versioned stable announcement-and-attachment identity tie-break policy MAY select the canonical filing projection while preserving every legal identity and the shared content hash
- **AND** reverse discovery order SHALL produce the same winner/projection, policy version, and evidence-set hash
- **AND** this identity tie-break SHALL NOT apply when the verified bytes differ or the common legal chain cannot be proven

#### Scenario: Effective correction is withdrawn or silently changed
- **WHEN** a source withdraws an effective correction or changes the attachment observed under an existing announcement id
- **THEN** the system SHALL retain a new observation and reevaluate the effective asset
- **AND** it SHALL only fall back to a legally valid locally available predecessor or SHALL commit the explicit no-winner withdrawal decision while coverage/readiness becomes blocked

#### Scenario: A governed withdrawal leaves no valid local replacement
- **WHEN** a withdrawal is legally bound to the current effective filing and no legally valid predecessor remains locally available
- **THEN** one serialized activation transaction SHALL append a `withdrawn_without_replacement` decision with nullable replacement filing/observation/asset/blob fields, clear the current-effective projection or replace it with an explicit no-winner tombstone, create the withdrawal outbox event, and prevent any falsely current winner
- **AND** the withdrawn asset SHALL become non-consumer-visible immediately, while its primary bytes remain until the withdrawal deletion intent, retention pins, backup, recovery manifest, and audit gates complete
- **AND** the deletion intent SHALL reserve a recovery pair id
- **AND** the withdrawn predecessor SHALL be backed up before the withdrawal recovery manifest is created
- **AND** an immutable `withdrawal_tombstone` manifest SHALL bind that recovery pair id, predecessor, decision/outbox key, backup object, and file watermark
- **AND** a recoverable catalog snapshot SHALL contain the immutable withdrawal tombstone
- **AND** a separate append-only pair-closure record SHALL complete the pair after the catalog snapshot contains the tombstone
- **AND** recovery-pin conversion or unlink SHALL occur only after that pair-closure record is durable
- **AND** because no replacement blob exists, the required backup set SHALL contain the withdrawn predecessor and compatible no-winner catalog snapshot rather than fabricate a replacement
- **AND** a withdrawal that falls back to a valid local predecessor SHALL use the ordinary predecessor/replacement pair workflow, treating the withdrawn asset as the deletion predecessor and the fallback winner as the replacement
- **AND** restore and final-invariant checks SHALL accept nullable replacement identity only for this explicit decision kind and SHALL reject dangling, contradictory, or consumer-visible withdrawn winners

#### Scenario: Withdrawal target cannot be proven
- **WHEN** a withdrawal or cancellation record cannot be bound to a candidate through provider state, target announcement/attachment identity, an official relation, or a versioned deterministic rule
- **THEN** the system SHALL retain the record as unresolved evidence and SHALL NOT deactivate the effective asset based only on generic title wording
- **AND** the affected decision SHALL expose ambiguity for review

### Requirement: One Effective Annual-Report Attachment Is Retained Per Fiscal Year
For each instrument and fiscal year with at least one legally valid, complete, and available candidate, the system SHALL expose exactly one effective formal annual-report attachment after convergence, preferring a valid complete correction over a non-correction and the latest valid correction over earlier corrections.
A period proven to have no legal winner, including `withdrawn_without_replacement`, SHALL expose no current attachment.
The system SHALL retain an explicit no-winner decision for a period proven to have no legal winner.
The system SHALL NOT fabricate or continue serving a winner for a period proven to have no legal winner.

#### Scenario: Only an original report exists
- **WHEN** one valid original full annual report exists for an instrument and fiscal year
- **THEN** it SHALL be the effective asset

#### Scenario: Correction arrives after a predecessor
- **WHEN** a complete corrected report is downloaded and passes identity, PDF signature, byte-length, and SHA-256 verification
- **THEN** the system SHALL atomically activate the correction
- **AND** it SHALL mark the original or earlier-correction attachment record superseded
- **AND** it SHALL invalidate or requeue consumer processing bound to the predecessor asset

#### Scenario: New correction fails verification
- **WHEN** a discovered correction cannot be downloaded or fails integrity validation
- **THEN** the predecessor SHALL remain the selected local asset and physically present
- **AND** its effective-decision state SHALL become `provisional` with pending-correction identity/reason rather than remain an unqualified final active winner
- **AND** the correction SHALL remain `retryable` for transient acquisition failures or `blocked/operator_action_required` for deterministic identity/integrity failures, with explicit diagnostics under the common retry taxonomy
- **AND** new default-effective consumer processing SHALL remain suppressed
- **AND** existing default-effective results SHALL become `stale/pending_correction`
- **AND** exact-observation or knowledge-cutoff results SHALL remain governed by their evidence scope
- **AND** the provisional predecessor bytes MAY remain locally usable under the stated policy
- **AND** the instrument SHALL retain `retryable|blocked` latest-only coverage until the correction is verified or the newer candidacy is resolved
- **AND** the instrument SHALL NOT receive `available|current` bootstrap coverage credit until the correction is verified or the newer candidacy is resolved

#### Scenario: Post-activation work fails
- **WHEN** the correction activation transaction commits but change-event delivery, consumer invalidation, backup, or predecessor unlink subsequently fails
- **THEN** the verified correction SHALL remain the effective winner and SHALL NOT roll back to the predecessor
- **AND** the activation transaction SHALL already contain an immutable replayable change-event outbox record
- **AND** the failed downstream step SHALL remain durably retryable
- **AND** the predecessor SHALL stay physically present until every deletion gate passes

#### Scenario: Activation cannot persist its event outbox
- **WHEN** effective-row activation, replacement-edge insertion, a required deletion-intent insertion (or the same-hash non-unlink audit), or change-event outbox insertion fails before transaction commit
- **THEN** the entire activation transaction SHALL roll back and the predecessor SHALL remain effective and physically present
- **AND** no partial replacement state SHALL be externally visible

#### Scenario: Corrections finish acquisition out of publication order
- **WHEN** two complete corrections for the same instrument and fiscal year are acquired or activated concurrently and the older correction finishes last
- **THEN** activation SHALL serialize at the `instrument + fiscal_year` decision scope and reselect the winner from all committed observations inside the activation transaction
- **AND** a stale worker SHALL fail its compare-and-swap or otherwise leave the newer valid correction effective
- **AND** no deletion intent SHALL target the actual current winner

#### Scenario: Superseded file is no longer referenced
- **WHEN** the corrected report is active and the superseded physical blob has no remaining non-recovery retention pin that blocks primary unlink
- **THEN** the superseded physical file SHALL be deleted only after both predecessor and replacement blobs are verified in the independent backup and paired catalog recovery watermark
- **AND** activation SHALL reserve a stable `recovery_pair_id`
- **AND** predecessor and replacement backup verification SHALL complete before the recovery-manifest entry is created
- **AND** the immutable, indefinitely active version 1 recovery-manifest entry SHALL bind that id to the legal/attachment identity, prior path/hash, replacement, backup object, and verified file-manifest watermark
- **AND** the recovery-manifest entry SHALL NOT claim that the later catalog snapshot is already closed
- **AND** a subsequent recoverable catalog snapshot SHALL include that immutable manifest
- **AND** an append-only recovery-pair closure record SHALL bind `recovery_pair_id`, catalog snapshot identity/hash, and file-manifest watermark after verifying both directions of the pair
- **AND** the recovery pin SHALL be compare-and-swap converted to a non-primary-blocking permanent backup `required_set_hold` only after the closure record is durable
- **AND** no crash boundary SHALL lose both the recovery pin and permanent backup hold forms of protection
- **AND** the immutable manifest SHALL never be rewritten to manufacture the closure
- **AND** its announcement metadata, content hash, size, prior archive path, replacement edge, deletion reason, and deletion timestamp SHALL remain auditable

#### Scenario: Superseded blob is shared
- **WHEN** another active source-qualified attachment references the same physical blob
- **THEN** the system SHALL remove only the superseded annual-report reference
- **AND** the system SHALL retain the primary shared blob until all primary/deletion-blocking pins are released
- **AND** a non-primary backup `required_set_hold` SHALL protect the verified backup copy rather than prevent primary unlink
- **AND** when predecessor and replacement legal filings resolve to the same content hash, activation SHALL record the legal replacement and reference change
- **AND** activation SHALL NOT create a physical-unlink deletion intent for that shared blob
- **AND** deletion audit SHALL record a non-applicable shared-blob outcome rather than claim an unlink

#### Scenario: A physical retention pin remains
- **WHEN** a blob is held by another effective attachment, a managed legacy alias, a not-yet-migrated consumer, or an active read or processing lease
- **THEN** the physical file SHALL remain even though historical metadata does not itself require physical retention
- **AND** a predecessor retained only by a migration alias SHALL not be exposed as current-effective, latest-only coverage, or a second consumer-visible annual report

#### Scenario: A retention lease expires
- **WHEN** a read or processing lease passes its TTL
- **THEN** its retention pin SHALL remain until compare-and-swap reconciliation checks owner, heartbeat, generation, and the configured safety grace period
- **AND** a newer heartbeat or uncertain live reader SHALL continue to block deletion
- **AND** an abandoned lease SHALL be reclaimed idempotently

#### Scenario: Process stops during predecessor deletion
- **WHEN** a process stops before or after unlinking a predecessor file
- **THEN** a durable `planned`, `deleting`, `deleted`, or `failed` deletion intent SHALL allow idempotent reconciliation
- **AND** before finalizing `deleted`, the reconciler SHALL revalidate the operation-captured approved mount identity/source/read-write state and confirm the path is absent on that same mount
- **AND** a changed, unavailable, or unverifiable mount SHALL keep the intent `deleting` while readiness/cleanup reports a `blocked` finalization condition; absence on a fallback or different mount SHALL NOT be accepted as deletion evidence
- **AND** audit state SHALL NOT claim physical deletion before it is confirmed

### Requirement: Local-First Ensure Is The Consumer Contract
The system SHALL provide a local-first `ensure` contract that returns a verified effective annual-report asset when available and performs bounded discovery and download only when permitted and necessary.

#### Scenario: Valid local asset exists
- **WHEN** a consumer requests an instrument and fiscal year whose effective attachment exists and passes configured integrity validation
- **THEN** the service SHALL return the local asset without a provider request or attachment download

#### Scenario: Metadata exists but attachment is absent
- **WHEN** eligible annual-report candidate metadata exists for the requested selector but no locally valid current attachment has been activated
- **THEN** an authorized ensure request SHALL acquire and verify the best eligible candidate and only then transactionally reselect, activate, atomically archive, and return it
- **AND** a metadata-only candidate SHALL NOT be labeled current or effective before successful attachment validation and activation

#### Scenario: Neither metadata nor attachment exists
- **WHEN** an authorized consumer requests a missing annual report with network acquisition allowed
- **THEN** the service SHALL execute bounded instrument-scoped discovery, register all relevant normalized metadata, select the effective full report, download it, and return the asset

#### Scenario: Caller prohibits network access
- **WHEN** a consumer requests a missing asset with network acquisition disabled
- **THEN** the service SHALL return `local_miss` disposition with explicit missing availability
- **AND** the service SHALL NOT create an operation
- **AND** the service SHALL NOT contact a provider

#### Scenario: Requested period is ambiguous
- **WHEN** discovery cannot determine one effective full report for the requested instrument and fiscal year
- **THEN** the service SHALL fail closed with candidate identities and reasons rather than returning an arbitrary attachment

#### Scenario: Exact legal filing is requested
- **WHEN** a consumer requests a source-qualified announcement or filing identity
- **THEN** ensure SHALL return that exact filing from already-retained eligible bytes, or acquire it only when the observation is eligible under the current selection and retention policy, including an initial metadata-only candidate whose successful validation would make it the period winner, and acquisition is permitted by the caller's network and integrity policy
- **AND** it SHALL NOT substitute a merely similar or newer legal filing

#### Scenario: Exact legal filing metadata is absent locally
- **WHEN** an authorized network-enabled exact-filing request names a `source + source_announcement_id` that is not registered locally
- **THEN** ensure SHALL use a provider-supported exact-id lookup or a bounded source-qualified instrument/category/date discovery scope capable of proving that requested identity
- **AND** the lookup SHALL preserve the requested source and legal announcement id as the only successful selector result
- **AND** other records returned by the bounded query MAY be registered as metadata evidence but SHALL NOT be substituted for the requested filing
- **AND** a complete exact no-match SHALL return explicit exact-filing metadata-missing status without downloading a same-period alternative
- **AND** an incomplete provider scope SHALL remain retryable or blocked
- **AND** an incomplete provider scope SHALL NOT be converted into a terminal not-found result
- **AND** a network-disabled exact-filing metadata miss SHALL perform zero provider, operation, blob, or file work

#### Scenario: Exact filing request pins an attachment observation
- **WHEN** an exact-filing request supplies an attachment id plus expected content hash or observation version
- **THEN** ensure SHALL return only that matching immutable observation or explicit metadata-only unavailable status
- **AND** it SHALL NOT substitute bytes from a later silent update under the same legal filing
- **AND** an unpinned exact-filing request SHALL explicitly return the filing's current observation identity, hash, and `version_available_at`

#### Scenario: Exact filing is a deleted predecessor
- **WHEN** the exact source-qualified filing is known to be superseded or withdrawn and its bytes were deleted under version 1 retention
- **THEN** ordinary consumer and public API ensure SHALL return its metadata with explicit `local_content_unavailable` status
- **AND** it SHALL NOT redownload the predecessor or recreate a second physical attachment for that instrument and fiscal year

#### Scenario: Exact filing is not the current effective observation
- **WHEN** a public API or ordinary consumer requests a known non-winning, superseded, withdrawn, or historical attachment observation for an instrument and fiscal year, whether or not those bytes were ever downloaded
- **THEN** public ensure SHALL return HTTP 200 with `disposition=local_miss`, no `asset_request_id`, `asset_availability=superseded` for a formerly effective filing or `metadata_only` for a never-effective filing, and `exact_content_state=retained_internal_only` when exact bytes remain under a legal internal pin or `local_content_unavailable` otherwise
- **AND** the public asset-id content route SHALL return HTTP 410
- **AND** the public asset-id content route SHALL NOT stream the retained byte
- **AND** only an authorized internal DataManager/service exact-observation handle MAY return already-retained eligible bytes after exact identity/integrity checks
- **AND** neither path SHALL contact a provider, create an acquisition operation, publish another blob, or recreate a second consumer-visible physical attachment for that instrument and fiscal year
- **AND** disaster-recovery retrieval or exceptional legal preservation SHALL require a separate bounded operator policy and SHALL NOT make that observation current-effective

#### Scenario: Inactive or delisted history is requested
- **WHEN** an authorized caller with the required acquire scope explicitly requests an inactive or delisted instrument under the bounded on-demand policy outside the default active-universe bootstrap
- **THEN** the same local-first ensure contract SHALL perform the permitted bounded lookup or acquisition without broadening the default scheduled universe
- **AND** it SHALL NOT expand bootstrap or daily denominators, create a scheduled cursor scope, or trigger a scan of the historical delisted universe

### Requirement: Latest-Only Historical Backfill Covers The Active A-Share Universe
The system SHALL provide a resumable bootstrap that targets current active stock instruments on SSE, SZSE, and BSE.
The bootstrap SHALL publish, persistently retain, and count coverage only for the latest available effective annual-report attachment for each instrument that has a provably published report.
An eligible instrument without a published report SHALL instead receive an explicit evidence-backed terminal or retry outcome.
An eligible instrument without a published report SHALL NOT be represented by a fabricated asset.
A bounded policy MAY temporarily verify competing candidate bytes when trusted hash or precedence evidence is unavailable.
The bounded verification policy SHALL NOT publish or retain a non-winner as a second canonical asset.
Verifiable older local files MAY be registered for explicit period-specific reuse.
Verifiable older local files SHALL NOT expand latest-only coverage.
Verifiable older local files SHALL NOT trigger adjacent historical network acquisition.

#### Scenario: Bootstrap discovery uses market windows before targeted repair
- **WHEN** latest-only bootstrap discovers candidate annual reports for the active-universe snapshot
- **THEN** it SHALL first scan the current and previous disclosure seasons through bounded date windows partitioned by market and normalized annual-report category
- **AND** it SHALL use single-instrument discovery only for a bounded, progressively shrinking missing-instrument cohort after market-window results are reconciled

#### Scenario: Version 1 universe eligibility is evaluated
- **WHEN** the bootstrap or daily coverage denominator is materialized
- **THEN** a versioned eligibility policy SHALL include active RMB-denominated A-share instruments on the main boards, STAR Market, ChiNext, and BSE, including ST or suspended-but-not-delisted stocks
- **AND** it SHALL exclude B shares, depositary receipts/CDRs even if loosely typed as stock, funds and ETFs, bonds, indices, and other non-A-share security types
- **AND** the resulting instrument identities, policy version, source master-data version, master-data last-success time, snapshot time, and paired listed-security-census snapshot id SHALL be persisted so the denominator is auditable
- **AND** a local-master-only snapshot MAY support bounded work but SHALL NOT by itself produce full-market completion or ready status before the paired census contract is satisfied

#### Scenario: Universe refresh is stale incomplete or fails
- **WHEN** master-data refresh fails, exceeds its independently configured master-data freshness limit, returns a partial result, or leaves security type, currency, exchange, or active-state eligibility indeterminate
- **THEN** the service SHALL retain the last complete acceptable snapshot instead of replacing it with an empty or partial denominator
- **AND** indeterminate instruments, freshness age, refresh failure, and missing fields SHALL remain explicit readiness evidence
- **AND** market announcement discovery MAY continue, but bootstrap SHALL NOT claim complete full-market coverage while no acceptable snapshot exists or eligibility remains indeterminate

#### Scenario: Local master data omits a listed security
- **WHEN** bootstrap or daily universe refresh compares the local instrument snapshot with an independent approved SSE, SZSE, and BSE listed-security census
- **THEN** the system SHALL persist the census source, exchange scope, query boundary, completeness watermark or version, acquisition time, raw-content hash, and normalized identity differences
- **AND** a census-only security, unresolved identity, missing target exchange, or stale/incomplete census SHALL enter `eligibility_indeterminate`
- **AND** an `eligibility_indeterminate` condition SHALL prevent a full-market completion claim
- **AND** failure of either side SHALL retain the last acceptable paired master-data/census snapshot rather than silently shrink the denominator
- **AND** master-data and census maximum ages SHALL be separately configured and persisted
- **AND** either side exceeding its maximum age SHALL independently make the pair stale
- **AND** one fresh side SHALL NOT mask an expired counterpart

#### Scenario: Instrument has several historical annual reports
- **WHEN** the bootstrap discovers multiple fiscal years for an instrument
- **THEN** it SHALL create one bootstrap effective record and physical attachment only for the latest available fiscal-year winner
- **AND** it MAY retain non-winning discovery metadata for audit without downloading those attachments

#### Scenario: Bootstrap derives fiscal-year search bounds
- **WHEN** bootstrap evaluates an instrument at a fixed `as_of`
- **THEN** a versioned policy SHALL deterministically derive the candidate upper year, disclosure-due year, and earliest searchable year from project timezone, fiscal-year end, listing date, configured disclosure-calendar boundary, provider coverage start, and bounded lookback
- **AND** the version 1 default for a calendar-year A-share report SHALL use April 30 of the following year as the disclosure-due boundary, with any governed calendar override changing the policy fingerprint
- **AND** it SHALL persist those inputs and outputs as coverage evidence

#### Scenario: Bootstrap fixes one evidence-visibility cutoff
- **WHEN** a latest-only bootstrap run starts
- **THEN** it SHALL persist one project-timezone `as_of` and inclusive evidence-visibility cutoff in the run scope and query fingerprint
- **AND** every market scan, targeted repair, retry, resume, candidate classification, observation selection, and winner decision SHALL reuse that same cutoff and fingerprint
- **AND** bootstrap SHALL admit a candidate only when its normalized publication evidence and selected observation `version_available_at` are both at or before the cutoff
- **AND** an announcement, attachment observation, silent byte update, or withdrawal first available after the cutoff SHALL NOT change that bootstrap run's winner or coverage credit
- **AND** post-cutoff evidence SHALL remain registered for the independent daily workflow without moving or silently regenerating the bootstrap cutoff
- **AND** resumed work SHALL reject a checkpoint whose cutoff or query fingerprint differs instead of mixing evidence populations

#### Scenario: Targeted bootstrap search walks fiscal years newest-first
- **WHEN** a missing-instrument repair evaluates the bounded fiscal-year range derived for that instrument
- **THEN** it SHALL complete each candidate fiscal-year scope in descending year order, starting at the candidate upper year and moving toward the earliest searchable year
- **AND** a completely scanned not-yet-due year with no published full report SHALL allow the search to continue to the next older candidate year so the newest actually published report can be selected
- **AND** an incomplete, retryable, blocked, or ambiguous newer-year scope SHALL stop that instrument's search without selecting or crediting an older year as latest
- **AND** after a valid latest-effective winner is committed, the repair SHALL stop before downloading or creating canonical attachments for older fiscal years
- **AND** only a complete empty search through the earliest bound with listing/source evidence SHALL permit `confirmed_missing`

#### Scenario: Newer fiscal year is not yet due
- **WHEN** a completely scanned newer fiscal year contains no published full report before its configured disclosure-due boundary
- **THEN** the newest actually published older report MAY remain the latest available asset
- **AND** the not-yet-due year SHALL NOT be misreported as a provider failure or permanent missing report

#### Scenario: Expected report is overdue
- **WHEN** the configured disclosure-due boundary has passed and complete source coverage finds no expected report
- **THEN** `bootstrap_asset_status` MAY remain `available` for the latest actually published older report while expected-period coverage SHALL be `overdue_missing`
- **AND** the overdue fiscal year SHALL remain a visible repair-eligible gap with delay or missing evidence
- **AND** version 1 default readiness SHALL be degraded without blocking daily discovery, while a versioned deployment policy MAY make the gap a stricter enablement blocker

#### Scenario: Older valid files already exist before bootstrap
- **WHEN** migration inventory finds valid annual reports from earlier fiscal years that are not the latest-only bootstrap winner
- **THEN** bootstrap SHALL NOT delete them merely because they are outside its acquisition target
- **AND** any later cleanup SHALL require an independently specified per-file retention or duplicate decision

#### Scenario: Latest fiscal year has corrections
- **WHEN** the latest available fiscal year has an original and one or more complete corrections
- **THEN** the bootstrap SHALL retain only the newest valid correction as the effective attachment

#### Scenario: Instrument has no published annual report
- **WHEN** a newly listed or otherwise uncovered active instrument has no valid full annual report
- **THEN** the bootstrap SHALL record `confirmed_missing` only after every required coverage scope, after applying the versioned provider capability and route-equivalence policy, and every required fiscal-year scope completes successfully and listing/source bounds prove the search empty
- **AND** an audited query-equivalent complete fallback result MAY satisfy the independent route-level required-coverage projection for the exact source/exchange/category/query scope, but it does not advance or erase the failed primary source's cursor or diagnostic gap
- **AND** a non-equivalent fallback SHALL leave the primary gap incomplete or retryable and SHALL NOT produce `confirmed_missing`, bootstrap success, or a full-market completion claim
- **AND** it SHALL remain eligible for bounded later repair and daily discovery
- **AND** the instrument SHALL have no winner or blob
- **AND** the instrument SHALL use `bootstrap_asset_status=confirmed_missing`
- **AND** the instrument SHALL NOT be reported as front-facing `asset_availability=local_valid`

#### Scenario: Confirmed-missing evidence is persisted and restored
- **WHEN** bootstrap writes or reloads a `confirmed_missing` terminal record
- **THEN** the record SHALL contain the complete required source/route scope set and, for each scope, exchange, normalized category, query bounds, successful-empty completion watermark, and page/subscope completion evidence
- **AND** the record SHALL contain instrument listing evidence, bootstrap `as_of`, evidence-visibility cutoff, confirmation time, expiry time, and the route-capability, query-policy, classifier, and eligibility fingerprints used for the decision
- **AND** the record SHALL retain stable references to the underlying source responses, coverage checkpoints, and route-equivalence evidence needed to audit the empty result
- **AND** missing any required scope, boundary, completion watermark, listing fact, time, fingerprint, or evidence reference SHALL prevent `confirmed_missing` from being committed or restored as terminal coverage
- **AND** restart recovery SHALL reload the complete evidence without silently replacing it with current configuration or a newly computed cutoff
- **AND** expiry or any relevant fingerprint change SHALL move the instrument back to bounded repair before it can receive terminal missing credit again

#### Scenario: A newer fiscal-year search is incomplete
- **WHEN** any provider, page, mapping, or candidate download needed to prove the newest fiscal year remains incomplete
- **THEN** bootstrap SHALL NOT select an older fiscal year and report it as the latest
- **AND** the instrument SHALL remain incomplete, retryable, or blocked

#### Scenario: Confirmed missing evidence expires
- **WHEN** `confirmed_missing` evidence reaches its configured expiry or its source/query fingerprint changes
- **THEN** the instrument SHALL reenter bounded coverage repair

#### Scenario: Latest correction cannot be verified during bootstrap
- **WHEN** an original is available but the legally newer complete correction cannot be downloaded or validated
- **THEN** bootstrap SHALL NOT claim final latest-effective coverage
- **AND** it SHALL expose the original only as provisional where policy permits
- **AND** the provisional bytes SHALL NOT receive `available|current` latest-only coverage credit; the instrument coverage SHALL remain `retryable|blocked` until the correction is verified or the newer candidacy is resolved

#### Scenario: Bootstrap is interrupted
- **WHEN** the process stops after completing part of the universe
- **THEN** a subsequent run SHALL resume from durable scope and acquisition state without redownloading verified assets or restarting completed instruments

#### Scenario: Bootstrap reaches an overall terminal result
- **WHEN** every target instrument is evaluated
- **THEN** the run SHALL report `success` only when every target has complete discovery evidence and `bootstrap_asset_status=available` or has unexpired `bootstrap_asset_status=confirmed_missing` evidence
- **AND** a completely proven `overdue_missing` expected period MAY coexist with batch success and an older available asset, but SHALL degrade readiness and remain in repair under the default version 1 policy
- **AND** any incomplete, retryable, or blocked target SHALL make the run `partial` or `blocked`, never falsely complete
- **AND** the persisted per-instrument report SHALL expose latest winner fiscal year, `bootstrap_asset_status=available|confirmed_missing|retryable|blocked`, expected-period coverage, and terminal/retry evidence as separate fields rather than derive them from the batch outcome
- **AND** `bootstrap_asset_status` SHALL remain a distinct vocabulary from the front-facing byte state `asset_availability=local_valid|metadata_only|missing|ambiguous|corrupt|superseded|blocked`

### Requirement: Daily Discovery Is Windowed Efficient And Fail-Closed
The daily annual-report update SHALL use category-filtered market discovery, durable provider cursors, overlap windows, bounded date partitions, and targeted missing-instrument repair.

The default overlap SHALL be three calendar days.
The configured overlap value SHALL be validated against bounded provider-delay evidence before scheduled enablement.
The configured overlap value SHALL be retained with the query-policy/configuration fingerprint.

#### Scenario: Normal daily run resumes
- **WHEN** a committed annual-report cursor exists for a source, exchange, and normalized category scope
- **THEN** the run SHALL begin from the committed range-coverage watermark `covered_until` minus the configured calendar-day overlap and end at one fixed run cutoff
- **AND** provider item position SHALL be persisted separately from completed time-range coverage
- **AND** an item or page cursor SHALL resume only the same incomplete query fingerprint, fixed cutoff, and parent window; a new overlap window SHALL restart from its date range unless the provider contract proves cursor reuse cannot truncate that overlap

#### Scenario: Provider accepts date-only window parameters
- **WHEN** an internal timezone-aware discovery window is routed to a provider whose contract accepts calendar dates rather than datetimes
- **THEN** the provider adapter SHALL convert each boundary to the declared project-timezone `YYYY-MM-DD` representation and apply explicit inclusive/exclusive result filtering
- **AND** it SHALL NOT pass an ISO datetime into a date-only parameter or reduce the precision of the internal `covered_until` watermark

#### Scenario: A complete daily window is empty
- **WHEN** every page and required source scope completes successfully through the fixed run cutoff but yields no in-range annual-report record
- **THEN** the system SHALL atomically advance `covered_until` to that cutoff
- **AND** a later run SHALL start from the advanced watermark minus overlap rather than repeatedly scanning from the last announcement timestamp

#### Scenario: A stale discovery worker commits after lease takeover
- **WHEN** worker A loses its scope lease, worker B acquires a newer generation and advances the cursor, gap state, parent completion, or `covered_until`, and worker A later attempts to commit
- **THEN** every discovery checkpoint and range-coverage mutation SHALL compare owner, lease generation, expected state version, query fingerprint, parent window, and fixed cutoff
- **AND** worker A MAY idempotently retain newly discovered metadata but SHALL NOT move `covered_until` backward, overwrite worker B's item cursor or gap, or change the newer parent's completion state
- **AND** the rejected stale commit and current monotonic state SHALL remain auditable and restart-safe

#### Scenario: Bootstrap hands off to daily maintenance
- **WHEN** bootstrap has completed an equivalent source/exchange/category scope through its fixed cutoff and daily readiness gates pass
- **THEN** daily mode SHALL adopt the compatible per-scope coverage watermark and begin from it minus overlap
- **AND** any scope without compatible complete bootstrap coverage SHALL use only the configured bounded initial filing-season window
- **AND** compatibility SHALL be proven by a canonical query-policy fingerprint shared by bootstrap and daily or by an explicit auditable handoff mapping; an operator-written daily fingerprint SHALL NOT substitute for that proof

#### Scenario: Provider supports only selected exchange routes
- **WHEN** bootstrap, daily, or on-demand discovery builds source/exchange/category scopes
- **THEN** it SHALL use one versioned provider capability and route matrix shared across those workflows
- **AND** it SHALL NOT query an unsupported source/exchange Cartesian-product combination or count that unsupported combination as a coverage gap

#### Scenario: Daily discovery finds a new complete original report
- **WHEN** the version 1 daily window discovers an eligible complete annual-report original for an active A-share instrument
- **THEN** the daily workflow SHALL register its metadata and proactively ensure, validate, and archive the effective attachment

#### Scenario: A new fiscal year is published after bootstrap
- **WHEN** daily discovery finds a complete annual report for a fiscal year newer than the instrument's existing managed periods
- **THEN** it SHALL create and acquire the new period's effective asset under the same one-winner-per-period policy
- **AND** it SHALL retain existing effective assets for older fiscal years and SHALL not treat the new period as a correction or delete an older period's file

#### Scenario: Daily discovery finds an older-fiscal-year correction
- **WHEN** the daily or reconciliation window discovers a complete correction for an older fiscal year
- **THEN** it SHALL reevaluate and replace only that instrument and fiscal year
- **AND** it SHALL NOT change the effective asset for a later fiscal year

#### Scenario: No cursor exists after bootstrap
- **WHEN** daily mode starts without a valid committed cursor
- **THEN** it SHALL derive a bounded current filing-season window and SHALL NOT silently scan unbounded market history

#### Scenario: Provider page bound is exceeded
- **WHEN** a market-wide date window cannot be completed within configured page or request limits
- **THEN** selected records from completed pages SHALL be durably registered
- **AND** the window SHALL be split into smaller date partitions and resumed
- **AND** the cursor SHALL NOT advance past the incomplete interval

#### Scenario: One publication day remains too dense
- **WHEN** a single-day window still exceeds the provider bound
- **THEN** the system SHALL continue through durable page ranges or stable provider-supported subscopes under the fixed run cutoff
- **AND** the parent day SHALL complete only after every child scope completes
- **AND** if no stable completion path exists, the day SHALL remain pending with an explicit blocker

#### Scenario: Cursor query semantics change
- **WHEN** source routing, exchange scope, category, time-boundary semantics, or classification policy changes incompatibly
- **THEN** the persisted cursor SHALL NOT be reused without explicit migration or bounded rediscovery

#### Scenario: Publication timestamps meet the run boundary
- **WHEN** records share timestamps across pages, carry future timestamps, or arrive while the run is executing
- **THEN** the run SHALL preserve raw timestamps, normalize them to the configured project timezone, and apply explicit inclusive/exclusive boundaries against one fixed cutoff
- **AND** it SHALL not advance past records that cannot be proven inside the completed window

#### Scenario: A correction is indexed late
- **WHEN** a correction whose publication time falls outside the normal overlap appears later at the provider
- **THEN** a bounded rotating long-lookback reconciliation of already-covered instruments SHALL discover it within the configured maximum reconciliation period

#### Scenario: An old managed fiscal year receives a very late correction
- **WHEN** a correction for a managed `instrument + fiscal_year` is first indexed outside the configured publication lookback
- **THEN** an oldest-first period-level reconciliation queue SHALL still revisit that managed period within the configured maximum cycle
- **AND** `last_reconciled_at`, retry state, and checkpoint progress SHALL prevent failed items or newer periods from starving older managed periods

#### Scenario: A managed attachment may change bytes without a provider version signal
- **WHEN** the provider capability matrix declares no trustworthy ETag, Last-Modified, official version, or content hash for an already managed attachment identity
- **THEN** a versioned bounded reconciliation cadence SHALL use conditional retrieval or temporary content-hash verification under the configured request, byte, reservation, rate, mount, and elapsed-time gates
- **AND** changed bytes SHALL create a new immutable observation with its own `version_available_at`
- **AND** the new changed-byte observation SHALL trigger period-scoped reselection
- **AND** unchanged bytes SHALL NOT create a new canonical blob
- **AND** unchanged bytes SHALL NOT create an effective-change event
- **AND** a route that cannot support bounded verification SHALL expose an explicit source-capability/readiness limitation and SHALL NOT claim silent-update completeness for that scope

#### Scenario: A fallback source returns data after a primary source fails
- **WHEN** a configured fallback source completes but a required primary source scope is incomplete
- **THEN** each source SHALL retain its own item cursor, `covered_until`, and gap state
- **AND** route-level coverage SHALL remain incomplete unless the versioned route policy explicitly declares the fallback an equivalent substitute for that scope

#### Scenario: Active universe changes
- **WHEN** a stock lists, delists, or changes active state after bootstrap
- **THEN** the daily workflow SHALL refresh or revalidate the paired master/census snapshot before each run according to a versioned `universe_refresh_cadence`
- **AND** the daily workflow SHALL persist attempted/effective refresh time plus the effective paired snapshot id
- **AND** an auditable successful refresh SHALL add new listings to coverage repair and remove delistings from the active denominator
- **AND** a missed or failed refresh SHALL use only a still-fresh last acceptable pair
- **AND** an expired pair SHALL prevent a full-market completion claim
- **AND** market discovery MAY continue under the documented degraded policy when the pair is expired
- **AND** delisting SHALL NOT delete retained assets

#### Scenario: Market scan misses an expected current report
- **WHEN** an active instrument is expected to have a latest annual report but coverage is absent
- **THEN** a bounded rotating repair cohort SHALL run instrument-scoped annual-category discovery without forcing one query per instrument in every daily run

#### Scenario: Missing-repair cohorts are prioritized and fair
- **WHEN** the daily job builds its bounded repair cohort
- **THEN** it SHALL prioritize selected metadata whose attachment is not ready, current-period expected gaps, due retryable items, and finally expired confirmed-missing evidence according to configured policy
- **AND** it SHALL persist cohort size, fairness/checkpoint state, retry timing, and skipped scopes so a failed item or a high-volume cohort cannot starve older managed periods or newly listed instruments

#### Scenario: The configured overlap lacks provider-delay evidence
- **WHEN** no bounded live or recorded provider-delay evidence supports the configured overlap
- **THEN** scheduled daily enablement SHALL remain degraded or blocked according to policy
- **AND** readiness SHALL expose the missing calibration evidence
- **AND** operators MAY run bounded manual discovery or repair while metadata-only reads remain available

#### Scenario: A discovery page fails
- **WHEN** metadata discovery fails before every page or child scope in the requested window completes
- **THEN** completed metadata SHALL remain reusable
- **AND** the prior committed discovery cursor SHALL be retained
- **AND** the incomplete discovery work SHALL enter bounded retry state

#### Scenario: Attachment acquisition fails after discovery completes
- **WHEN** every metadata page in a discovery window completes but one or more selected attachments fail acquisition
- **THEN** the discovery cursor MAY advance for the completed metadata window
- **AND** attachment failures SHALL remain in a separate bounded retry queue without losing their metadata

#### Scenario: Overlap observes an unchanged attachment again
- **WHEN** an overlap or reconciliation scan observes the same attachment observation identity without new bytes or source state
- **THEN** the system SHALL update observation evidence without resetting completed, not-yet-due, exhausted, blocked, or operator-held acquisition state to queued
- **AND** only a new observation/version, a due `next_retry_at`, or an audited operator repair SHALL reopen acquisition work

#### Scenario: Withdrawal metadata has no downloadable report
- **WHEN** a new withdrawal or cancellation observation can be bound to a managed annual-report candidate but contains no eligible complete PDF
- **THEN** the system SHALL still transactionally reevaluate that instrument and fiscal year
- **AND** it SHALL NOT require an attachment acquisition item before applying the governed withdrawal decision

#### Scenario: Original and later correction are discovered together
- **WHEN** one discovery batch contains an original and a legally later complete correction for the same instrument and fiscal year
- **THEN** only the verified correction SHALL become the final effective winner
- **AND** if the correction cannot be acquired or verified, bootstrap SHALL remain blocked or retryable for that period
- **AND** an already-serving legal original MAY remain explicitly provisional
- **AND** the provisional legal original SHALL NOT be represented as an unqualified final winner
- **AND** daily SHALL report batch outcome `partial|blocked` for that scope
- **AND** the correction attachment SHALL remain in durable `retryable|blocked` state
- **AND** daily SHALL NOT include the provisional/original asset id in the completed affected-asset set passed to consumers

### Requirement: Attachment Acquisition Is Atomic Idempotent And Concurrency-Safe
The asset service SHALL ensure that concurrent schedulers, API requests, and business consumers cannot download or publish duplicate physical assets for the same attachment observation.

#### Scenario: Two consumers request the same missing asset
- **WHEN** two callers concurrently ensure the same source-qualified attachment
- **THEN** one caller SHALL hold the acquisition lease and perform the download
- **AND** the other caller SHALL wait for or reuse the committed result
- **AND** only one physical blob SHALL be published

#### Scenario: Process stops during download
- **WHEN** a process terminates while writing a temporary attachment
- **THEN** no partial file SHALL be visible as a valid asset
- **AND** a later retry SHALL verify owner, heartbeat, lease generation, and safety-grace evidence before cleaning or adopting the temporary file

#### Scenario: Temporary and quarantine bytes accumulate
- **WHEN** stale `.part` files or quarantined evidence exceed configured age or byte thresholds
- **THEN** readiness SHALL expose their actual bytes independently from released reservations
- **AND** stale `.part` cleanup SHALL be lease-generation safe
- **AND** quarantine cleanup SHALL require an operator-authorized audited command
- **AND** quarantine cleanup SHALL preserve evidence metadata

#### Scenario: Existing file has matching evidence
- **WHEN** an existing path has the registered byte length, valid PDF signature, and SHA-256
- **THEN** it SHALL be adopted or reused without network acquisition

#### Scenario: Archive mount identity is unsafe
- **WHEN** the filings NFS is missing, read-only, remounted to an unapproved source, or has become a local fallback directory at operation start or immediately before publish, link, move, or unlink
- **THEN** attachment writes and destructive file operations SHALL be blocked
- **AND** every filesystem mutation SHALL revalidate the approved mount source/read-write identity at its execution boundary so a remount race cannot reuse stale preflight evidence

#### Scenario: Concurrent operations reserve storage
- **WHEN** several attachment acquisitions pass individual space preflight concurrently
- **THEN** atomic filesystem-scoped byte reservations SHALL prevent their combined temporary and final bytes from violating the hard reserve

#### Scenario: Existing file is corrupt
- **WHEN** an existing registered file is missing, unreadable, not a PDF, size-mismatched, or hash-mismatched
- **THEN** it SHALL not satisfy local-first access
- **AND** the service SHALL quarantine or mark it corrupt before bounded reacquisition

### Requirement: Retry Classification Is Explicit And Bounded
Attachment, discovery, backup, and repair failures SHALL be classified into retryable, operator-action, or blocked outcomes with durable attempt and scheduling evidence.

#### Scenario: A transient provider or mount failure occurs
- **WHEN** a request fails because of a timeout, rate limit, temporary 5xx response, temporarily unavailable attachment, or transient archive/backup mount error
- **THEN** the operation SHALL remain retryable with bounded exponential/backoff scheduling, `attempt`, `max_attempts`, `next_retry_at`, and a redacted reason code
- **AND** the failure SHALL NOT be counted as complete coverage or publish an incomplete asset

#### Scenario: A deterministic integrity or identity failure occurs
- **WHEN** a candidate has an identity conflict, unsafe path, non-PDF body, persistent length/hash mismatch, unresolved winner ambiguity, or an unsplittable dense window
- **THEN** the retry item and parent operation SHALL enter `blocked` with an explicit `operator_action_required` diagnostic
- **AND** blocked retry work SHALL remain reopenable only through corrected evidence, a new observation, or an audited operator action
- **AND** it SHALL NOT perform high-frequency automatic retries or advance the affected coverage watermark
- **AND** operation status `failed` SHALL be reserved for a terminal execution failure that is neither a waiting resource/policy condition nor one of these operator-remediable deterministic data/identity blockers; it SHALL NOT be used as an interchangeable projection of ambiguity, integrity conflict, or retry exhaustion

#### Scenario: Storage reserve is insufficient
- **WHEN** planned or streamed bytes would cross the configured archive or backup hard reserve
- **THEN** attachment/backup work SHALL enter `blocked` pending capacity change or audited operator override
- **AND** metadata discovery and verified local reads SHALL remain available
- **AND** the blocked item SHALL not consume ordinary network retry budget

#### Scenario: A retry reaches its bound
- **WHEN** a retryable item reaches `max_attempts` or its retry deadline
- **THEN** the attachment or discovery retry item SHALL become `exhausted` with durable diagnostics and readiness impact
- **AND** its parent durable operation and API projection SHALL use status `blocked` with reason `retry_exhausted`
- **AND** `exhausted` SHALL NOT be added as a separate operation-status value
- **AND** only a new observation/version, an explicitly due repair, or an audited operator repair SHALL reopen it

### Requirement: Asset Operations Are Durable Idempotent And Recoverable
Backfill, daily, ensure, migration, integrity, deletion, and backup work SHALL use durable operation state and leases rather than process-local background-task state.

#### Scenario: The same ensure scope is requested concurrently
- **WHEN** scheduler, API, or consumers request the same normalized instrument/fiscal-year or exact-filing scope under the same versioned acquisition work fingerprint
- **THEN** the service SHALL create at most one active internal asset operation for the acquisition scope
- **AND** that fingerprint SHALL cover operation type, normalized selector/scope, acquisition and retention policy, provider route/capability matrix, classifier and integrity policy, relevant configuration fingerprint, and accepted work/network/storage bounds
- **AND** semantically equivalent normalized inputs SHALL share work, while any change that alters source routing, selection, integrity, or execution bounds SHALL NOT silently reuse an incompatible active operation
- **AND** every external caller SHALL receive the completed local asset or its own authorized subscription/opaque query handle to that shared operation
- **AND** sharing work SHALL NOT transfer another principal's idempotency key, consumer continuation, ownership, or privileged diagnostics

#### Scenario: A worker stops during an operation
- **WHEN** a process exits after persisting progress or holding a lease
- **THEN** operation status, stage, checkpoint, attempts, heartbeat, retry time, and bounded diagnostics SHALL remain queryable
- **AND** lease expiry SHALL permit safe resume without repeating verified work

#### Scenario: Operation state is exposed
- **WHEN** an operation is queried
- **THEN** internal durable operation status SHALL be `queued|running|completed|missing|failed|blocked|cancelled`, separate from its versioned `discovering|reconciling|adopting|downloading|validating|activating|deleting|backing_up|restoring|auditing` stage
- **AND** batch `success|partial|blocked|failed` outcomes, `local_hit|local_miss|operation_created|operation_reused` ensure disposition, and asset availability SHALL remain separate concepts
- **AND** `expired` SHALL be reserved for caller-owned asset/consumer request projections and idempotency tombstones, never used to fabricate or rewrite internal operation state

#### Scenario: An operation has no applicable active stage
- **WHEN** an operation is terminal before entering a stage, or its operation type does not use a particular stage
- **THEN** the persisted and API stage field SHALL use the versioned `null`/`not_applicable` representation
- **AND** `completed` SHALL remain an operation status only and SHALL never be fabricated as an operation stage
- **AND** schema round-trip tests SHALL preserve the distinction for every operation type

#### Scenario: An operator requests cancellation of internal work
- **WHEN** an operator requests cooperative stop for an internal batch operation or already-started consumer processing that is in a non-cancellable stage or whose domain stop contract is disabled
- **THEN** the operator/domain command SHALL reject the stop explicitly while lease expiry and bounded recovery remain defined
- **AND** this rule SHALL NOT apply to caller-owned `asset_request_id` DELETE, which always performs version 1 logical subscription detach according to the request contract

#### Scenario: One subscriber cancels shared acquisition
- **WHEN** one principal cancels its asset-request subscription while another subscriber or scheduler still requires the same internal asset operation
- **THEN** only the cancelling principal's subscription SHALL become cancelled; deleting an `asset_request_id` SHALL NOT mutate the linked `consumer_request_id` or its continuation
- **AND** the shared acquisition SHALL continue or checkpoint according to its remaining subscribers and scheduler policy; one caller SHALL NOT cancel work required by another caller

#### Scenario: The last asset subscriber cancels
- **WHEN** the last external request subscription is cancelled after a bounded shared acquisition operation has been created
- **THEN** version 1 SHALL detach only that subscription while leaving any linked consumer continuation queryable and governed by the consumer-request cancellation contract
- **AND** the internal acquisition SHALL continue to a bounded terminal state so its canonical result remains reusable
- **AND** cancellation of the asset request SHALL NOT stop consumer processing that has already started; any such stop SHALL use the consumer domain's own authorized contract or be explicitly rejected

#### Scenario: Shared rollout is rolled back before physical cleanup
- **WHEN** an operator disables shared consumer routing or daily writes before legacy files are removed
- **THEN** additive canonical metadata, replacement lineage, operation history, and audit records SHALL remain intact
- **AND** rollback SHALL use feature gates rather than deleting shared database state

#### Scenario: Shared rollout is rolled back after physical cleanup
- **WHEN** an operator must restore a legacy consumer after predecessor or duplicate files have been physically removed
- **THEN** consumer rollback SHALL first validate a mutually compatible application version, catalog snapshot, and attachment-backup/file-manifest watermark in an isolated temporary root without overwriting the live catalog
- **AND** it SHALL reconstruct every required legacy path in a temporary root from the corresponding immutable hash-verified recovery-manifest entry (`legacy_path_rollback` for a legacy alias and the correction-predecessor kind for superseded source bytes) and prove the legacy consumer can read the reconstructed files before publishing those paths or starting that consumer
- **AND** if live catalog data was actually lost, paired restore SHALL freeze writes
- **AND** paired restore SHALL declare the snapshot RPO
- **AND** paired restore SHALL replay post-snapshot outbox/operation increments before reopening
- **AND** code rollback alone SHALL be rejected
- **AND** no consumer rollback stage SHALL delete canonical metadata, replacement lineage, recovery entries, operation history, or audit records

### Requirement: Effective-Asset Changes Are Durable And Replayable
The system SHALL append a durable monotonic change event or watermark whenever an effective annual-report asset is added, replaced, repaired, withdrawn, or physically removed.
For an effective-decision change, the event/outbox record SHALL be committed atomically with that decision.
The event/outbox record SHALL be delivered asynchronously with idempotent consumer checkpoints.
The system SHALL persist every change event durably.
Parser dispatch SHALL occur only under an explicit consumer continuation or versioned scheduler dependency policy.

#### Scenario: Asset event is recorded without implicit consumer work
- **WHEN** a generic ensure or an asset scheduler commits an `added|repaired|replaced|withdrawn|deleted` change
- **THEN** the immutable event SHALL record trigger origin, policy version, affected asset/period, and dispatch-policy version
- **AND** generic ensure SHALL create no consumer operation
- **AND** `added|repaired` events SHALL enqueue parser work only for an explicit consumer continuation or declared scheduler dependency
- **AND** `replaced|withdrawn|deleted` events SHALL invalidate only lineage whose normalized selector/cutoff evidence scope is changed by the event
- **AND** `replaced|withdrawn|deleted` events SHALL enqueue reprocessing only under the consumer's declared policy
- **AND** default-effective results SHALL become stale when their effective asset changes, while exact-observation or knowledge-cutoff results SHALL remain current when the event lies outside their evidence scope

#### Scenario: A consumer is offline during a correction
- **WHEN** a correction becomes effective while a registered consumer is not running
- **THEN** the consumer SHALL resume from its own checkpoint and receive the affected instrument, fiscal year, predecessor asset, and replacement asset
- **AND** idempotent replay SHALL NOT require announcement rediscovery or attachment redownload

#### Scenario: Delivery stops after replacement commit
- **WHEN** a process stops after a correction and its outbox record commit but before delivery or before a consumer checkpoint advances
- **THEN** restart SHALL redeliver the immutable event until each consumer records its idempotent checkpoint
- **AND** the replacement SHALL remain effective without permanently losing stale-result invalidation or reprocessing work

### Requirement: Existing Annual-Report Files Are Reconciled And Reused
Migration SHALL inventory existing annual-report manifests and paths, validate their identities and content, and adopt valid files before enabling new downloads.

#### Scenario: Initial migration inventory runs
- **WHEN** an operator inventories existing business-profile and broker archives
- **THEN** the default operation SHALL be read-only and SHALL NOT download, move, link, quarantine, or delete files
- **AND** it SHALL report adoptable, duplicate, missing, corrupt, conflicting, orphan, derived, and out-of-scope entries

#### Scenario: An orphan has recoverable official identity
- **WHEN** an existing valid PDF lacks a trusted manifest but filename/hash evidence can be resolved uniquely through metadata-only official discovery or an audited operator mapping
- **THEN** migration MAY reconcile source announcement, attachment, instrument, report period, classification, length, and hash without downloading attachment bytes and register the result in shadow state
- **AND** ambiguous, conflicting, or partially resolved orphans SHALL remain fail-closed and SHALL NOT satisfy effective lookup or coverage

#### Scenario: A valid older-fiscal-year report already exists
- **WHEN** migration finds a verifiable complete annual report for an older fiscal year that is outside the latest-only bootstrap target
- **THEN** it SHALL register the filing and bytes as a migration-adopted asset so an explicit local-first request for that fiscal year can reuse it with zero network access
- **AND** adoption SHALL NOT add that older period to latest-only coverage or trigger network backfill of adjacent historical years
- **AND** any original/correction competition within that adopted fiscal year SHALL use the same effective-version and deletion gates as newly acquired assets

#### Scenario: Business-profile archive contains a valid latest report
- **WHEN** an existing business-profile manifest and file identify the selected latest effective annual report
- **THEN** migration SHALL register that file as the shared asset without redownloading it

#### Scenario: Broker archive contains the only valid copy
- **WHEN** a broker annual-report manifest contains the selected latest effective annual report and no business-profile copy exists
- **THEN** migration SHALL register and reuse the broker file as the shared asset

#### Scenario: Broker archive contains a complete corrected annual report
- **WHEN** a broker file has an annual period end and passes complete annual-report classification as either an original or complete correction
- **THEN** migration SHALL apply normal effective-version selection
- **AND** migration SHALL allow that verified correction to be adopted
- **AND** it SHALL continue to exclude semiannual reports and correction notices without a complete report body

#### Scenario: Shadow adoption has not reconciled
- **WHEN** an adopted record has not completed source, instrument, report-period, classification, content, and latest-effective reconciliation
- **THEN** it SHALL NOT satisfy production effective lookup, bootstrap coverage, or consumer parsing
- **AND** conflict-free reconciliation plus an explicit asset-adoption promotion gate SHALL be required before production visibility

#### Scenario: Adopted asset is promoted before consumer cutover
- **WHEN** a shadow record passes the asset-adoption promotion gate while business-profile or broker migration remains disabled
- **THEN** the shared asset layer SHALL allow the promoted record to satisfy effective lookup, bootstrap reuse, daily maintenance, and local-first ensure
- **AND** each consumer SHALL remain governed by its own separate cutover gate

#### Scenario: A promoted adopted path remains under a legacy directory
- **WHEN** asset-adoption promotion would make a legacy-path file production-visible before its legacy consumer cuts over
- **THEN** the exact hash-qualified file SHALL be under shared-module custody and legacy writers or cleaners SHALL be unable to mutate or remove it, or migration SHALL first converge it to a controlled canonical path
- **AND** production reads, backup, and integrity audit SHALL verify registered length/hash
- **AND** detected external mutation or deletion SHALL immediately invalidate local availability
- **AND** the service SHALL prevent content streaming after detected external mutation or deletion until verified repair

#### Scenario: Duplicate valid copies exist
- **WHEN** business-profile and broker archives contain the same source filing and content hash at different paths
- **THEN** migration SHALL select one canonical physical file or create one verified canonical link
- **AND** it SHALL switch consumers before deleting the redundant copy

#### Scenario: Existing manifests disagree
- **WHEN** source id, instrument, report period, content hash, or file contents conflict
- **THEN** migration SHALL report the conflict
- **AND** migration SHALL NOT delete or silently merge either file

#### Scenario: Legacy directories contain unrelated files
- **WHEN** an archive contains semiannual reports, fiscal-year files not selected by the current operation's explicit adoption/cleanup allowlist, derived artifacts, orphans, or conflicting files beside adoptable annual reports
- **THEN** migration cleanup SHALL use an explicit per-file manifest/hash allowlist and default dry-run
- **AND** it SHALL NOT delete any excluded file or perform directory-level cleanup

#### Scenario: Migration executes beside excluded files
- **WHEN** an approved adoption, convergence, or cleanup operation executes in a mixed legacy archive
- **THEN** every excluded semiannual, non-allowlisted fiscal-year, derived, orphan, and conflicting file SHALL retain its path, bytes, content hash, modification time, and permissions
- **AND** the operation SHALL NOT touch, chmod, move, link, quarantine, or rewrite an excluded file in either dry-run or execution mode

#### Scenario: Legacy paths are approved for physical cleanup
- **WHEN** a verified duplicate business-profile or broker path is approved for deletion after consumer cutover
- **THEN** migration SHALL first persist a versioned `manifest_kind=legacy_path_rollback` entry inside the common recovery manifest mapping the legacy path and consumer identity to the shared asset and content hash
- **AND** a temporary-root drill SHALL prove required aliases or copies can be reconstructed from verified canonical or backup blobs before cleanup proceeds

### Requirement: Archive Layout And Storage Gates Are Governed
The shared archive SHALL reside below the remounted `data/filings` volume, use safe project-relative configuration, include immutable content identity in filenames or blob paths, and enforce disk and attachment-size gates.

#### Scenario: New annual-report file is stored
- **WHEN** a verified attachment is committed
- **THEN** its canonical path SHALL be derived from a content-addressed SHA-256 blob pool beneath the configured root
- **AND** it SHALL not be written beneath a business-owned archive root

#### Scenario: Two identities share one content hash
- **WHEN** two legal attachment identities resolve to identical verified content
- **THEN** the blob pool SHALL retain one physical SHA-256 object
- **AND** legal identities SHALL remain database projections rather than duplicate physical files

#### Scenario: Free-space warning threshold is crossed
- **WHEN** the configured warning utilization is reached
- **THEN** the job SHALL emit an operational warning with planned and available bytes

#### Scenario: Stop threshold is crossed
- **WHEN** available storage is below the configured hard reserve or utilization stop threshold
- **THEN** scheduled prefetch SHALL stop before downloading new attachments
- **AND** metadata synchronization SHALL remain allowed
- **AND** explicit on-demand requests SHALL return an actionable storage blocker unless an operator override is authorized

#### Scenario: Attachment exceeds configured limit
- **WHEN** Content-Length or streamed bytes exceed the annual-report attachment limit
- **THEN** acquisition SHALL stop immediately without publishing a partial file
- **AND** the temporary file SHALL be removed or moved into governed quarantine according to the configured evidence policy
- **AND** the byte reservation SHALL be reconciled against actual `.part` and quarantine bytes
- **AND** no attachment version or canonical blob SHALL become valid

### Requirement: Source Assets And Business Processing Remain Separate
The shared asset state SHALL NOT be overwritten by business parser outcomes.
Consumer processing SHALL be independently retryable and versioned.

#### Scenario: Broker parser fails
- **WHEN** broker risk-control parsing fails for a valid shared annual-report asset
- **THEN** the shared asset SHALL remain valid and reusable
- **AND** only the broker processing record SHALL be marked failed

#### Scenario: Business-profile parser upgrades
- **WHEN** business-profile changes its PDF, section, or semantic parser version
- **THEN** it SHALL reuse the same verified source asset and create new derived processing identity without downloading the annual report again

#### Scenario: Effective asset changes
- **WHEN** a correction supersedes the asset used by a consumer
- **THEN** affected default-effective consumer processing records SHALL be marked superseded or requeued according to consumer policy
- **AND** a result pinned to an exact observation or knowledge cutoff SHALL remain current for that selector when the later correction is outside its evidence scope
- **AND** old derived facts SHALL not remain silently current

### Requirement: Stable Internal And API Access Is Provided
The system SHALL expose shared annual-report asset access through DataManager/service contracts and additive FastAPI endpoints without exposing arbitrary filesystem access.

#### Scenario: Business service requests an asset
- **WHEN** an internal workflow requests the effective annual report
- **THEN** it SHALL receive a structured asset reference containing asset id, instrument, fiscal year, report period, source, filing id, publication time, correction status, content hash, length, integrity status, and controlled local handle or path abstraction

#### Scenario: Front-facing client checks availability
- **WHEN** a client queries annual-report status for an instrument
- **THEN** the API SHALL return effective-version metadata, local availability, integrity, correction status, acquisition status, and last checked time
- **AND** it SHALL not disclose an unrestricted server filesystem path

#### Scenario: Client requests missing asset acquisition
- **WHEN** an authorized client requests ensure/download for a missing report
- **THEN** the API SHALL create or reuse a bounded acquisition job and return the caller's stable opaque `asset_request_id` and current authorized status projection rather than holding an unbounded HTTP request open or exposing the internal operation id

#### Scenario: A read-only API is called
- **WHEN** a client invokes an annual-report metadata GET or an existing business-profile GET
- **THEN** the request SHALL perform zero provider calls and zero attachment writes

#### Scenario: Mutation authorization is not configured
- **WHEN** no trusted identity and scoped authorization boundary exists
- **THEN** ensure, content-streaming, cancellation, and operator endpoints SHALL remain disabled or fail closed

#### Scenario: API caller observes progress
- **WHEN** a client inspects an acquisition and its downstream business result
- **THEN** asset availability and ensure disposition SHALL be exposed through the caller's `asset_request_id` projection while downstream continuation/processing SHALL be exposed through a separate caller-owned `consumer_request_id`
- **AND** the two projections SHALL be independently queryable without exposing internal asset or consumer operation ids

#### Scenario: A predecessor remains usable while a newer correction is pending
- **WHEN** a legally newer complete correction is discovered but cannot yet be verified and policy continues serving the predecessor
- **THEN** the effective decision SHALL be exposed as provisional with the pending correction identity and stable reason
- **AND** clients SHALL NOT present the predecessor as an unqualified final latest-effective report
- **AND** `asset_availability=local_valid` SHALL describe only verified local bytes
- **AND** `asset_availability=local_valid` SHALL NOT authorize new default-effective consumer processing while the decision is `provisional|ambiguous`
- **AND** only an explicit exact-observation or knowledge-cutoff selector whose evidence scope remains valid MAY proceed under its consumer policy

#### Scenario: Client downloads an available report
- **WHEN** an authorized endpoint streams a local annual report
- **THEN** it SHALL resolve the asset by identifier, validate current availability, set a safe filename and media type, and prevent path traversal

### Requirement: Asset Operations Are Observable And Auditable
Backfill, daily, on-demand, migration, deletion, and integrity operations SHALL emit structured results and stage logs sufficient to prove coverage and diagnose failures.

#### Scenario: Daily update completes
- **WHEN** the daily task finishes
- **THEN** its persisted and reloadable result SHALL include target exchanges, discovery and reconciliation windows, pages and requests, records seen, formal reports selected, corrections selected, silent-update verification and source-limitation counts, excluded and ambiguous counts, local hits, adopted/reused/downloaded/failed counts, new effective and dereferenced assets, bytes reserved/written, superseded files, deletion states, retries, missing-repair cohort and skipped scopes, cursor decisions, per-stage timings, storage/backup gates, elapsed time, and errors by source

#### Scenario: Latest-only backfill completes
- **WHEN** the bootstrap reaches terminal coverage
- **THEN** its persisted and reloadable result SHALL report target active-instrument universe snapshot identity, instruments with effective assets, confirmed missing, retryable, incomplete, and blocked instruments with detail entry points, original and correction winner counts, existing files adopted, downloaded files, duplicate-content and conflict counts, validation failures, completed and incomplete windows, checkpoint/resume identity, total bytes, remaining free space, unprotected bytes, and checkpoint completeness

#### Scenario: Physical predecessor is deleted
- **WHEN** any superseded original or earlier-correction file is deleted
- **THEN** an append-only audit SHALL identify the old asset, replacement asset, hashes, paths, reason, retention-pin decision, actor or job, and timestamp

#### Scenario: Final annual-report uniqueness is audited
- **WHEN** bootstrap, restore, migration cleanup, or release acceptance evaluates the catalog and controlled archive
- **THEN** every managed `instrument + fiscal_year` with a production-available annual report SHALL have exactly one current effective winner, and a missing or blocked period SHALL have no falsely current winner
- **AND** no superseded, withdrawn, metadata-only, candidate-verification, alias, quarantine, or backup object SHALL be exposed or counted as a second consumer-visible canonical attachment
- **AND** a superseded primary blob MAY remain only while it is byte-identical and shared by another valid reference or while an explicit deletion-blocking pin or governed deletion intent is active; any pending or failed convergence SHALL block the final unique-storage completion gate until it is resolved or separately accepted as a documented blocker
- **AND** the audit SHALL reject orphan current blobs, contradictory effective rows, and a second same-period canonical attachment

#### Scenario: Operator inspects readiness
- **WHEN** readiness is queried
- **THEN** it SHALL report active-universe coverage, attachment readiness, integrity failures, pending discovery/reconciliation windows, retry queues, storage reservations/gates, backup freshness and unprotected bytes, bootstrap completion, scheduler enablement/last result, and consumer migration status
- **AND** it SHALL report `missing_attachment_count`, `estimated_required_bytes`, the estimate basis, estimate `as_of`, configuration fingerprint, and an explicit unavailable/indeterminate estimate state when required bytes cannot be bounded safely
- **AND** the estimate SHALL account separately for known content lengths, configured unknown-length reservations, temporary publication overhead, and replacement old-plus-new peak bytes without presenting reserved bytes as already consumed bytes
- **AND** asset-scheduler readiness SHALL be calculated independently from per-consumer migration readiness so an unready consumer cannot block shared backfill or daily maintenance

#### Scenario: Predecessor cleanup does not converge
- **WHEN** a distinct predecessor remains `planned`, `deleting`, or `failed` beyond the configured cleanup warning or hard age
- **THEN** asset readiness SHALL become at least `degraded` at the warning age
- **AND** asset readiness SHALL block the unique-storage completion claim at the hard age
- **AND** the verified replacement SHALL remain effective across both cleanup-age thresholds
- **AND** readiness SHALL expose the oldest unresolved predecessor age, state, last error, threshold, and operator-repair disposition without blocking verified local reads of unrelated assets
- **AND** both configured ages SHALL be positive with warning age strictly less than hard age
- **AND** both configured ages SHALL participate in the versioned configuration fingerprint and audit
- **AND** crossing either age SHALL change only readiness and repair routing and SHALL NOT release a retention pin, remove a deletion-intent predecessor from the required set, or authorize unlink

#### Scenario: Legacy cleanup readiness is evaluated
- **WHEN** an operator or rollout controller evaluates whether legacy annual-report writers may be stopped or duplicate paths may be removed
- **THEN** readiness SHALL expose independent `legacy_write_stop_allowed` and `duplicate_cleanup_allowed` states (or an equivalent versioned gate projection)
- **AND** those states SHALL remain blocked until consumer dual-read reconciliation, shared custody/adoption, required independent backup and paired catalog watermark, recovery-manifest validation, active retention-pin checks, and the relevant consumer cutover gates pass
- **AND** an asset-scheduler readiness result SHALL not be downgraded merely because a consumer has not yet opted into cutover; only the corresponding cleanup gate remains blocked

### Requirement: File Backup Protects The Shared Archive
Canonical attachment files SHALL have a governed incremental backup or replication workflow separate from SQLite online database backup.

#### Scenario: Archive backup runs
- **WHEN** new or changed canonical blobs exist
- **THEN** the backup workflow SHALL enumerate the catalog required-blob set, including adopted blobs still located at controlled legacy paths, every correction-predecessor, withdrawal-tombstone, or legacy-duplicate blob in the immutable version 1 recovery manifest, and every predecessor named by a `planned|deleting` deletion intent whose recovery manifest is not yet committed, and copy only missing content-addressed files to the configured backup mount
- **AND** it SHALL verify size and hash for copied content without requiring a legacy-path blob to move first

#### Scenario: A hash-named backup blob already exists
- **WHEN** the backup target already contains the expected content-addressed path
- **THEN** the workflow SHALL verify its byte length and SHA-256 before marking the source blob protected
- **AND** a mismatched existing target SHALL remain unprotected and SHALL NOT advance the paired backup watermark
- **AND** an ordinary backup run SHALL preserve the mismatched target's path, bytes, and modification time
- **AND** quarantine or replacement SHALL occur only inside an operator-authorized, auditable repair operation that preserves original path/hash evidence and uses temporary verified publication

#### Scenario: Backup stops during file publication or watermark commit
- **WHEN** the process stops while copying a temporary file, after atomic publication, or before committing the paired file-manifest watermark
- **THEN** no temporary or uncommitted file SHALL satisfy backup readiness
- **AND** a resumed operation SHALL reconcile target bytes idempotently, verify the final hash, and commit one paired watermark without recopying already verified content

#### Scenario: Recoverable catalog increments occur after a paired snapshot
- **WHEN** durable catalog, outbox, operation, lineage, or audit state changes after the latest paired catalog/file snapshot
- **THEN** an append-only recovery journal in the independent backup failure domain SHALL retain ordered increment identity, integrity hash, predecessor watermark, terminal coverage watermark, and source catalog generation needed to detect truncation and replay the accepted recovery scope
- **AND** a restore SHALL either verify and replay the complete journal interval in order or prove from a write-freeze watermark that no post-snapshot increment exists; an assertion based only on the older catalog snapshot SHALL NOT satisfy recovery readiness

#### Scenario: Backup mount is unavailable
- **WHEN** the configured NAS mount is missing or resolves to an unsafe local fallback
- **THEN** the backup SHALL fail and alert without writing a full archive copy to the local data volume

#### Scenario: Backup shares the primary failure domain
- **WHEN** backup and primary filings resolve to the same storage server or otherwise non-independent failure domain
- **THEN** that copy SHALL NOT satisfy the predecessor-deletion backup gate

#### Scenario: Backup independence evidence is ambiguous
- **WHEN** runtime mount source, server, export, or available filesystem identity conflicts with the configured failure-domain identity, or independence relies only on path/host aliases or labels
- **THEN** the backup SHALL be treated as non-independent and predecessor deletion SHALL remain blocked

#### Scenario: File and catalog recovery watermarks disagree
- **WHEN** a replacement blob backup and consistent file manifest watermark cannot be paired with a recoverable catalog database snapshot containing the replacement transaction
- **THEN** predecessor physical deletion SHALL remain blocked

#### Scenario: Asset has not been backed up
- **WHEN** a canonical asset is locally valid but has no verified backup copy
- **THEN** readiness SHALL expose the backup gap
- **AND** physical deletion of its predecessor SHALL remain blocked until both the replacement and that predecessor are verified in an independent failure domain and paired with the catalog recovery watermark

#### Scenario: Backup target reserve would be violated
- **WHEN** planned or streamed backup bytes would violate the backup target's configured free-space reserve or hard-stop threshold
- **THEN** backup SHALL fail or checkpoint
- **AND** backup SHALL NOT publish a partial target blob
- **AND** backup SHALL clean or reconcile temporary files
- **AND** the local asset SHALL remain valid while readiness exposes unprotected bytes and predecessor deletion remains blocked

#### Scenario: A superseded blob already exists in backup
- **WHEN** a local predecessor is superseded after its content-addressed blob was backed up
- **THEN** version 1 SHALL keep that backup blob as non-consumer-visible disaster-recovery content
- **AND** no automatic backup garbage collection SHALL run without a separately specified audited retention policy

#### Scenario: A predecessor has never been backed up
- **WHEN** a superseded predecessor is otherwise eligible for deletion but its hash is absent or invalid in the independent backup required set
- **THEN** primary-storage unlink SHALL remain blocked even when the replacement is fully protected
- **AND** backup SHALL protect the predecessor as non-consumer-visible recovery content before deletion proceeds

#### Scenario: Database and attachment assets are restored
- **WHEN** an operator restores the announcement-asset capability after data loss
- **THEN** a compatible catalog database snapshot and attachment backup watermark SHALL be restored together
- **AND** hash/integrity reconciliation SHALL complete before consumer reads, destructive cleanup, or daily writes are re-enabled
- **AND** every current-effective, retention-pinned, pending-deletion replacement and predecessor, and every immutable indefinitely-active version 1 correction-predecessor, withdrawal-tombstone, or legacy-duplicate recovery blob referenced by the restored catalog SHALL pass full presence, length, and SHA-256 verification
- **AND** recovery-only predecessor and legacy-duplicate blobs SHALL remain in the backup required set or an isolated non-consumer-visible restore area; they SHALL NOT be republished as a second primary canonical attachment merely to satisfy verification
- **AND** effective-version rows, immutable decision/replacement history, deletion intents, change-event outbox records, recovery-manifest entries, and consumer-processing lineage/current-result bindings SHALL reconcile to the restored catalog without dangling, missing, overwritten, or contradictory transitions
- **AND** a missing or mismatched required blob SHALL keep recovery readiness blocked; sampling MAY be used for routine drills but SHALL NOT replace this enablement gate

#### Scenario: Legacy recovery metadata is migrated
- **WHEN** an older schema contains a recovery manifest with a catalog snapshot watermark or hint but no independently verified recovery-pair closure
- **THEN** migration SHALL preserve or assign the stable reserved pair identity
- **AND** migration SHALL NOT synthesize a closure from that legacy hint
- **AND** primary unlink and restore enablement SHALL remain blocked until both directions of the catalog/file pair are reverified and a new append-only closure is durably recorded

### Requirement: Release Traceability Is Complete And Reproducible
Before further implementation-completion claims, the change SHALL maintain a versioned, machine-validated bidirectional mapping between every independently testable requirements-document leaf and every normative OpenSpec scenario or independently testable normative obligation.
The bidirectional mapping SHALL bind each independently testable item to an exact implementation task and accountable owner.
Release acceptance SHALL preserve those ids.
Release acceptance SHALL add reproducible evidence and final status.
A grouped topic matrix, parent section, whole-file fingerprint, inherited requirement-wide task set, or checked task box SHALL NOT by itself prove requirement coverage.

#### Scenario: Normative coverage is registered
- **WHEN** task 1.8 establishes the coverage baseline
- **THEN** `evidence/traceability_registry.json` SHALL contain one immutable unique id for every independently testable requirements-document leaf and every normative scenario or independently testable SHALL, including normalized clause text hash and an auditable source locator
- **AND** the registry SHALL bind each requirements leaf bidirectionally to the exact OpenSpec clause or scenario, exact implementation task, and accountable owner
- **AND** validation SHALL reject missing, orphan, duplicate, parent-only, or requirement-wide Cartesian mappings
- **AND** renamed or relocated clauses SHALL preserve identity only through an append-only alias history verified against the prior registry baseline; an arbitrary hash SHALL NOT qualify as a valid source alias
- **AND** a normative line containing multiple independent SHALL obligations SHALL be split into separately addressable clauses or rejected by validation rather than duplicated by occurrence count
- **AND** adding a requirements leaf under an existing heading SHALL fail validation until its exact spec, task, and owner mapping exists, even when the whole-document fingerprint is updated
- **AND** checked tasks without exact registered coverage SHALL fail the baseline gate
- **WHEN** task 11.7 performs release acceptance
- **THEN** `evidence/release_trace_report.json` SHALL contain exactly one row per pre-registered id with the reproducible test node or command, bounded live/operator/API-client evidence path where applicable, accountable owner, final status, OpenSpec CLI/schema version, and retained strict-validation result
- **AND** acceptance SHALL fail when any requirements leaf or normative clause lacks a registry id, exact implementation task, evidence owner, or reproducible evidence, or when any checked task lacks matching exact evidence

#### Scenario: Canonical traceability registry is validated directly
- **WHEN** continuous integration or release acceptance evaluates the promoted `evidence/traceability_registry.json`
- **THEN** it SHALL load that canonical file and validate it against the current requirements, specs, tasks, schema, and explicitly pinned prior v2 baseline with complete coverage required
- **AND** source or task fingerprint drift, an invalid previous-baseline chain, an unmapped active leaf or clause, a pending multi-obligation disposition, or an uncovered checked task SHALL fail the gate
- **AND** validation of a generated candidate or migration fixture alone SHALL NOT substitute for direct validation of the promoted canonical registry

#### Scenario: API-only integration is claimed
- **WHEN** release status claims completed AI/API caller integration
- **THEN** the API-client registry SHALL declare `client_mode=ai_api_only`, bind the frozen backend candidate and OpenAPI contract version, and register reproducible acquire, authorization, idempotency, polling, `content_url`, and safe-content evidence
- **AND** an absent or non-passed API-client gate SHALL keep API integration unaccepted
- **AND** no external Web UI repository, owner, page, or deployment SHALL be required; production rollout and consumer cutover remain independently gated

#### Scenario: Latest-only retention is approved
- **WHEN** release acceptance permits deletion of superseded primary PDFs
- **THEN** the evidence set SHALL contain explicit product and operations sign-off that version 1 deletion unlinks the predecessor from the primary consumer-visible archive while retaining metadata and governed non-consumer-visible disaster-recovery bytes, and therefore provides neither normal point-in-time source replay nor secure erasure from every backup medium
- **AND** absence of that sign-off SHALL block destructive rollout and final acceptance

#### Scenario: Full-market scope is approved
- **WHEN** release acceptance claims latest-only full-market bootstrap coverage
- **THEN** the evidence set SHALL contain explicit product sign-off that version 1 “full market” means the independently reconciled snapshot-time still-listed RMB A-share universe on SSE, SZSE, and BSE
- **AND** the sign-off SHALL state that historically delisted and other inactive securities remain bounded on-demand only and are not part of the scheduled bootstrap/daily denominator
- **AND** absence of that sign-off SHALL block the full-market completion claim without blocking bounded development or validation work

### Requirement: The Capability Is Extensible Beyond Annual Reports
The data model and service boundaries SHALL permit future semiannual and other announcement attachment types without weakening version 1 annual-report rules.

#### Scenario: Semiannual support is added later
- **WHEN** a future change enables semiannual reports
- **THEN** that future change is expected to reuse announcement, attachment, blob, acquisition, integrity, API, and processing contracts while defining its own effective-version and retention policy
- **AND** version 1 acceptance SHALL only prove the generic registration/identity/blob boundaries with a test document family
- **AND** version 1 acceptance SHALL NOT claim semiannual production support

#### Scenario: Version 1 discovers a non-annual announcement type
- **WHEN** semiannual or another announcement metadata record is observed before its attachment policy is enabled
- **THEN** version 1 SHALL NOT proactively download that attachment as part of annual-report maintenance
- **AND** it MAY retain normalized metadata according to the existing announcement-layer policy

#### Scenario: Domain-specific announcement is added later
- **WHEN** another announcement type gains multiple consumers
- **THEN** its raw attachment MAY use the shared asset store
- **AND** its business classification, parsing, and fact approval SHALL remain outside the generic asset layer unless separately specified

#### Scenario: A future document family selects an attachment policy
- **WHEN** a future semiannual or other document-family policy is registered
- **THEN** the neutral policy contract SHALL represent `metadata_only`, bounded explicit-universe proactive acquisition, or independently governed full-market proactive acquisition as distinct versioned scopes
- **AND** the policy SHALL define its own effective-version and retention rules and include those rules and the acquisition scope in its normalized fingerprint
- **AND** version 1 SHALL prove the three scopes can round-trip with a neutral test family while leaving production acquisition for that family disabled and performing no unintended attachment download
