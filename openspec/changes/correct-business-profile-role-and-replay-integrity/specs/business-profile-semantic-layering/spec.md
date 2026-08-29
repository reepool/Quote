## ADDED Requirements

### Requirement: Atomic disclosures preserve object identity
The system MUST persist source-supported atomic activities and operating facts at their disclosed object or occurrence granularity and MUST NOT merge distinct inventory objects merely because they share an action, evidence span, segment, or report period.

#### Scenario: Annual report discloses two inventory types
- **WHEN** one report discloses `成品酒` and `半成品酒` as separate stored inventory objects
- **THEN** the system MUST retain two distinct atomic records with their own object identities and common evidence lineage where applicable

#### Scenario: Multiple objects share one table or evidence span
- **WHEN** several disclosed objects originate from the same table or evidence span
- **THEN** shared evidence MUST NOT cause their atomic identities to collapse

### Requirement: Internal inventory does not imply an external storage role
The system MUST interpret generic internal `stores` or inventory-holding activity as an operating disclosure and MUST NOT derive `storage_provider` unless exact evidence and structured service context establish that storage or warehousing is provided to another party.

#### Scenario: Company stores its own inventory
- **WHEN** an approved activity only states that the reporting company stores or holds its own products, materials, work in progress, or inventory
- **THEN** the activity MUST remain available but MUST NOT generate a `storage_provider` role

#### Scenario: Scope of storage activity is unknown
- **WHEN** the semantic result cannot establish whether storage is internal or an external service
- **THEN** the system MUST retain the evidence-backed atomic activity, emit a typed role-derivation gap, and MUST NOT infer a role from confidence alone

### Requirement: Storage semantics use a closed program-facing classification
Every storage-related atomic activity MUST expose a program-facing `storage_semantics` value of exactly `internal_inventory`, `external_service`, or `unknown`. The value MUST be derived from source wording, beneficiary/service hints, and exact evidence by program rules; an LLM role label or confidence score alone MUST NOT set it.

#### Scenario: Internal inventory classification
- **WHEN** the evidence describes the reporting company holding its own finished goods, materials, or work in progress and no third-party service recipient
- **THEN** `storage_semantics` MUST be `internal_inventory` and the activity MUST remain non-role-producing

#### Scenario: External service classification
- **WHEN** the evidence establishes warehousing, storage, or logistics storage provided to a customer, third party, or named counterparty
- **THEN** `storage_semantics` MUST be `external_service` and the normalized service-recipient fields MUST be non-empty before a role can be derived

#### Scenario: Contradictory or incomplete hints
- **WHEN** storage wording or beneficiary information is missing or contradictory
- **THEN** `storage_semantics` MUST be `unknown`, the activity MUST be retained, and role derivation MUST emit a typed gap

### Requirement: External storage service requires governed evidence
The system MUST derive `storage_provider` only from an approved external-service activity whose exact evidence and closed structured classification establish warehousing, storage, logistics-storage, or an equivalent service for another party.

#### Scenario: Explicit third-party warehousing service
- **WHEN** exact evidence states that the company provides warehousing or storage service to customers or another identified party and the structured external-service fields pass validation
- **THEN** the system MUST be allowed to derive a `storage_provider` candidate with the source activity and evidence in its lineage

#### Scenario: Model supplies only a high confidence label
- **WHEN** an LLM labels an activity `storage_provider` with high confidence but the required external-service evidence fields are absent
- **THEN** programmatic governance MUST reject role derivation and preserve only the underlying evidence-backed activity

### Requirement: Derived role identity matches its business scope
The system MUST create at most one current derived role per instrument, governed segment/scope, role, report cohort or business regime, and rule version. The same shared business-identity fields MUST be used by record-id generation and temporal stable identity; `evidence_id` and supporting activity ids MUST NOT be identity fields for a company/scoped role.

#### Scenario: Several qualifying activities support one role
- **WHEN** multiple approved external-service activities in one governed scope support the same role
- **THEN** the system MUST create or reuse one role and MUST retain the sorted distinct supporting activity ids and evidence ids

#### Scenario: Underlying object details are queried
- **WHEN** a consumer needs the products or activities supporting an aggregated role
- **THEN** the supporting atomic records MUST remain independently addressable and MUST NOT be replaced by the role summary

#### Scenario: Same role has a changed support set
- **WHEN** a later extraction adds or removes supporting activities for the same role and report scope
- **THEN** the system MUST update the one role lineage or create a governed successor under `replace`, and MUST NOT create overlapping role records solely because the evidence set changed

### Requirement: Only governed current layers feed downstream publication
Exposure and publication consumers MUST use governed current atomic facts and derived roles according to their own contracts and MUST NOT treat a candidate, held, expired, or semantically invalid role as executable company evidence.

#### Scenario: Invalid inventory-derived role exists historically
- **WHEN** a historical `storage_provider` role is supported only by internal inventory activities
- **THEN** the corrected current projection and downstream publication MUST exclude that role while preserving its audit history until repair is applied

### Requirement: Anonymous contracts remain relationships unless they are concentration facts
An LLM assertion with an unnamed counterparty (for example ``客户 A(1)``) MUST remain a normal relationship when no concentration label or disclosed share is present. The disclosed-share requirement applies only to explicit concentration labels such as ``前五大客户`` or ``前五大供应商``.

#### Scenario: Anonymous contract has no share
- **WHEN** a report identifies an unnamed customer or supplier for a specific contract or transaction and ``disclosed_share`` is absent
- **THEN** the system MUST persist a disclosed-name-only relationship edge with ``anonymous=true`` and MUST NOT reject the entire semantic batch
