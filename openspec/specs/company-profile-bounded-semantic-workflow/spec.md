# company-profile-bounded-semantic-workflow Specification

## Purpose
TBD - created by archiving change implement-company-profile-common-semantic-model. Update Purpose after archive.
## Requirements
### Requirement: Each semantic request is bounded to one chapter task
The workflow MUST define separate versioned `extract`, `repair`, and `verify` request and response models. Every request MUST identify one report, one active package manifest, one chapter task, its checklist, allowed object and enum values, prohibited inferences, and a continuous Evidence bundle containing required headers, units, footnotes, and continuation pages. The v1 chapter-task set is closed to `extract_business_overview`, `extract_segment_financials`, `extract_operating_quantities`, `extract_material_inputs`, `extract_counterparties_and_concentration`, and `extract_business_regime`; an unknown task is rejected before provider invocation. Missing required preparation inputs MUST fail before an LLM provider is called.

#### Scenario: Table row is supplied without its unit header
- **WHEN** a numeric table extract request omits the header or footnote that owns the unit
- **THEN** request preparation returns a typed context-incomplete failure
- **AND** the provider is not called

#### Scenario: One request attempts multiple chapter tasks
- **WHEN** a request combines business overview, segment financials, and counterparties into one free-form prompt
- **THEN** contract validation rejects the request
- **AND** no candidate is accepted from that response

#### Scenario: Unknown chapter task is requested
- **WHEN** a request uses a chapter task outside the frozen v1 set
- **THEN** request validation returns a typed unknown-task failure
- **AND** the provider is not called

### Requirement: Deterministic extraction precedes bounded LLM fallback
The application workflow MUST first accept deterministic results for source structures that satisfy the active task contract and MUST invoke semantic fallback only for unresolved cells, headers, narrative meaning, subject, period, or event boundaries. In stage four, deterministic input means an already structured fixture candidate with table headers, cells, units, and anchors; this change MUST NOT implement PDF selection, OCR, table parsing, or page-layout recovery. A deterministic and LLM candidate for the same physical occurrence MUST be reconciled through one validator rather than published twice.

#### Scenario: Standard revenue cost margin table is complete
- **WHEN** the table parser provides complete row labels, metric headers, units, cells, and anchors
- **THEN** the workflow creates deterministic Segment and Measurement candidates without an extract-model call
- **AND** only unresolved semantic fields, if any, can enter repair or review

#### Scenario: Multi-page header is ambiguous
- **WHEN** deterministic parsing cannot bind a continued page to the owning header
- **THEN** the bounded extract fallback receives the complete continuation Evidence and active checklist
- **AND** it returns source-native candidates or `unclear`, not a guessed canonical value

### Requirement: Candidate responses are schema and semantic constrained
Every candidate response MUST preserve request identity, allowed object/metric/action values, source-native data, physical Evidence, subject and period semantics, uncertainty, and prohibited-inference status. It MUST preserve `processing_direction`, `identity_class`, `row_class=consolidation_adjustment`, `activity_actor`, and `comparison_basis` when applicable; missing comparison basis on a restated comparative is a blocker, not a repairable guess. The validator MUST reject JSON-external prose, unknown enums, canonicalized source values, unrequested fields, missing required capacity/comparison semantics, and Activity/Measurement mixing.

#### Scenario: LLM converts a reported percentage
- **WHEN** a response changes source value `23.84` with unit `%` into source value `0.2384`
- **THEN** validation returns a typed source-value mutation failure
- **AND** the converted candidate cannot enter verification or the profile projection

#### Scenario: LLM returns a new commodity direction
- **WHEN** an extract response labels a material as profit-positive or profit-negative
- **THEN** validation rejects the prohibited inference
- **AND** the disclosed material fact, if otherwise supported, remains separate from commodity exposure

### Requirement: Repair is typed scoped and bounded
A repair request MUST include the original request, original candidate or coverage result, one supported typed error, and an allowlist of writable fields. It MUST NOT expand to a different chapter, checklist field, object, or evidence range. The stage-four workflow MUST permit at most one repair attempt for a candidate before returning a deterministic unresolved result.

#### Scenario: Capacity kind is ambiguous
- **WHEN** an otherwise complete capacity candidate lacks a supported `capacity_kind`
- **THEN** repair may fill the kind only from supplied header or narrative Evidence or leave it `unclear`
- **AND** it cannot select a kind from magnitude or industry custom

#### Scenario: Repair changes an unlisted field
- **WHEN** a repair response modifies a JSON field outside the request's writable-field allowlist
- **THEN** the response is rejected as a contract failure
- **AND** the original candidate remains unresolved

### Requirement: Verification is independent and cannot mutate facts
Verify MUST evaluate each candidate and each active checklist item against the original Evidence, returning `pass`, `block`, or `unclear` checks and reason codes. It MUST NOT add candidates, edit source values, choose package assignment, grant approval, perform canonical conversion, or waive a blocking check through an aggregate score.

#### Scenario: Third-party actor is not rewritten
- **WHEN** a report describes a trader selling to the end customer while the issuer only supplies or contracts with that trader
- **THEN** verify blocks or leaves unresolved an issuer-level `sells` Activity
- **AND** the source-supported third-party actor remains the only permitted actor

#### Scenario: Aggregate counterparty names remain undisclosed
- **WHEN** a top-five section reports only aggregate amounts without names
- **THEN** the workflow may pass the source-bound concentration Measurement and independently verified name coverage `not_disclosed`
- **AND** it blocks any Relationship candidate whose Evidence is bound only to the concentration field before verification

#### Scenario: Separate aggregate counterparty identity is source-bound
- **WHEN** another section explicitly identifies a transaction counterparty as `集团所属单位` or an equivalent report-local aggregate
- **THEN** verify may pass an independent `identity_class=report_local_aggregate` Relationship whose Evidence is bound to the relationship field
- **AND** it does not import that identity into the top-five names checklist

#### Scenario: Candidate subject is unsupported
- **WHEN** a candidate claims `consolidated_group` from the word `公司` without affirmative evidence
- **THEN** verify returns `block` or `unclear` for subject support
- **AND** it does not rewrite the candidate to force completion

#### Scenario: Most candidates pass but a required table is omitted
- **WHEN** candidate-level scores are high but an active required checklist item has no result
- **THEN** verify blocks task completion for missing coverage
- **AND** average accuracy cannot override the blocker

### Requirement: Workflow dispositions and human review material are explicit
The workflow MUST emit a disposition for every deterministic or model request-local candidate: accepted-for-review, blocked with a reason, or unresolved. It MUST derive task completeness from committed candidate dispositions and CoverageResults and MUST generate a human review package containing the candidate, source Evidence, conflicting interpretations, and reason codes. The human review package MUST NOT be an LLM request.

#### Scenario: Candidate remains semantically ambiguous after repair
- **WHEN** one bounded repair attempt cannot resolve the subject or field meaning
- **THEN** the workflow emits an unresolved disposition and a human review item
- **AND** it does not mark the checklist field observed or the task complete

### Requirement: Gold fixtures prove the executable contract without production side effects
The stage-four test suite MUST load the approved manufacturing/materials Gold and negative cases as local fixtures and MUST verify key positive, legal-empty, and blocking semantics. Tests MUST use an injected fake provider when provider interaction is necessary and MUST NOT access the network or production database.

#### Scenario: Restated comparative without basis is blocked
- **WHEN** a candidate represents a same-control or otherwise restated comparative without `comparison_basis`
- **THEN** verify returns a deterministic blocker
- **AND** the task cannot become complete until the basis is supplied or the field is marked unresolved

#### Scenario: Manufacturing Gold contract suite runs
- **WHEN** the approved stage-three fixture suite executes
- **THEN** it proves Activity/Measurement separation, source-native preservation, subject uncertainty, capacity kind, comparison basis, anonymous relationships, processing direction, page coordinates, coverage honesty, and prohibited-inference blockers
- **AND** no production state is read or written

### Requirement: Report usability requires complete core chapters and no frozen blocker
The bounded workflow MUST derive report status from the six core chapter tasks: business overview and Activity, segment facts or legal empty, operating quantities or legal empty, material/energy inputs or legal empty, counterparties/concentration or legal empty, and business regime event or legal empty. A required core chapter with `extraction_failed`, `deadline_exceeded`, missing coverage, or unresolved frozen §14 blocker MUST keep the report at `hold` or `failed`; an `unclear` subject alone MUST NOT fail the entire report when the field-level usage policy is satisfied.

#### Scenario: Required chapter is incomplete
- **WHEN** `extract_business_regime` ends with `deadline_exceeded` or another required-result failure
- **THEN** the report remains `hold` or `failed`
- **AND** it cannot be labeled `usable_with_caveats` until the chapter is rerun successfully

#### Scenario: Unclear subject does not block a qualitative result
- **WHEN** an Activity is accepted with complete Evidence and `subject_scope=unclear`
- **THEN** the workflow keeps the Activity available for research projection
- **AND** it does not create a report-level blocker solely for that subject scope

### Requirement: Confidence and usage restrictions are deterministic workflow outputs
The workflow MUST preserve the existing candidate, CoverageResult, disposition, and Evidence models as the authority for acceptance. It MUST derive confidence and usage restrictions programmatically after verification, without LLM-supplied probabilities, free-form usage lists, weighted scores, or automatic subject promotion.

#### Scenario: Accepted uncertain record is bounded
- **WHEN** a record is accepted for research with complete Evidence and `subject_scope=unclear`
- **THEN** the workflow derives a bounded confidence no higher than `medium`
- **AND** its permitted projection uses come from the closed policy table

#### Scenario: Unsupported promotion remains blocked
- **WHEN** a model response promotes source wording `公司` to `consolidated_group` without affirmative evidence
- **THEN** the existing independent verification blocks the candidate
- **AND** the new usability policy does not waive that blocker

### Requirement: Verification preserves one primary metric for a composite source-native label
Verification MUST evaluate a candidate against its requested checklist field, physical Evidence anchor, source meaning, and complete source-native label. A parenthetical or secondary source word MUST NOT require a second metric or cause `evidence_field_mismatch` when the same physical fact has one governed primary metric. This rule MUST NOT permit a candidate whose metric conflicts with the economic direction or a different table header.

#### Scenario: External processing volume includes a sales alias
- **WHEN** one Evidence anchor says `涂覆加工量（销量）` and the source describes an external processing service provided by the reporting business
- **THEN** verify may pass one `processing_volume` preserving that complete label
- **AND** it does not require or synthesize `sales_volume` from the same anchor

### Requirement: No-change disclosures use coverage rather than fabricated events
For the business-regime chapter task, the workflow MUST keep consolidation-scope changes separate from principal-business, product, service, and statistical-calibre change status. Complete Evidence that explicitly reports no applicable major business change MAY produce `business_regime` coverage with status `not_applicable` and a source-supported reason, but a checkbox concerning only principal-business statistical calibre MUST NOT close the broader regime field. A `not_applicable` result MUST be rejected when the same bounded Evidence affirmatively reports an applicable consolidation-scope/control change or describes a source-supported inclusion, establishment, acquisition, transfer, or equivalent change. The workflow MUST NOT fabricate a BusinessEvent from a cross-reference, merge a no-change statement into a separate consolidation event, or silently discard a contradicted result.

#### Scenario: Statistical-calibre checkbox is narrow
- **WHEN** Evidence marks only the question about adjustment of principal-business statistical calibre as not applicable
- **THEN** the workflow does not use that checkbox as `business_regime=not_applicable`
- **AND** the broader regime requirement remains dependent on an owning business/control-change disclosure

#### Scenario: Not-applicable coverage conflicts with a consolidation change
- **WHEN** a response returns `business_regime=not_applicable` while the same Evidence marks consolidation-scope change applicable or describes a subsidiary inclusion, establishment, acquisition, transfer, or equivalent change
- **THEN** normalization rejects the response through the existing typed schema/repair path
- **AND** it does not synthesize, mutate, or silently drop an event or coverage result

#### Scenario: Business change field explicitly says not applicable
- **WHEN** a complete owning business-change field is explicitly marked not applicable and contains no contradictory change disclosure
- **THEN** the workflow may complete that checklist obligation with `not_applicable` coverage and Evidence
- **AND** no event is required merely to avoid an empty record array

### Requirement: Material-input verification preserves explicit energy inputs
For `extract_material_inputs`, the governed `material_input` Relationship MUST accept both explicitly named raw-material inputs and explicitly named energy inputs when the Evidence identifies them as procured or consumed inputs. Verification MUST preserve the source-native item name and MUST NOT return `object_not_allowed` solely because the report classifies an item as energy. A generic accounting or cost category such as `材料`, `原材料`, `燃料`, or `能源` MUST NOT become a named `material_input` Relationship unless the same bounded Evidence directly identifies that expression as the specific procured or consumed item. An invalid generic candidate MUST be rejected through the existing typed path rather than silently removed or converted into legal-empty coverage. This rule MUST NOT relabel energy as raw material or authorize CommodityExposure inference.

#### Scenario: A source table lists steam and electricity as energy inputs
- **WHEN** a complete major-materials-and-energy table explicitly lists steam or electricity as an input
- **THEN** verify may pass the source-supported `material_input` Relationship
- **AND** the result remains a research fact without inferred price sensitivity or production publication

#### Scenario: A cost table contains only a generic material category
- **WHEN** the model returns `材料` or another generic cost heading as the object name and the same Evidence does not identify it as a specific procured or consumed input
- **THEN** normalization rejects the candidate through the existing typed schema/repair path
- **AND** the workflow does not silently accept, drop, or reinterpret it

### Requirement: Product-extension events are not overridden by non-regime no-change wording
In the `business_mode_and_extension` scope, an evidenced `product_extension` BusinessEvent MUST complete the `business_regime` checklist field through accepted observed coverage. A separate statement that the operating mode did not materially change MUST NOT create a BusinessRegime or a legal-empty coverage result that overrides the accepted event.

#### Scenario: A new product is supplied while the operating mode remains unchanged
- **WHEN** the Evidence reports a new product or service supply during the period and separately says the operating mode did not materially change
- **THEN** the workflow retains the product-extension event and derives observed coverage from that accepted event
- **AND** it does not add a conflicting `not_applicable` result for the same checklist field

### Requirement: Same-control comparative verification is column and knowledge-time aware
Verification MUST require `comparison_basis` for each explicitly restated comparative and MUST preserve reported period, knowledge time, and subject evidence per column. It MUST NOT treat a current-period value as restated merely because prior-year columns exist, and MUST NOT reject a valid restated column solely because its value differs from the predecessor's original annual report.

#### Scenario: Adjusted and pre-adjustment columns coexist
- **WHEN** Evidence includes clearly labelled adjusted and pre-adjustment comparative columns
- **THEN** verify checks each candidate against its own column label and comparison basis
- **AND** it blocks any instruction to overwrite the predecessor original-as-published fact

### Requirement: Safely blocked supplemental candidates do not erase satisfied coverage
The workflow MUST derive observed coverage from accepted records even when the same field also contains an additional candidate that is safely blocked as `object_not_allowed`. This exception MUST apply only when the field has at least one accepted record and the supplemental candidate's only blocking reason is `object_not_allowed`. The blocked candidate MUST remain in dispositions and review material, MUST remain absent from the research projection, and MUST NOT be treated as accepted. Any other blocked, unresolved, conflicting, missing-verification, Evidence, subject, provider, or contract failure MUST continue to block task completion according to the existing contract.

#### Scenario: Invalid top-five aggregate relationship accompanies valid ranked facts
- **WHEN** a top-five scope has accepted anonymous ranked Relationships and concentration Measurements plus one blocked aggregate Relationship whose only reason is `object_not_allowed`
- **THEN** the accepted records satisfy observed counterparty coverage and the supplemental rejection does not make the task incomplete
- **AND** the aggregate Relationship remains blocked, reviewable, and absent from projection

#### Scenario: Accepted row accompanies a substantive verifier failure
- **WHEN** one field has an accepted record and another record is blocked for `subject_unsupported`, Evidence mismatch, provider failure, or another non-supplemental reason
- **THEN** the failing record remains a task-completion blocker under the existing contract
- **AND** the accepted record does not hide that failure

### Requirement: Explicit consolidation-adjustment rows carry their source-supported subject
The workflow MUST recognize a Segment or Measurement row as an explicit consolidation adjustment only when its source-native row label identifies consolidation and elimination or adjustment semantics and the candidate preserves `row_class=consolidation_adjustment`, `subject_scope=consolidated_group`, and `subject_basis=direct_source_wording`. Independent verification MUST NOT block such a candidate solely with `subject_unsupported`. This rule MUST NOT apply to ordinary product or segment rows, to source wording that only says `公司`, or to a candidate with any additional Evidence, value, unit, field, period, or prohibited-inference failure.

#### Scenario: Consolidation elimination row is explicit
- **WHEN** a source-native row label such as `合并抵消项` passes the adjustment-label validator and its Segment or Measurement carries the required row class and subject fields
- **THEN** verification may pass the source-supported consolidated subject
- **AND** the adjustment fact remains distinct from ordinary product or segment facts

#### Scenario: Ordinary company wording is not promoted
- **WHEN** an ordinary row or narrative uses only `公司` and lacks affirmative consolidated Evidence
- **THEN** verification continues to block or retain `subject_scope=unclear`
- **AND** the consolidation-adjustment rule cannot promote it to `consolidated_group`

#### Scenario: Adjustment row has another verification failure
- **WHEN** an explicit adjustment candidate also has an Evidence, value, unit, field, period, or prohibited-inference reason
- **THEN** that additional reason remains blocking
- **AND** subject normalization cannot convert the check to pass

### Requirement: A hash-bound shadow manifest may admit reports without changing historical closed sets
The bounded workflow MUST support a distinct `manufacturing_materials_shadow_batch` manifest and Evidence-plan mode whose report identities are validated against the active versioned manifest, local PDF hashes, and manifest hash at the application boundary. This mode MUST require exactly twenty reports and MUST NOT add them to `APPROVED_STAGE5_SAMPLES`, `KNOWN_STAGE5_SAMPLES`, or any historical four-report/OOS constant. Existing four-report and OOS manifest kinds, report limits, run-bundle semantics, and success rules MUST remain unchanged.

#### Scenario: Historical Stage 5 mode receives a shadow report
- **WHEN** a four-report or OOS operator receives a report that is present only in a shadow manifest
- **THEN** the existing closed-set validation rejects it
- **AND** the shadow admission mode cannot be selected implicitly

#### Scenario: Shadow report identity matches the active manifest
- **WHEN** a shadow report, Evidence plan, prepared scopes, and report output all match the same active manifest revision, manifest hash, instrument identity, report period, PDF hash, and sample identity
- **THEN** the application service may run the existing bounded semantic workflow for that report
- **AND** the resulting payload remains research-only and is referenced by a separate shadow-batch result

### Requirement: Automatic Evidence planning reuses governed document owners
The bounded workflow MUST allow the shadow application path to translate existing immutable PDF artifacts and governed selected sections into the existing six chapter tasks and `PreparedRequestScope` contract. This translation MUST preserve physical page and source hashes, MUST derive each scope field subset from positive source signals inside that scope, MUST require a chapter-owning disclosure before the scope can establish legal-empty coverage, MUST retain required owning table context, and MUST validate the existing field contract and report-level required-field ownership before provider invocation. Incidental terms in financial statements, balance-sheet variance commentary, retained-earnings notes, generic business commentary, or unrelated risk text MUST NOT be treated as chapter ownership. It MUST NOT introduce a new parser, selector, semantic field, verification owner, answer-bearing plan format, or model-selected request contract.

#### Scenario: Generated plan uses an unknown unsupported or non-owning source
- **WHEN** automatic planning produces a field outside the existing Stage 5 contract, assigns a known field without a positive source signal, or uses a non-owning disclosure for chapter-level legal-empty authority
- **THEN** preparation or replay validation fails before provider invocation with a typed plan/field-contract diagnostic
- **AND** the planner does not add a semantic field or rely on model output to accommodate the report

#### Scenario: Generated scopes are semantically executable
- **WHEN** a generated plan passes manifest, hash, page, Evidence, owner, field-signal, table-context, required-field-ownership, and field-contract validation
- **THEN** each scope remains eligible for submission to the existing `CompanyProfileSemanticService` owner
- **AND** verification, dispositions, projection restrictions, and report status continue to use the same rules as the prior research slice

#### Scenario: A chapter is split across multiple scopes
- **WHEN** different governed scopes in one report chapter support different existing fields
- **THEN** the workflow evaluates each scope only against its emitted field subset and aggregates required chapter coverage across the report
- **AND** genuine scope-local schema, verification, Evidence, subject, or prohibited-inference failures remain blockers

### Requirement: High-cardinality segment extraction is deterministically partitioned
The Stage 5 adapter MAY partition only `extract_segment_financials` requests that meet a frozen source-cardinality threshold and contain at least two unresolved output-bearing metric fields. Each partition MUST retain the same report, scope, Evidence identities, period and subject constraints, MUST include `segment_dimension`, and MUST request a disjoint metric-field subset. All other chapter tasks and non-eligible segment requests MUST remain single extract calls.

#### Scenario: A wide segment table meets the partition threshold
- **WHEN** a bound segment-financial scope meets the frozen numeric-cardinality threshold and requests revenue, cost, and reported margin
- **THEN** the adapter creates stable metric partitions with derived request IDs and identical Evidence
- **AND** the union of partition fields exactly equals the original requested field set

#### Scenario: Another chapter has large source text
- **WHEN** a business-overview, operating-quantity, material, counterparty, or regime scope is large
- **THEN** this partition policy does not split the request
- **AND** existing bounded execution and typed failure behavior remains authoritative

### Requirement: Partition results merge fail-closed before semantic expansion
Partition responses MUST be merged at the compact segment-row layer under the original request identity. Before row identity comparison, an ordinary row whose controlled Evidence binds its source dimension and label MUST normalize absent, `unclear`, and `business_segment` scope to the existing `business_segment` representation. A `consolidated_group` draft with `subject_basis=report_default_group_scope` MUST collapse to `business_segment` when paired with that segment representation, but the existing stronger-basis rule MUST retain a direct-source or numeric-reconciled consolidated-group result when it is paired only with the report-default draft. Explicit issuer, named-subsidiary, affirmative consolidated-group subjects, and consolidation-adjustment rows MUST remain distinct. Rows may merge only when their source dimension, label, normalized period, normalized subject fields, and Evidence IDs match; metric cell keys MUST be disjoint. Coverage MUST remain field-local. The combined result MUST pass the original minimal schema, full `ExtractResponse` validation, existing occurrence reconciliation, independent verification, and report projection without lowering any requirement.

#### Scenario: Revenue and cost partitions describe the same row
- **WHEN** two successful partitions return the same Evidence-bound source row with disjoint revenue and cost cells, and one uses the report-default group draft while the other uses `unclear` or `business_segment`
- **THEN** the adapter normalizes both ordinary rows to `business_segment`, combines the cells into one compact row, and expands it once under the original request ID
- **AND** the workflow does not create duplicate Segment records or a false subject identity conflict

#### Scenario: Stronger group basis refines the report default
- **WHEN** one partition uses `report_default_group_scope` and another partition for the same row carries a direct-source or numeric-reconciled consolidated-group basis
- **THEN** the existing stronger group basis remains authoritative
- **AND** the report-default draft is not prematurely collapsed to `business_segment`

#### Scenario: Explicit group subject conflicts with a segment row
- **WHEN** one partition has an affirmative direct-source or numeric-reconciled consolidated-group subject and another partition resolves the same anchor only as a business segment
- **THEN** the extract ends with a typed schema/contract failure
- **AND** the report-default normalization does not erase the substantive subject conflict

#### Scenario: Consolidation adjustment retains special subject semantics
- **WHEN** a partition row is a `consolidation_adjustment`
- **THEN** its existing explicit wording or reconciliation subject rules remain authoritative
- **AND** it is not normalized to `business_segment`

#### Scenario: Other row identities conflict
- **WHEN** partitions disagree on source dimension, label, Evidence IDs, normalized period, preserved subject semantics, row class, or return the same metric cell twice
- **THEN** the extract ends with a typed schema/contract failure
- **AND** no partial partition result enters verification or the research projection

### Requirement: Physical partition calls retain truthful accounting
Every physical partition request MUST consume the existing provider-call budget before gateway invocation and MUST emit a trace with the parent semantic request ID and stable partition index/count. Consecutive physical partition traces MAY map to one logical workflow `extract`, but repair and verify order MUST remain unchanged. A failure in any required partition MUST fail the logical extract without importing another run or model.

#### Scenario: One of three partitions fails
- **WHEN** the second physical partition ends with a typed provider, deadline, truncation, parse, or schema failure
- **THEN** the provider budget and traces include both attempted physical calls and the logical extract fails with that typed error
- **AND** the successful first partition is not returned as a complete extract

### Requirement: A unique complete segment heading is bound from controlled Evidence

For `extract_segment_financials`, when no explicit `source_row_dimensions` mapping exists and controlled Evidence yields exactly one supported complete segment table heading, the Stage 5 adapter MUST own that heading locally. The compact provider row schema MUST omit and forbid `dimension`; normal rows MUST be expanded and partition-merged using the exact Evidence heading. The provider MUST continue to own the row label, metric cells, period, subject decision, and Evidence IDs. An explicit `source_row_dimensions` mapping MUST retain precedence.

#### Scenario: One complete report-segment heading is locally bound

- **WHEN** controlled Evidence contains exactly one supported complete heading `报告分部的财务信息` and a compact normal row omits `dimension`
- **THEN** the adapter expands the row with `segment_dimension=报告分部的财务信息`
- **AND** the resulting Segment and Measurements preserve the model-supplied row label, cells, period, subject decision, and Evidence IDs.

#### Scenario: Explicit row mapping remains authoritative

- **WHEN** the prepared scope provides `source_row_dimensions` for its allowed labels
- **THEN** the existing per-row local mapping remains authoritative
- **AND** the unique-scope-heading rule does not replace or broaden that mapping.

### Requirement: Ambiguous segment headings remain provider-owned and fail closed

The Stage 5 adapter MUST NOT locally bind a segment heading when controlled Evidence contains zero or more than one supported complete heading. It MUST NOT derive a complete heading from a prefix, translated value, neighboring narrative heading, or inferred business meaning. Existing source-label, full-heading, Evidence, period, subject, duplicate-cell, coverage, and row-identity validation MUST remain enforced.

#### Scenario: Multiple complete headings are not automatically selected

- **WHEN** one controlled scope contains two supported complete headings such as `分产品` and `分地区`
- **THEN** the provider schema continues to require a source dimension
- **AND** the adapter does not choose either heading locally.

#### Scenario: Short prefix is not repaired when local binding is unavailable

- **WHEN** local binding is unavailable because controlled Evidence contains multiple supported headings including `报告分部的财务信息`, and a provider returns `报告分部`
- **THEN** existing validation rejects the ambiguous prefix
- **AND** the adapter does not rewrite it to the complete heading.

### Requirement: Consolidation adjustments retain their special dimension

A compact row marked `row_class=consolidation_adjustment` MUST resolve to `dimension=adjustment` after its label passes the existing explicit adjustment checks, even when the surrounding scope has one unique locally bound table heading. This rule MUST NOT relax the existing subject or reconciliation requirements for consolidation adjustments.

#### Scenario: Adjustment row does not inherit the normal table heading

- **WHEN** a uniquely bound segment scope contains a valid `consolidation_adjustment` row
- **THEN** the expanded Segment and Measurements use `segment_dimension=adjustment`
- **AND** normal rows in the same scope use the unique complete Evidence heading.

### Requirement: Provider Evidence catalogs identify field ownership
The provider-facing Evidence catalog MUST classify each Evidence item as `field_owner`, `context_only`, or `mixed`, and MUST list the exact checklist field IDs that own it. The roles MUST be derived from existing prepared field bindings without changing immutable Evidence identities or source content.

#### Scenario: Context-only Evidence is present
- **WHEN** a request includes an Evidence item with no prepared field binding
- **THEN** the provider catalog marks it `context_only` with an empty owning-field list
- **AND** the item remains available for context but cannot be treated as field-owning proof

#### Scenario: One Evidence item supports multiple fields
- **WHEN** the same Evidence item is prepared with bindings for two checklist fields
- **THEN** the provider catalog marks it `mixed` and lists both exact field IDs
- **AND** it does not authorize the Evidence for any unlisted field

### Requirement: Legal-empty verification requires owning Evidence
Legal-empty coverage MUST be accepted only when the cited Evidence owns the requested field or the source explicitly contains the requested field's governed not-applicable disclosure. Context-only Evidence alone MUST remain insufficient, and verification MUST continue returning the existing typed block or unclear outcome rather than mutating the candidate.

#### Scenario: Context-only Evidence is used for legal-empty coverage
- **WHEN** a model cites contextual page text but no Evidence item owns the requested checklist field
- **THEN** verification blocks or leaves the coverage unresolved with the existing typed coverage reason
- **AND** it does not treat the contextual text as proof that the field is undisclosed or not applicable

#### Scenario: Control-scope wording is narrower than business regime
- **WHEN** Evidence says only that consolidation scope or control did not change
- **THEN** verification does not accept that statement as broader principal-business, product, service, or statistical-regime no-change coverage
- **AND** an owning broader regime disclosure is still required

### Requirement: Segment candidates require segment-owning Evidence
A cost-component or generic operating-cost row MUST NOT be emitted as a business Segment unless the supplied field-owning Evidence explicitly identifies a segment dimension and segment row. The existing closed schema and semantic validator MUST remain authoritative when the model violates this instruction.

#### Scenario: Cost composition is mistaken for a segment
- **WHEN** a model cites a cost-component table as the basis for a business Segment without a field-owning segment dimension
- **THEN** the response is rejected or remains unresolved through the existing schema/semantic path
- **AND** the cost fact is not relabeled as a business segment

### Requirement: Company-profile provider failures use bounded pool failover
New company-profile semantic requests MUST use the existing logical `semantic_extraction` route so the configured LLM pool can select among its eligible candidates. A single logical request MAY fail over to another pool member for configured rate-limit, transient transport, provider, response-parse, or schema-validation failures, subject to the pool's finite hop limit and one execution deadline. Semantic uncertainty, unsupported inference, Evidence mismatch, and verifier rejection MUST NOT trigger blind model cycling. Every physical attempt and typed error MUST remain in existing gateway lineage; the stage-five trace MUST retain the selected provider/model and final disposition without merging failed-attempt candidates.

#### Scenario: Primary provider is unavailable
- **WHEN** the selected model returns a configured transient/provider failure and execution time remains
- **THEN** the logical request selects the next eligible pool member within the finite hop limit
- **AND** the trace records both the failed attempt and the selected fallback model

#### Scenario: Fallback model succeeds
- **WHEN** a fallback model returns a schema-valid response that passes local validation
- **THEN** the logical request returns that response with its selected model and failover count
- **AND** no candidate from the failed attempt is merged into the result

#### Scenario: Semantic rejection is not provider failure
- **WHEN** local validation or independent verification rejects a candidate for subject, Evidence, metric, or prohibited-inference reasons
- **THEN** the request records the typed semantic disposition
- **AND** it does not automatically cycle through all remaining models

#### Scenario: All eligible providers fail
- **WHEN** the finite failover hop limit or execution deadline is exhausted
- **THEN** the request returns the final typed provider failure
- **AND** the report remains failed/hold according to the existing report-isolation policy

### Requirement: Active extract fields are complete at the provider boundary
Every company-profile extract response MUST represent each active non-optional checklist
field with either a candidate object for that field or a legal-empty coverage result
allowed by the checklist. Empty arrays and partial responses that omit an active
non-optional field MUST fail the model-side response schema as
`schema_validation_error`, so the existing logical pool MAY perform bounded failover.
Optional fields MAY remain absent. A candidate rejected later for Evidence, subject,
metric meaning, or prohibited inference remains a semantic rejection and MUST NOT cause
blind model cycling.

#### Scenario: Required legal-empty response is omitted
- **WHEN** a material-input or business-regime scope returns no candidate and no permitted coverage for its active non-optional field
- **THEN** the response fails schema validation before it is recorded as provider success
- **AND** the existing finite pool failover policy may select another eligible model

#### Scenario: One field in a multi-field scope is missing
- **WHEN** a response represents some active fields but omits another active non-optional field
- **THEN** the response fails the field-completeness constraint
- **AND** no candidate from that failed physical response is spliced into a later attempt

#### Scenario: Optional field is absent
- **WHEN** a response omits an active field whose checklist requirement level is `optional`
- **THEN** the response may remain schema-valid
- **AND** the workflow retains its existing optional coverage semantics

#### Scenario: Represented candidate is semantically rejected
- **WHEN** a response represents the required field but independent verification rejects the candidate for unsupported inference or Evidence mismatch
- **THEN** the semantic disposition is preserved
- **AND** the rejection does not trigger provider failover
