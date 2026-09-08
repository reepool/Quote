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
For the business-regime chapter task, the workflow MUST keep consolidation-scope changes separate from principal-business, product, or service change status. When complete Evidence explicitly reports no applicable major business change, the workflow MUST allow `business_regime` coverage with status `not_applicable` and source-supported reason; it MUST NOT fabricate a BusinessEvent from a cross-reference or merge the no-change statement into a separate consolidation event.

#### Scenario: Business change field says not applicable
- **WHEN** the requested business-change field is complete and explicitly marked not applicable
- **THEN** the workflow may complete that checklist obligation with `not_applicable` coverage and Evidence
- **AND** no event is required merely to avoid an empty record array

### Requirement: Material-input verification preserves explicit energy inputs
For `extract_material_inputs`, the governed `material_input` Relationship MUST accept both explicitly named raw-material inputs and explicitly named energy inputs when the Evidence identifies them as procured or consumed inputs. Verification MUST preserve the source-native item name and MUST NOT return `object_not_allowed` solely because the report classifies an item as energy. This rule MUST NOT relabel energy as raw material or authorize CommodityExposure inference.

#### Scenario: A source table lists steam and electricity as energy inputs
- **WHEN** a complete “主要原材料及能源” table explicitly lists steam or electricity as an input
- **THEN** verify may pass the source-supported `material_input` Relationship
- **AND** the result remains a research fact without inferred price sensitivity or production publication

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
