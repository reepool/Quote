## MODIFIED Requirements

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
