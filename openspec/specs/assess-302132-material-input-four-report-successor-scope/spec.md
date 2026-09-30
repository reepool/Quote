# assess-302132-material-input-four-report-successor-scope Specification

## Purpose

This assessment asks whether the approved 2025 annual report of `302132.SZ` can supply a bindable named material input for `extract_material_inputs`. The closed result is unsuitable. The dossier found no source-native named material and no formal main-materials table. Procurement is only a process description, cost composition is only industry totals, related-party purchases say only "采购商品", and suppliers appear only as totals and ratios. Generic labels, energy, inventory classes, and accounting policy were not promoted to named inputs. Unsuitable does not create a four-report material-input successor, does not add `302132.SZ` to the three-report material-input result, and leaves the Stage 4 aggregate on hold. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false.

## Requirements

### Requirement: The assessment uses one approved report and one existing chapter
Before the 1.1 review passes, the change MUST only contain its scope documents. The assessment MUST use only `302132.SZ` on SZSE, report identifier `asset_0a488da55636b09107be6d719c9ebf39`, document version `ver_2d20ba3aebc5fac6c562cd619695995a`, report period `2025-12-31`, publication time `2026-04-28T16:00:00+00:00`, and PDF content hash `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020`. It MUST assess only the existing chapter `extract_material_inputs`. It MUST NOT add an issuer outside the approved list, reassess the major reorganization or package regime, change Python, enqueue, replay, or prefill a metric.

#### Scenario: Scope review does not open the dossier
- **WHEN** the 1.1 review has not yet accepted this change
- **THEN** no material-input dossier is written
- **AND** no fitness result is recorded
- **AND** no historical artifact bytes change

### Requirement: The material-input dossier is new
After scope approval, the dossier MUST be a new material-input dossier for this report. The regime dossier `docs/development/company_profile_manufacturing_materials_dossier_302132_2025_regime.md` MUST NOT be reused as that dossier. The sample identity `manufacturing-materials-302132-2025-regime` MUST NOT be treated as a material-input conclusion. Page ranges from another chapter MUST NOT be copied in as material-input evidence.

#### Scenario: A regime baseline is not a material-input review
- **WHEN** the dossier is written for `extract_material_inputs`
- **THEN** it is a new dossier bound to this chapter
- **AND** the regime dossier remains unread as this chapter's evidence

### Requirement: Each candidate keeps its own evidence binding
The dossier MUST check procurement mode and named procurement objects, a formal main-materials table, named inputs inside cost composition, named material rows inside related-party procurement, explicit raw-material disclosure in the risk chapter, and any resulting `not_disclosed`, `not_applicable`, `unclear`, or `extraction_failed`. Each candidate MUST bind a physical page, the formal section and header, a bounded quote, the page-text hash, the subject, the report period, the source-native name, and the unit where the source states one. The physical page MUST be the 1-based pypdf page order. The page-text hash MUST be the SHA-256 of that page's stripped extracted text. A printed page number MUST NOT replace the physical page. A candidate missing any required binding MUST NOT be recorded as an observed fact.

#### Scenario: An unbound sentence is not an observed fact
- **WHEN** a sentence names a material but has no physical page, formal header, bounded quote, page-text hash, subject, or report period
- **THEN** the dossier does not record it as an observed material input
- **AND** the missing binding stays visible

### Requirement: Named inputs are not inferred
The dossier MUST NOT supply a material from aviation-manufacturing knowledge. Energy, a sales counterparty, an inventory amount, or the generic phrase "直接材料" MUST NOT by itself become a named input. Facts from before and after the reorganization MUST stay separate. A local true from another chapter MUST NOT fill a missing material-input fact.

#### Scenario: A generic cost label stays generic
- **WHEN** the report states only a generic direct-material amount or an energy, sales, or inventory amount
- **THEN** that row does not become a named material input
- **AND** no material name is added from outside the report

### Requirement: Coverage status stays distinct from an observed fact
`not_disclosed`, `not_applicable`, `unclear`, and `extraction_failed` MUST remain distinct. A readable applicable omission MUST stay `not_disclosed`. An express or structural exclusion MUST stay `not_applicable`. Evidence whose subject, unit, period, or header is not unique MUST stay `unclear`. An unbound page, header, unit, or continuation MUST stay `extraction_failed`. `legal_empty` MAY wrap one of those statuses and MUST NOT replace it or be presented as an observed fact.

#### Scenario: A legal empty does not become a fact
- **WHEN** a checked location has no named material input
- **THEN** the dossier records one coverage status for that location
- **AND** the empty status is not rewritten as an observed material name

### Requirement: Fitness has two outcomes and is not prefilled
The 2.2 review recorded unsuitable after the dossier review, because the report supplies no bindable named material input. Suitable would have allowed only a later, separate four-report material-input successor scope change, and this change MUST NOT implement that successor or run its replay. Unsuitable MUST record the reason and stop. The Stage 4 aggregate MUST remain hold, and no issuer outside the approved list may be added. The review MUST NOT rewrite MI-1 or MI-2, add `302132.SZ` to MI-2, create a four-report successor replay, or reopen the aggregate-gate judgment. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false. Restricted promotion, the six-chapter package, scale quality, and production MUST stay inactive.

#### Scenario: Suitable does not start a replay
- **WHEN** the dossier review finds the report suitable for `extract_material_inputs`
- **THEN** the only permitted follow-up is a separate successor scope change
- **AND** this change still does not enqueue or replay

#### Scenario: Unsuitable leaves the aggregate on hold
- **WHEN** the dossier review finds the report unsuitable
- **THEN** the assessment stops with the reason recorded
- **AND** the Stage 4 aggregate remains hold
- **AND** no issuer outside the approved list is added
