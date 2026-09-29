## ADDED Requirements

### Requirement: The sample stays on the approved reports
The change MUST use only the four approved 2025 reports in manifest `manufacturing_materials.2026-09-03.4`: `300750.SZ` on SZSE, `603659.SH` on SSE, `920015.BJ` on BSE, and `302132.SZ` on SZSE. It MUST NOT add an issuer. It MUST keep three disclosure forms: anonymous rank identities; top-five totals whose name coverage is `not_disclosed`; and named or report-local aggregate identities mixed with a related-party column. Before the 1.1 review passes, the change MUST NOT change Python, enqueue, replay, or fill recall, accuracy, critical numeric errors, or a gate.

#### Scenario: Scope approval does not start extraction
- **WHEN** the 1.1 review has not yet accepted this change
- **THEN** no counterparty implementation, enqueue, or replay starts
- **AND** no metric or gate is written

### Requirement: Relationships and concentration stay separate
A customer Relationship and a supplier Relationship MUST stay separate. An anonymous identity MUST be scoped to one report, one relation type, and one rank or label. The same anonymous label on the customer side and the supplier side MUST NOT be merged. A contract counterparty and a ranked customer MUST stay independent when the text does not state they are the same party, including when both amounts are 58,159,202 thousand yuan. A top-five total and a related-party share of annual sales or purchases MUST be concentration Measurements and MUST NOT create a Relationship. Related-party transaction tables, other-receivable rankings, and credit-risk tables MUST NOT backfill names into this chapter.

#### Scenario: Equal amounts do not merge two customer identities
- **WHEN** one row is anonymous contract party `客户 A(1)` and another row is customer rank `第一名` with the same amount
- **THEN** the two rows remain separate Relationship candidates
- **AND** the amount alone is not treated as identity evidence

#### Scenario: A total is not a counterparty
- **WHEN** a section reports a top-five total amount or share and does not list counterparty rows
- **THEN** the total is a concentration Measurement
- **AND** name coverage is `not_disclosed`
- **AND** no Relationship is created from that total

### Requirement: Coverage statuses stay distinct and output stays isolated
Readable omission of an applicable name MUST be `not_disclosed`. An express or structural exclusion MUST be `not_applicable`. Evidence that cannot uniquely determine subject, unit, period, or header MUST be `unclear`. An unbound page, header, unit, or continuation MUST be `extraction_failed`. A bundle `legal_empty` MAY wrap one of those statuses and MUST NOT replace it. A section whose subject is only `公司` MUST keep `subject_scope` as `unclear`. Research output MUST stay in this change's isolation directory with disposition `accepted_for_review`. The change MUST NOT write production or common-core stores. `production_authorization` MUST remain `not_authorized`, and `scale_quality_claim_allowed` MUST remain false. Publication, closure, mode, and processing identity MUST remain unchanged. The six-chapter package, scale quality, and production MUST stay inactive.

#### Scenario: An inapplicable contract stays empty without erasing concentration
- **WHEN** a report marks major sales or procurement contracts inapplicable and separately reports top-five concentration
- **THEN** the contract item is `not_applicable`
- **AND** the concentration Measurement remains observed
- **AND** missing counterparty names stay `not_disclosed` rather than `extraction_failed`
