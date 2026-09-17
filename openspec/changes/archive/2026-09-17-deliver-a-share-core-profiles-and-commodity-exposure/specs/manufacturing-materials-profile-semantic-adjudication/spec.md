## MODIFIED Requirements

### Requirement: The four blocking semantic families have explicit outcomes
The adjudication MUST separately resolve subject scope, reported business change, external-service processing volume, and same-control comparative semantics. Subject scope for new requests MUST follow report_default_group_scope unless explicit narrower Evidence or a conflict exists; direct wording and reconciliation retain their own basis; a no-major-business-change disclosure MUST produce supported `not_applicable` coverage rather than an invented event; a combined processing/sales source label at one physical anchor MUST produce only the source-supported primary metric; and a restated comparative MUST retain `comparison_basis` without overwriting earlier knowledge-time facts.

#### Scenario: No major business change is disclosed beside a consolidation change
- **WHEN** one Evidence scope states that consolidation scope changed and separately states that products, services, or principal business had no applicable major change
- **THEN** the consolidation change may form its own event and the no-change item forms `not_applicable` coverage
- **AND** the two statements are not merged into one BusinessEvent

#### Scenario: One physical fact uses a processing label with a sales alias
- **WHEN** the source-native name describes one external processing volume and includes a parenthetical sales alias
- **THEN** the accepted result contains one `processing_volume` with the complete source-native name
- **AND** neither extract nor verify requires a second `sales_volume` from that anchor

#### Scenario: A same-control table includes adjusted and pre-adjustment comparatives
- **WHEN** the report presents current, same-control-restated, and pre-adjustment comparative columns
- **THEN** each accepted fact retains its reported period, knowledge time, subject evidence, and comparison basis
- **AND** no later restated fact deletes or overwrites an original-as-published fact
