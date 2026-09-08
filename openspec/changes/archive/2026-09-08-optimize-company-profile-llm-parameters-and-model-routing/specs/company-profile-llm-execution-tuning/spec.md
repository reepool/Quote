## ADDED Requirements

### Requirement: Model comparison uses identical annual-report requests
The comparison MUST send the same frozen Evidence, request scope, compact schema, temperature, output budget, and deadline to each concrete Grok, GLM, and Gemini profile. It MUST record structured parse success, repair use, latency, provider-reported token usage, candidate shape, Evidence binding, and source support without writing production data.

#### Scenario: Three concrete profiles are compared
- **WHEN** a frozen narrative or table-heavy Stage 5 request is evaluated
- **THEN** each enabled concrete model receives semantically identical input
- **AND** the result attributes every observation to that concrete profile

### Requirement: Stage 5 uses measured bounded defaults
The Stage 5 operator MUST provide bounded defaults for primary model, extract/repair output tokens, verify output tokens, request deadline, and provider-call budget. Explicit positive overrides MUST remain available, and a complete run MUST NOT require an outer timeout shorter than its internal bounded workflow.

#### Scenario: Operator runs without manual tuning
- **WHEN** the Stage 5 semantic operator is invoked with its default execution parameters
- **THEN** it uses the measured primary model and safe bounded token/deadline values
- **AND** extract, repair, and verify remain subject to the existing bounded workflow

### Requirement: Primary model selection prioritizes usable structure
The primary annual-report model MUST be chosen by repeated local structured-validation success first, source-supported semantic output second, and latency/token volume third. A model with intermittent parse/repair failure MUST NOT be selected solely because its successful calls are faster.

#### Scenario: Fast and reliable results differ
- **WHEN** one model is fastest but another completes all comparable structured requests reliably
- **THEN** the reliable model is selected as primary unless its semantic output is unsupported

### Requirement: Optimized validation remains research-only
One new BaoSteel validation MAY run after parameter tests pass. Its output MUST remain isolated, `accepted_for_review`, and `production_authorization=not_authorized`; it MUST NOT start Stage 6 or write approved, commodity, value-chain, DCF, scheduler, API, or Telegram paths.

#### Scenario: Optimized run completes or fails
- **WHEN** the new bounded run reaches an atomic bundle or typed failure
- **THEN** the result is retained honestly without historical splicing or production authorization
