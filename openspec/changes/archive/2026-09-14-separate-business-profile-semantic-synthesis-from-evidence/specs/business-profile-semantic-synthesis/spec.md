## ADDED Requirements

### Requirement: Semantic conclusions are independent from source transcription
The system SHALL treat LLM-returned business labels, products, roles, counterparties, scopes, units, and summaries as semantic candidate values and MUST NOT reject them solely because the same string is absent from referenced source text.

#### Scenario: Model normalizes a disclosed business description
- **WHEN** the model returns a schema-valid semantic label supported by referenced annual-report spans but the normalized label is not a verbatim substring
- **THEN** the system accepts the candidate semantic value and preserves the exact source spans independently

#### Scenario: Semantic output violates a structural constraint
- **WHEN** the model returns an unsupported enum, non-finite number, wrong issuer, wrong report period, or malformed response
- **THEN** the system rejects or isolates the affected output with a stable structural reason code

### Requirement: Evidence spans provide provenance rather than lexical approval
The system SHALL resolve every model-referenced span locally to immutable source lineage and SHALL support multiple spans from different selected sections of the same source document without requiring one contiguous quote.

#### Scenario: A conclusion uses multiple annual-report sections
- **WHEN** one semantic conclusion references valid spans from multiple selected sections of the same annual report
- **THEN** the system creates a deterministic composite evidence bundle containing each exact page, section, range, excerpt hash, and span identifier

#### Scenario: A span identifier is not offered in the request
- **WHEN** a response references an unknown, malformed, or duplicate evidence span identifier
- **THEN** the system rejects that row with a stable evidence-provenance reason and does not invent source coordinates

### Requirement: Quantitative candidates preserve governed sanity checks
The system SHALL validate semantic numeric values as finite and schema-bounded, normalize supported units through the governed catalog, and retain unconvertible rows with explicit diagnostics instead of classifying them as missing text context.

#### Scenario: Semantic unit is supported but differs from source typography
- **WHEN** a model returns a governed unit alias or canonical unit that is not a verbatim source substring
- **THEN** the system normalizes the value and unit and retains the exact source spans without lexical rejection

#### Scenario: Semantic unit cannot be normalized
- **WHEN** a semantic quantitative row uses an unsupported unit
- **THEN** the system records `unit_normalization_failed`, preserves the row and evidence diagnostics, and retries or routes the row according to automated machine-rework policy

### Requirement: LLM outcomes are inspectable and persisted
The system SHALL persist bounded, redacted semantic result details for successful and failed work and SHALL emit lifecycle information at INFO and content-level diagnostics at DEBUG.

#### Scenario: LLM semantic extraction succeeds
- **WHEN** a provider returns a schema-valid response
- **THEN** persisted audit metadata and INFO logs identify the model, field family, accepted counts, evidence counts, usage, latency, and hashes, while DEBUG logs expose bounded semantic rows and transformation decisions

#### Scenario: Downstream transformation fails after LLM success
- **WHEN** schema validation succeeds but local normalization or persistence raises an exception
- **THEN** the exception type, stable reason code, bounded message, transformation stage, semantic rows, evidence references, and DEBUG traceback are retained

#### Scenario: Logging runs at production INFO level
- **WHEN** DEBUG logging is disabled
- **THEN** prompts, full source text, structured semantic rows, credentials, and tracebacks are omitted while aggregate progress remains visible

### Requirement: Semantic work resumes without duplicate LLM calls
The system SHALL checkpoint successful field-family outcomes independently, apply consumable budgets to the next field-family request, and prevent a retryable work item from being reclaimed twice by one stage-worker invocation.

#### Scenario: One field family succeeds and another fails
- **WHEN** a company work item is retried after one field family has a completed semantic run
- **THEN** the completed result is reused and only unfinished field families can invoke the LLM

#### Scenario: Retry delay expires during a long batch
- **WHEN** a failed work item's retry timestamp becomes due before the current stage worker exits
- **THEN** that worker invocation excludes the already claimed work ID and leaves it for a later run

#### Scenario: Prior request consumed most of the pipeline token budget
- **WHEN** the next unfinished field family is eligible for processing
- **THEN** its request is evaluated against a fresh bounded field-family budget while cumulative usage remains observable

### Requirement: Equivalent verbatim assumptions are prohibited across business-profile LLM paths
All business-profile LLM adapters SHALL distinguish semantic values from evidence text and MUST apply the same provenance, structural validation, diagnostics, and candidate-only rules to structured segments, operating facts, activities, relationships, value-chain roles, and commodity exposure inputs.

#### Scenario: A business-profile LLM path is changed
- **WHEN** code introduces or modifies a semantic extractor or verifier
- **THEN** tests demonstrate that a valid paraphrase or normalized label is accepted without verbatim matching and that invalid evidence identifiers remain rejected
