## ADDED Requirements

### Requirement: PDF parser performance comparisons use identical source bytes
The business-profile parser benchmark SHALL compare concurrency or engine candidates using the same explicit bounded PDF corpus and verified content hashes.

#### Scenario: Concurrency matrix is executed
- **WHEN** an operator benchmarks parser concurrency 4, 6, and 8
- **THEN** every trial SHALL process the same ordered document and content-hash set in an isolated temporary artifact root
- **AND** cache hits from an earlier trial SHALL NOT satisfy extraction work in a later trial

### Requirement: Parser benchmarks are read-only and bounded
The benchmark SHALL perform no discovery, download, production database write, production archive write, queue mutation, or shared-asset state mutation.

#### Scenario: Benchmark receives local cached PDFs
- **WHEN** the benchmark is invoked with explicit local PDF paths or verified manifests
- **THEN** it SHALL enforce document, concurrency, and elapsed bounds and write only temporary or explicitly selected benchmark reports

#### Scenario: Input attempts to use implicit discovery
- **WHEN** no explicit local corpus is provided
- **THEN** the benchmark SHALL fail before any provider or archive activity

### Requirement: Performance evidence includes fidelity and resource metrics
The benchmark SHALL report enough evidence to reject a faster parser that changes governed output semantics.

#### Scenario: Trial completes
- **WHEN** a parser trial completes or partially fails
- **THEN** the report SHALL include wall time, per-document timing, throughput, peak concurrency, parser warnings, page counts, normalized text hashes, page hashes, heading counts, extraction errors, and available process resource metrics

#### Scenario: Candidate output differs from baseline
- **WHEN** a candidate parser changes page count or normalized text/page hashes beyond the configured fidelity policy
- **THEN** the report SHALL mark that candidate ineligible for production rollout regardless of speed

### Requirement: Production parser defaults require benchmark evidence
The business-profile production parser engine or concurrency SHALL NOT change solely from uncontrolled production-batch timing.

#### Scenario: Candidate has no same-corpus evidence
- **WHEN** a proposed parser or concurrency change lacks a completed same-corpus fidelity and throughput report
- **THEN** production defaults SHALL remain unchanged
