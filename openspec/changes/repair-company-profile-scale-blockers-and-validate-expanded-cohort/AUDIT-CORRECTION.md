# Audit correction and fact-level delivery

The preceding closure claim from commit a3dc726 is withdrawn in part.
The batch execution metrics (8 reports, 133 calls, 742 accepted records) remain
recorded by the original batch. The derived source-review, reviewed-readiness,
and empirical-audit v1 files are historical, invalid acceptance evidence.
They MUST NOT be used for semantic precision, defect recurrence, or promotion.

The deleted generator only checked identifiers and pages. It mislabeled those
checks as semantic correctness, producing 42/42 precision without semantic
review. It also hardcoded zero recurrence counts for several failure families.
Absence of a particular reason code is insufficient to prove absence of a bug.
The reported 101/101 source review and five-family zero recurrence are withdrawn.
Task 5.3 and its dependent closure checks remain incomplete.

Under the users request for a usable MVP, delivery now operates at fact level:
the existing shadow application service can export accepted research records,
source-native values, evidence, usage restrictions, and unresolved coverage.
Report hold and review workload do not prevent delivery of already accepted
records. Missing fields are not zero, not disclosed, or not applicable by default.
The export does not change source dispositions or claim a semantic precision.

The partial-data export is a usable local research interface. It is not evidence
that unattended industry-wide ingestion is ready. The next business milestone
is bounded production ingestion with persistence, resume and record quarantine,
measured with real source-text samples rather than another perfect-report gate.
Existing production publication boundaries stay unchanged by this read interface.
