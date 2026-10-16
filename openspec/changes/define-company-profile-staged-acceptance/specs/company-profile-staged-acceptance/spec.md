## ADDED Requirements

### Requirement: Acceptance has finite cohorts and bounded advancement
Future company-profile acceptance MUST follow S0 current two-company closure, S1 twelve historical regression companies, S2 twenty unseen companies, then S3 one hundred additional companies for controlled research use. S2 MUST allow at most one additional distinct twenty-company confirmation after failure, so S2 MUST NOT exceed forty new companies. S3 MUST NOT add a 101st company to dilute failure. Historical development companies MUST NOT substitute for unseen companies. Existing in-flight contracts, results, production permissions and scale claims MUST remain unchanged.

#### Scenario: Second unseen cohort still fails
- **WHEN** both twenty-company S2 cohorts fail their declared thresholds
- **THEN** S2 is failed, additional companies stop and remaining capability limits require an explicit scope decision
- **AND** failed cohorts remain separate retained observations

### Requirement: Cohorts freeze sources strata and one first-delivery version
Before observation, each cohort MUST freeze company ids, original report versions, cutoff, code, sampling seed, primary conditions and run budgets. S2 MUST use six manufacturing, six nonfinancial service, four financial and four other/mixed companies, with at least six SSE, six SZSE, two BSE companies and eight first-level industries; financial coverage MUST include banking, insurance and securities. S3 MUST use thirty manufacturing, thirty nonfinancial service, twenty financial and twenty other/mixed companies, with at least thirty SSE, thirty SZSE, ten BSE companies and fifteen first-level industries. Classification MUST precede outputs and be mutually exclusive for quotas. Missing assets, timeouts and failed deliveries after freeze MUST remain in the company denominator. A frozen first-delivery cohort MUST NOT tune code after inspecting individual outputs or splice best results from different versions.

#### Scenario: A frozen company fails to publish
- **WHEN** a selected company's official asset becomes unreadable or its semantic stage fails
- **THEN** the company remains in the fixed denominator with its failure reason and zero delivered recall
- **AND** no easier replacement is drawn after observation

### Requirement: Deliveries distinguish usable limited and isolated results
A usable company MUST have three substantive evidenced business answers, established principal business and revenue mechanisms, at least ninety percent reviewed core recall and ninety-eight percent reviewed accuracy, and no unhandled critical finding in the reviewed scope. A limited result MUST expose only independently reliable facts and explicit gaps; a missing answer MUST keep core_complete false. An isolated company MUST NOT supply a certain core conclusion. Critical subject, period, scope, unit, important-number, blank-cell and current-versus-planned errors MUST be corrected or quarantined together with dependent answers before research consumption. Original first-delivery mistakes MUST remain in the raw scores even after quarantine.

#### Scenario: A rental amount cannot be resolved
- **WHEN** the original source has a material unresolvable rental-number conflict
- **THEN** that value and dependent certainty are withheld with the source, attempted remedy and restriction
- **AND** unrelated supported business facts remain available without inventing the value

### Requirement: Review prioritizes core evidence and states sampling limits
All substantive answers, core supporting facts and critical-risk objects MUST receive source review. Core contracts MUST include all major businesses and revenue mechanisms; a comparable segment at least five percent of group revenue or explicitly described as principal MUST NOT be omitted. Comparable published composition MUST cover at least ninety percent of business revenue without summing incompatible scopes or inventing missing disclosure. Supplemental actual outputs MUST be sampled using the frozen seed with at least twenty unique objects per company, or all when fewer exist, plus every identified suspicious object. Accuracy MUST count reviewed unique objects only, disclose core and supplemental groups and actual sample size, and MUST NOT label unreviewed output accurate. Each company's source recall MUST include all frozen core conditions; failed delivery MUST count zero. Cohort recall MUST use the arithmetic mean of company recall rates and show per-company and per-stratum results.

#### Scenario: Many small rows could hide a missing core
- **WHEN** one company has many correct supplemental rows and another lacks its main-business answer
- **THEN** the missing company does not become usable and the cohort macro recall retains that company's failure
- **AND** the supplemental sample cannot be described as all-output verification

### Requirement: Promotion balances first-delivery quality and bounded remediation
S2 MUST require at least eighteen of twenty first-delivery usable companies and nineteen after remediation; S3 MUST require at least ninety of one hundred initially and ninety-five after remediation. Each cohort MUST achieve first-delivery reviewed unique accuracy at least ninety-nine percent and core macro recall at least ninety-five percent. Each of the four S2 primary strata MUST have at most one initially unusable company; each S3 primary stratum MUST achieve initial usability at least eighty percent and core recall at least ninety percent. Tiny industry subgroups MUST NOT introduce an accidental all-companies-must-pass rule. Final verification MUST retain the applicable accuracy, recall and stratum floors, zero known uncontained critical errors and consistency of actual query, export and disk deliveries. All applicable negatives on delivered usable/limited companies MUST be actually evaluated and passed; isolated unpublished companies MUST remain unassessable for such negatives and consume the unresolved-company allowance, never count as negative passes. Missing mandatory cohort assessments MUST NOT count as passed. Remediation MUST NOT overwrite initial results or rescue an initial accuracy/recall threshold failure as a blind first-delivery pass.

#### Scenario: Correction improves a weak initial score
- **WHEN** a twenty-company cohort initially has ninety-seven percent reviewed accuracy but reaches one hundred percent after fixes
- **THEN** initial S2 quality remains failed and the corrected score is reported separately
- **AND** only the one permitted new twenty-company confirmation can establish subsequent first-delivery readiness

### Requirement: Remediation and unresolved exceptions have limits
Each company MUST allow at most one extra business retry and thirty minutes of extra active human remediation; a cohort MUST allow at most two common-rule repair iterations without bypassing the per-company retry limit. At most two S2 companies and ten S3 companies MUST need human remediation. Routine independent review, waiting and machine time MUST be recorded separately from remediation. Unresolved companies within the permitted final one-of-twenty or five-of-one-hundred limit MUST retain limited or isolated status and document field, source/version, cause, attempted remedy, affected judgments and recovery conditions. Manual replacement answers MUST NOT count as automatic usable delivery. Exceeding attempts, unsafe dirty-file overlap or changed scope MUST go to A before dependent execution.

#### Scenario: Per-company remediation is exhausted
- **WHEN** a company remains unresolved after its extra retry or thirty-minute active remediation limit
- **THEN** B records the limitation and stops retrying that company
- **AND** the cohort can pass only if all original and final thresholds, including the permitted unresolved-company count, still hold

### Requirement: Progress and execution authority remain explicit
The single progress table in docs/development/company_profile_acceptance_program.md MUST track planned and observed counts, initial and remediated usability, reviewed precision, macro recall, uncontained critical findings, attempts, blockers and next checkpoints. S2 MUST checkpoint every five companies and S3 every twenty. B MUST use the existing application/writer owners and A/B workflow v1. Future execution cards MUST separately bind actual cohorts, code, per-slice and whole-stage budgets before starting; each single/two-company slice MUST remain bounded by three hundred continuous seconds and fifty thousand shared tokens unless explicitly changed. This planning change MUST NOT launch cohorts, enable production or claim market-wide quality.

#### Scenario: A planning document exists without executed observations
- **WHEN** this plan is committed but S2 has not run
- **THEN** progress remains zero of twenty and quality metrics remain unassessed
- **AND** no company-analysis count, task checkbox or prior repaired score is substituted for cohort progress
