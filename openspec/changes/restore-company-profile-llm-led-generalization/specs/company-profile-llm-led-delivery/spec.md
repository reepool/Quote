Current policy: product v9 (2026-11-11), acceptance section 7.109 and company-profile-chapter-workpaper-card.md govern new work. The sourced manual reference and original-owner extraction path have been delivered; the user's continued-development authorization now permits the bounded report observations in that card, including optional supply-chain enrichment and actual-model attribution. Use each report's TOC/headings to build concentrated complete section contexts, extract one versioned eight-element JSON projection and render it through existing owners. Source-first means original-text-grounded discovery and independent review, not a mandatory inventory/claims/checks call sequence before a draft can exist. Historical named policies, stages and execution scenarios below apply only to their frozen identities; none revive exhausted previews or override the v9 workpaper requirements. Formal trusted release, historical scores and production boundaries are unchanged; updating a target spec does not mean implementation is complete.

## ADDED Requirements

### Requirement: The existing job command accepts an explicit bounded workpaper plan
The CLI and scheduled-task adapter MUST forward an explicitly supplied execution plan file to the company-profile application adapter. The adapter MUST reuse the existing configuration, gateway, budget, workpaper identity and published task owner for first_read, review_batch or enrich. It MUST require explicit authorized scope, stock/report versions, configured profile identities and finite limits before model construction; it MUST reject unknown plan fields, profile drift and out-of-scope report requests. Constructed model dependencies MUST remain local to this explicit invocation, without changing shared configuration, default routes or production schedules. It MUST NOT implement another extraction/review loop.

#### Scenario: A new-version ordinary first read recovers an eligible structural rejection
- **WHEN** an explicitly versioned workpaper first-read path receives a rejected extraction with finish_reason=stop and the exact raw response satisfies the existing strict terminal-object-brace/known-field-relocation normalization
- **THEN** it MUST reuse that normalization locally before another paid format retry and continue the existing typed/source/numeric checks, writer and graded research delivery without completing strings, arrays, missing elements or business values
- **AND** the original raw response, rejected contract status, author/model and actual usage MUST remain intact alongside normalization operations and before/after hashes; successful normalization MUST NOT be represented as native model acceptance or semantic review
- **AND** the ordinary inline reviewer and deferred saved-plan reviewer MUST use their existing owners; unknown anchors, wrong versions, ambiguous keys, nonfinite values and truncated outputs retain their rejection/isolation rules, and independent restrictions cannot be silently removed
- **AND** historical identities and explicit local-only plans retain their original behavior; the new behavior MUST NOT relax global gateway parsing, reopen exhausted budgets or mutate saved research releases

#### Scenario: A local saved-content plan uses the existing job without model construction
- **WHEN** a caller explicitly authorizes a new local-only execution file for one saved rejected workpaper response, its accepted original selection, matching report/PDF version and original model attribution
- **THEN** the adapter MUST dispatch the existing run/TaskService/runtime/writer path without constructing LLM clients, requiring model credentials, creating a new supplier budget or implicitly performing source review
- **AND** only the existing strict local structural normalization may be used; original raw rejection/usage stay unchanged, hash/source/type/number checks and local quarantine still apply, and useful content is delivered as unreviewed with normalization provenance
- **AND** run_id reuse, publication-stop and mismatched-input rejection MUST remain effective; paid first_read/review_batch/enrich plans retain their authorization/limits and serialization fingerprints without inheriting a free local bypass
- **AND** a later source review requires its own explicit bounded authority and the existing review owner; pending local recovery does not alter historical scores, unrelated profiles or production authorization

#### Scenario: A saved API gap plan is executed through the daily job
- **WHEN** a caller supplies an explicitly authorized enrich execution file containing exactly one stock and its existing report/hash-bound enrichment plan
- **THEN** the adapter MUST reject mismatched stock/version or a missing plan, and forward the original concrete questions to TaskService.enrich for its existing lineage/hash checks, targeted selection/extraction and source review
- **AND** the original writer MUST retain the first reading and append the supplementary reading, with content, remaining gaps and actual review state available through the existing API/export; repeated run_id submissions MUST NOT issue new model requests
- **AND** first_read and review_batch files MUST reject enrichment plans, while later explicitly requested readings and review recovery continue through their existing owners without automatic gap-filling

#### Scenario: An operator submits a bounded first-read or saved-review command
- **WHEN** an authorized caller supplies a valid plan through the existing job command
- **THEN** its finite parameters and dependencies reach the original published task action and the existing writer
- **AND** resubmitting its run_id returns the recorded result without constructing another client or budget; changed parameters or an unfinished execution cannot reopen that ID with fresh limits
- **AND** status, pause, query and export can use the same file to locate saved data without model construction; missing plans for new paid actions are rejected and legacy defaults remain compatible

### Requirement: The published application entry dispatches both workpaper phases
The existing published task application entry and TaskService execute method MUST expose first_read and review_batch with explicitly injected providers, identities, storage and bounded execution budgets. first_read MUST require an explicit nonempty stock list, use the existing queue/runtime/writer with deferred review, drain only its own enqueued work, and avoid loading a full-market registry. It MUST retain publication controls and frozen-plan constraints, reject active first-expansion mode and restore the original service identity afterwards. review_batch MUST require explicit saved plans and reject any separately supplied stock scope that differs from those plans before dispatching to the existing review-batch owner. Existing run/resume behavior MUST remain compatible. These actions MUST NOT create a generic or implicit model factory, parallel business loop, implicit paid authorization or production permission. An explicit job-plan adapter MAY construct only its specifically authorized configured dependencies as described above.

#### Scenario: One caller submits first reads and later submits saved review plans
- **WHEN** an authorized internal caller invokes first_read with explicit stocks and dependencies, saves the resulting pending-review plans, then invokes review_batch through a new application-entry instance
- **THEN** the first action issues only selection/extraction requests and delivers source-bound pending drafts, while the second consumes the saved plans without repeating extraction
- **AND** resubmitting the same first-read identity or processed review request does not resend completed work; invalid scope, exhausted budgets or stopped publication cannot initiate model sends

### Requirement: Explicit stock lists return complete graded research profiles without execution
The existing read owner MUST support a bounded explicit stock list and reuse the same annual-profile projection as single-stock detail reads. The HTTP query MUST retain local source/model identities, review continuations, gaps and fact-level restrictions, deduplicate canonical stock IDs in request order, and separately identify stocks without a saved annual workpaper. Missing records MUST NOT be described as undisclosed business, zero exposure or a diagnosed execution failure. This query MUST NOT call models, enqueue work, write files, silently select all stocks or grant trusted/production status. Existing single-stock, catalog and export contracts remain compatible.

#### Scenario: A requested list mixes saved profiles and absent stocks
- **WHEN** a caller supplies between one and one hundred stocks, including aliases and a stock without a saved annual workpaper
- **THEN** the response contains each found stock once in request order with the same complete content as its single-stock API and a separate not-found entry for each absent stock
- **AND** hidden/withheld facts remain hidden, same-report successor chains stay intact, all-absent lists return a readable not-found result, and invalid empty/over-limit lists are rejected before reading

### Requirement: Deferred source review does not block first-read research delivery
An explicitly deferred-review workpaper MUST use the existing task/runtime/writer path to deliver its source-bound extraction without issuing review, repair or final-check requests. The deferred policy MUST be part of processing identity, preserve the default inline mode, and expose not-reviewed/deferred scope through the existing API and export. Useful missing or uncertain supplementary detail MUST NOT reject the draft; deferred content MUST NOT be counted as formally reviewed, accurate or production-approved.

#### Scenario: A batch completes first reads while the reviewer is unavailable
- **WHEN** the caller explicitly enables deferred review for bounded first-read tasks
- **THEN** each valid extraction remains readable with its gaps and a bound pending-review plan, without contacting the review channel
- **AND** later review failures do not remove these drafts or stop a separately executed first-read task; known wrong facts and independent restrictions remain isolated

#### Scenario: Saved review plans are consumed as one bounded work list
- **WHEN** the original TaskService executes an explicit finite list of saved per-stock review plans
- **THEN** it reuses the existing per-plan recovery path, original report/source/hash checks, persisted request idempotency and finite execution budget
- **AND** two adjacent newly executed reviews for distinct companies with the same channel and execution cause pause only remaining review dispatch, retain processed results, and report every unattempted plan as deferred
- **AND** resubmitting the same list after restart does not resend processed requests or count their old failures as fresh consecutive failures; blocked plans and completed open findings do not trigger automatic repair loops, new request IDs, budgets or production approval

### Requirement: Acceptance separates basic business answers from supplementary acquisition and resource policy
New batch acceptance contracts MUST separate substantive principal-business, main-product/service and earning-mechanism answers from recommended supplementary detail. Missing detailed models, uses, subsidiary inventories, parallel income labels or fine-grained supply-chain information MUST NOT alone reject an otherwise supported basic research profile. Whole major business lines and known false assertions remain subject to the existing correctness requirements. Existing frozen conditions and results MUST NOT be rescored by removing inconvenient items. A current research-delivery assessment MUST NOT be reported as a same-version first-run success, a new source audit or measured accuracy.

#### Scenario: A and B deliver an integrated research release
- **WHEN** the user authorizes research-release closure, bounded real daily use, an acceptance contract and an expansion decision as one work package
- **THEN** A MUST freeze the business scope and cumulative resources, independently review the semantic evidence, and make the expansion decision while B implements and executes the existing daily task owner through the declared entry
- **AND** necessary in-scope fixes and direct verification MUST continue within that work package without per-field or per-phase approval pauses; successful work MUST NOT be resent or cumulative budgets duplicated across first_read, review_batch and enrich files
- **AND** technical delivery, substantive basic answers, review execution, independently audited content, missing information and resources MUST be reported separately; a useful graded research release MUST NOT be relabeled as a passed historical S2 or unauthorized unseen/production expansion

#### Scenario: An explicitly authorized prospective batch follows a closed historical scheme
- **WHEN** the user explicitly authorizes a new bounded twenty-company evaluation of the delivered two-phase workflow after the historical scheme has ended
- **THEN** the new batch MUST have its own fixed unseen eligibility, source-first independent references, code/model/source identity, cumulative resources and stop conditions, without rewriting or pooling historical failures
- **AND** source references MUST be frozen before that company's first semantic call and MUST NOT be model input; first_read, source-review completion, actual audited accuracy, remaining gaps and resources MUST be reported separately while failures stay in the fixed company denominator
- **AND** the applicable eighteen-of-twenty initial, nineteen-of-twenty remedied, ninety-five-percent core macro recall, ninety-nine-percent audited unique-object accuracy and known-error isolation requirements MUST remain explicit; missing optional supply-chain or detailed fields MUST NOT introduce new company rejection gates
- **AND** one bounded package MUST include implementation of real blockers, direct checks, any authorized in-budget continuation, persisted API/readable delivery, independent assessment, cleanup and a terminal expansion decision; it MUST NOT implicitly authorize a twenty-first company, another confirmation group, one hundred companies or production

#### Scenario: Existing profiles contain useful basic answers and optional omissions
- **WHEN** existing real profiles have source-linked substantive business answers but retain optional acquisition gaps
- **THEN** the current research data remains readable with those gaps and limitations
- **AND** formal batch, audit and resource requirements are assessed separately without inventing a formal stage pass

#### Scenario: High-tier review changes the applicable resource envelope
- **WHEN** a proposed extraction-and-review batch needs a different resource profile from the frozen ordinary contract
- **THEN** the proposal records both profiles, the requested prospective applicability and unknown costs before any new calls
- **AND** only explicit authorization can activate that batch's new resource contract; historical results, ordinary-cost claims and production permission remain unchanged

### Requirement: Source-backed uncertainty does not require deterministic closure
New workpapers MUST retain source qualifications such as approximate values, bounds, differing units and unclear scope without forcing exact normalization. A metric MAY carry a null normalized value with its original spelling, source reference and uncertainty reason. Such a metric MUST NOT enter exact arithmetic. Facts with unclear scope MAY retain sourced text and an explicit uncertainty reason. Rendering MUST expose the qualification and original source, and MUST NOT label unknown scope as absence of disclosure. Correctly qualified uncertainty alone MUST NOT trigger repair or block the remaining profile or review workflow. Known false assertions and invalid evidence MUST remain withdrawn or rejected, not laundered by an uncertainty label. Historical results MUST remain unchanged.

#### Scenario: A retained review predates display-only qualifications
- **WHEN** a hash-bound review continuation reuses its exact original draft and stage receipts under newer source checks
- **THEN** every original fact and source span is revalidated, but display-only qualification or normalization notes MUST NOT rewrite that retained review input
- **AND** invalid source content fails before sending, original receipt hashes remain enforced, and final review is still required for newly recoverable repair candidates

#### Scenario: A metric cannot safely be reduced to one number and unit
- **WHEN** a selected source supports the quoted description but its magnitude, unit or scope cannot safely be normalized
- **THEN** JSON and the readable profile retain the original wording, evidence and uncertainty reason with a null normalized value
- **AND** useful independent business content remains available without a paid clarification loop

#### Scenario: PDF layout inserts spaces inside a source number
- **WHEN** a source number has same-line layout spaces immediately after its decimal point or a complete thousands-group comma
- **THEN** new source binding MAY recognize that complete numeric token while retaining its exact source range, original Evidence and model raw value
- **AND** it MUST NOT join separate integer columns, cross lines, change digits/sign/unit or treat a numeric match as proof of the metric's column, subject or period

#### Scenario: A monetary table expresses a loss with parentheses
- **WHEN** a source monetary table or an explicit financial-account row with multiple numeric columns contains a standalone parenthesized grouped or decimal amount
- **THEN** new binding recognizes its negative accounting value and the governing literal unit, not both positive and negative alternatives
- **AND** a continuation row without a unit header supplies no missing unit; ordinary note markers or parentheses without accounting context do not supply a negative amount; historical files and model-review decisions remain unchanged, and a locally recovered repair still requires its own real source recheck before adoption

#### Scenario: A qualified estimate is not a review defect
- **WHEN** the draft preserves a source estimate or boundary with its original qualification and no unsupported conclusion
- **THEN** review may complete with that content still uncertain and need not request a deterministic replacement
- **AND** a wrong amount or unsupported company attribution is not approved merely because the draft adds an uncertainty label

#### Scenario: A returned rate change preserves literal directional wording
- **WHEN** a metric preserves literal 增加/减少 followed by a decimal amount and 个百分点, and its same-unit signed reference agrees with that direction
- **THEN** local projection MAY retain that literal raw value as an explicitly unnormalized value with a reason instead of rejecting the entire response
- **AND** a conflicting sign, amount or unit remains invalid; source binding still locates the complete literal wording, and a repaired candidate still requires real source recheck before adoption

#### Scenario: A reported business volume exceeds a stated boundary
- **WHEN** a metric preserves a source phrase such as 突破2.8万亿元 and declares a matching greater-than qualifier
- **THEN** the consumer MUST retain that lower-bound meaning and original spelling, not reject it merely because the source uses 突破 rather than 超
- **AND** a conflicting exact qualifier, changed decimal value, magnitude or unit remains invalid

#### Scenario: A printed percent header governs a literal margin-point change
- **WHEN** the cited table prints its margin-change header as % but the returned raw cell explicitly says 增加/减少X个百分点, with a consistent signed reference and unit 百分点
- **THEN** the consumer MAY retain source_unit=% beside that original wording and a null normalized value with its uncertainty reason
- **AND** a changed sign, number or incompatible unit remains invalid; local parsing alone never accepts a repaired fact without its authorized source recheck

#### Scenario: One returned metric cannot be located in its cited source
- **WHEN** an otherwise schema-valid extraction contains one fact whose metric number or unnormalized raw wording is not present in the supplied evidence
- **THEN** binding MUST withhold that fact, record its identity and concrete limitation, and retain independently valid facts in the same workpaper
- **AND** direct workpaper validation still rejects invalid retained facts, unknown source identities remain rejected, and this local isolation does not authorize a new paid entry or rewrite the original failed run
- **AND** local isolation identities and reasons MUST survive a later review that withdraws disputed scope prose; they remain program processing notes rather than being erased with model-authored limitations

#### Scenario: An explicit period change spells the negative direction in words
- **WHEN** a cited prose clause explicitly reports 同比 or 环比 followed by 下降/减少/降低, a magnitude and its percent or percentage-point unit
- **THEN** numeric source binding MAY recognize the same-unit negative signed change without requiring a printed minus sign, preserving the original source and returned raw value
- **AND** this does not turn a decline into a negative absolute balance, accept a changed magnitude or unit, infer subject/period/negation/plan semantics, or approve a fact without source review; the binding policy is included in new processing identities and historical results remain unchanged

#### Scenario: Supplementary container formatting is invalid
- **WHEN** the extraction includes unknown keys beside the eight required elements or a malformed optional supply-chain container
- **THEN** the wire contract MUST permit retaining that original response, and the application adapter MUST withhold the unknown/invalid portion with an explicit limitation before typed binding
- **AND** required business fields, evidence identities and metric validation remain enforced; neither discarded content nor a preserved raw response counts as a supported conclusion

#### Scenario: A returned fact has an invalid metric representation
- **WHEN** a schema-valid draft contains a fact that fails local validation only within its metrics, such as an inconsistent raw qualifier or a null normalized value without the required reason
- **THEN** the extraction adapter MUST withhold that complete fact and retain independent valid facts, its original raw response, affected fact ID and concrete validation reason
- **AND** the adapter MUST NOT guess a missing qualifier, value, unit or source explanation; a literal approximate/bounded raw value with null MAY carry an explicit program-only non-normalization note rather than an invented source reason, while contradictory qualifiers, non-metric/source identity errors and unsourced original wording remain invalid, and program isolation notes survive later review scope changes

### Requirement: Metric standardization preserves source meaning and audit history
New workpaper binding and repaired projections MUST clear currency on dimensionless ratios. A rate difference expressed as percentage points MUST remain distinct from relative percentage growth. Standardization MAY retain a source_unit beside the normalized unit, MUST preserve the raw source spelling and decimal value, and MUST leave an auditable note. It MUST NOT guess currency conversions, magnitudes, subject scope or rate-change meaning from a bare number. Historical workpaper files MUST NOT be rewritten by reading them.

#### Scenario: Prose displays a source-backed amount in a different monetary magnitude
- **WHEN** the same fact has an original-valued metric already validated against its cited source, with explicit monetary units and matching period, and prose uses an equivalent magnitude or decimal-rounded display
- **THEN** local binding MAY retain that prose with a program-generated display qualification while preserving the exact original metric, raw value, unit and evidence
- **AND** no missing amount, sum, account ownership, currency, scope or period is inferred; wrong sign/value/magnitude, unsupported source units or absent corresponding metrics remain invalid, and rounded display is not a new exact normalized fact
- **AND** each metric retains its own source-backed subject; a compound fact's enclosing group subject MUST NOT force its business-segment metrics to inherit that subject or prevent a valid display conversion, and this arithmetic check does not establish semantic ownership

#### Scenario: An aligned table continuation carries an explicit negative point change
- **WHEN** a validated same-row continuation spells 减少X个百分点 and a returned metric retains the same negative value with a percentage-point unit
- **THEN** numeric source binding MAY recognize that signed rate without requiring a printed minus sign
- **AND** it MUST NOT supply a negative monetary balance, a percent-growth unit, a changed amount or a rate borrowed from an ambiguous continuation

#### Scenario: Comparative financial columns split only their decimal suffixes
- **WHEN** an explicitly unit-labelled monetary table has comparative year/period columns and at least two uniquely aligned same-row decimal continuations
- **THEN** source binding MAY restore their two-place decimal cells without changing the source quote or coordinates
- **AND** unknown units, cross-column or ambiguous alignment, changed digits, integer-fragment regrouping and arbitrary whitespace joins remain unsupported; local success does not adopt a repair without its real final review

#### Scenario: Prose expresses the magnitude of a signed comparative decline
- **WHEN** a cited table explicitly labels its comparative rate column and reports a negative percent, while prose explicitly states 同比/环比下降 with the same positive magnitude and percent unit
- **THEN** source binding MAY retain that positive decline-magnitude metric without changing the signed source or original returned values
- **AND** growth wording, absent direction/header, changed magnitude and absolute monetary balances MUST NOT use this equivalence

#### Scenario: A monetary table wraps one cell over an explicit continuation row
- **WHEN** a cited fixed-layout monetary table has a unique aligned numeric continuation and a matching wrapped percentage-point cell identifying the same continued row
- **THEN** source validation MAY join only those same-column cell fragments without changing the immutable quote or coordinates
- **AND** it MUST reject cross-column concatenation, changed digits and ambiguous or unmarked row joins; this is not permission to remove arbitrary whitespace or combine figures from different rows/pages

#### Scenario: An approximate null has no model-authored uncertainty explanation
- **WHEN** an original metric explicitly retains 近/约/超/突破 and null but omits its explanation
- **THEN** the local representation MAY add a program-only note that the original qualification was retained and no exact value was normalized, with the raw response and source wording still verified and unchanged
- **AND** it MUST NOT fabricate a numeric value or source explanation, override a contradictory qualifier, or treat local decoding as a confirmed final review

#### Scenario: A literal percentage-point suffix repeats the declared unit
- **WHEN** a returned raw value ends in 个百分点 while its declared unit is 百分点 and its same-unit decimal reference agrees
- **THEN** the consumer preserves the original spelling and accepts that equivalent suffix without leaving 个 in the decimal parser
- **AND** a changed sign, numeric value or incompatible unit remains invalid; local repair decoding is not final source verification

#### Scenario: A new binding changes an explicit monetary table unit
- **WHEN** the supplied cited amount has one unambiguous explicit governing monetary unit but the returned monetary unit differs
- **THEN** new binding or repair validation withholds the affected fact with its concrete source-unit limitation, preserving other business facts and the original response
- **AND** it does not select a different unit by magnitude or turn source ambiguity into an exact amount; historical JSON remains readable without retroactive mutation, and faithful uncertain source-only values remain allowed

#### Scenario: A bank reports approximate or lower-bound business volumes
- **WHEN** the source and returned raw metric use 近/约/超 with a decimal reference value
- **THEN** the workpaper MUST retain the source spelling and an explicit approximate or greater-than qualifier, not silently assert an exact value or discard the whole company
- **AND** a contradictory qualifier or changed magnitude MUST remain invalid

#### Scenario: A gross-margin difference is printed with a percent sign
- **WHEN** a sourced margin-change metric and its governing comparison context explicitly support percentage-point meaning
- **THEN** its normalized unit may be percentage points while its original percent spelling/unit and value remain available
- **AND** ordinary percentage growth is not converted, and no ratio carries a currency

### Requirement: Bounded source review and local correction preserve useful workpapers
An explicitly review-enabled workpaper MUST reuse the current gateway and writer to review the original selected source packet together with its draft. Extraction and review MAY use different existing profiles or requested reasoning levels; actual successful model/source/profile, requested reasoning effort and usage MUST be recorded for review, repair and final verification. A stronger name or model agreement MUST NOT establish factual truth or production approval. Legacy unreviewed workpapers MUST remain readable without implicit calls.

The reviewer MUST identify checked fact IDs/elements and source-cited errors, ambiguity and material omissions within the supplied scope. Unknown references MUST NOT remove or approve facts. Validly flagged erroneous/ambiguous facts MUST leave the ordinary narrative while remaining auditable. Independent useful facts and all original sources MUST be retained. Optional omissions MUST NOT require repair. At most one combined local repair and one source-grounded verification of its proposed changes are allowed; repairs MUST preserve unrelated facts, report/source identity, exact numeric constraints and original responses. Only explicitly resolved issues with actually checked repaired objects may be applied. Failure, insufficient coverage, unresolved or newly found issues MUST remain visible and limited, never silently clear gaps. Review status MUST distinguish model-reviewed scope from trusted release and full-report completeness.

#### Scenario: Clients plan recovery of a saved review execution failure
- **WHEN** an API client inspects or explicitly plans follow-up for a persisted reading
- **THEN** the original review/read owners MUST distinguish pending initial review, pending repair, pending final recheck, completed review with ordinary open findings, and failures requiring source/contract inspection
- **AND** only complete accepted stage receipts matching the stored review-call identity, JSON payload, response hash and typed contract may be listed for reuse; absent or inconsistent history MUST remain blocked rather than silently repeating completed stages
- **AND** a stock-specific plan MUST bind the exact same-report reading work ID and current workpaper hash, expose only stage/request/candidate identities and safe reasons, and preserve existing facts, uncertainty, local restrictions and old failures without returning raw rejected or unadopted candidate content
- **AND** GET and planning MUST NOT queue, call models or claim original-draft/source binding, final acceptance, finite-budget authorization or production permission; execution still requires the existing TaskService/seed/resume validation and explicitly authorized bounded calls
- **AND** optional omissions and completed reviews with open findings MUST NOT by themselves trigger another review or repair

#### Scenario: An explicitly bounded internal task executes a saved review plan
- **WHEN** an authorized internal caller supplies the exact current reading/report/hash plan and a finite provider budget to the original TaskService
- **THEN** the owner MUST reconstruct and validate the original seed and retained stage receipts from local saved data, run only the unfinished review stages through the existing queue/seed/resume path, and reject stale/cross-report/blocked plans before any model send
- **AND** the same request identity MUST NOT resend completed work; recovery writes a distinct report/hash-bound successor, preserves original failures and source/model attribution, and does not expose a restored unreviewed original before the recovery review returns
- **AND** API and combined exports MUST preserve predecessor metadata while marking superseded facts/gaps explicitly so unchanged content is not counted twice as newly extracted facts
- **AND** unchanged independently source-checked facts may inherit their existing decision, restrictive source-only/withheld/duplicate decisions MUST NOT be cleared by model review, and changed or new facts MUST NOT inherit independent approval from a different content hash
- **AND** planning/GET remain read-only, no new public paid executor or parallel fact store is created, and historical stage results, finite-call authority and production restrictions remain separate

#### Scenario: A final verification execution failure follows an initial review failure in a bounded batch
- **WHEN** an explicitly enabled consecutive-same-execution-failure batch policy observes adjacent companies in actual processing order failing on the same review channel and gateway cause, whether at initial review or final verification
- **THEN** the original task owner stops subsequent dispatch after retaining both available drafts, without interpreting incomplete final verification as a successful execution or reviving unverified edits
- **AND** actual failure stage and safe gateway cause/channel remain machine-readable, requested profiles are not misreported as actual selected models, ordinary missing information or open findings do not trigger this execution-failure rule, legacy records keep their original serialized shape, and no additional retry, model call or authorization is created

#### Scenario: A partial review contains independent valid findings
- **WHEN** a structurally valid review lists missing, duplicate or unknown checked IDs, or one finding has invalid references while other findings have unique known targets and sources
- **THEN** the owner MUST retain the known unique checked scope and independent valid findings, isolate invalid findings with explicit reasons, and keep the review incomplete
- **AND** legitimate errors may use the existing single bounded repair/recheck without clearing unreviewed scope; unknown or ambiguous references MUST NOT remove or approve any fact, and original responses remain unchanged

#### Scenario: An omission action is attached to an existing fact
- **WHEN** a valid possible_omission finding refers to an existing correct fact, including an issue_type other than material_omission
- **THEN** the existing fact remains unchanged and the finding becomes an explicitly related supplement with a separate target ID
- **AND** severity still controls whether the supplement warrants the one bounded repair, and a separate valid error finding still withdraws the erroneous original

#### Scenario: A valid unique fact ID has a mismatched element hint
- **WHEN** a finding cites an existing unique draft fact and valid source IDs but supplies a different known element hint
- **THEN** local consumption binds that ID to its actual draft element and records the original-to-bound mapping without rewriting the raw review receipt
- **AND** a pure possible_omission/material_omission attached to that existing ID becomes a separately identified supplement with an explicit related-original ID, not a withdrawal or replacement of the correct original; the supplement still requires confirmed source recheck, while unknown IDs/elements/evidence remain invalid

#### Scenario: A supplement cannot complete but the original fact is correct
- **WHEN** an initial review requests additional material context without flagging the existing fact itself as wrong, and the supplement is absent, rejected or unverified
- **THEN** that original fact remains unchanged and readable, the omitted context remains unresolved, and no supplement is adopted
- **AND** a separate correct/quarantine finding still withdraws the erroneous original; an omission label alone cannot override that finding

#### Scenario: A review identifies a scope limitation without a fact ID
- **WHEN** an otherwise valid review flags limitations or an element-level ambiguity without an affected fact
- **THEN** that finding MUST remain unresolved and visible without discarding independent valid findings or inventing a new fact
- **AND** a limitation correction MUST NOT become a fact repair or approve release; disputed scope assertions are withheld in the limited projection while the original stays auditable

#### Scenario: Several valid review issues concern the same original fact
- **WHEN** the initial source review reports distinct valid findings for one fact, such as subject scope and revenue attribution
- **THEN** all findings remain auditable and the existing combined repair request contains one target with every issue, not duplicate replacement targets
- **AND** the final source check receives all issues for that candidate; absent, rejected or unverified repairs do not revive the flagged fact, while duplicate returned candidates remain invalid

#### Scenario: A repaired amount still uses the wrong financial column
- **WHEN** a patch quotes a locatable amount but final source review rejects its metric meaning
- **THEN** the patch is not accepted, the affected fact remains withheld and independent business facts stay readable
- **AND** neither shared-dimension membership nor absence of a new issue alone resolves the original issue

#### Scenario: Independent revenue statements have different reconciliation scopes
- **WHEN** selected sources contain an independently meaningful company total, principal-business income and a reconciliation adjustment
- **THEN** extraction and omission review SHOULD organize independently usable assertions and repair targets without bundling unrelated scopes into one all-or-nothing candidate
- **AND** related same-subject metrics MAY remain together; the program MUST NOT salvage assertions from a rejected candidate by splitting text or guessing scope

#### Scenario: One repair candidate cannot safely bind its source
- **WHEN** a combined repair has unique known target IDs but one candidate fails its existing source, number or unit checks
- **THEN** only that candidate is rejected with its original proposal and concrete local reason retained, while independent valid candidates may reach the single bounded final source verification
- **AND** unknown target IDs or duplicate targets still reject the batch before any final send; invalid candidates are never verified or adopted, no valid candidates means no final request, and local validity never substitutes for an explicit final check
- **AND** candidate-local typed metric errors are isolated under the same rule; the transmitted schema still describes complete typed facts, and raw proposals plus concrete local rejection reasons remain auditable

#### Scenario: A repair quotes a source-native monetary change or currency-decorated unit
- **WHEN** a same-unit metric retains an explicit source phrase such as 增加8196万元 or 减少2805万元 and its proposed signed value agrees, or source_unit spells 人民币百万元 while unit is 百万元
- **THEN** the owner may retain the exact directional wording with a null normalized value and uncertainty note, or accept the same-currency same-magnitude unit spelling without converting the amount
- **AND** a changed sign, magnitude, incompatible currency/unit or missing source qualifier remains invalid; the unchanged wording must still bind to source and a proposed correction still requires final source review

#### Scenario: A source table has a missing consolidation adjustment
- **WHEN** review discovers a materially omitted subtotal/elimination in supplied source text
- **THEN** one local patch may add source-backed adjustment facts, and final review must explicitly confirm that issue and check those facts before adoption
- **AND** unrelated facts and the original extraction remain unchanged and the result does not claim whole-report recall

#### Scenario: Review or final verification cannot finish
- **WHEN** the configured review profile fails, reaches its budget or returns invalid references
- **THEN** the draft and any valid issue/withdrawal information remain queryable with the concrete failure and incomplete review status
- **AND** no new company entrance, automatic paid loop, production approval or fabricated pass is introduced

#### Scenario: An explicitly authorized final recheck reuses accepted prior stages
- **WHEN** a user authorizes only final source verification of existing review findings and repair candidates
- **THEN** the original owner may consume the retained accepted responses after binding the source version, original draft and response hashes, and send only final verification
- **AND** retained model calls and usage are identified separately from the current run, original failures remain unchanged, and exhausted logical recovery limits are not reset to reissue earlier stages

#### Scenario: A retained initial review resumes at the first repair
- **WHEN** a new bounded authorization explicitly selects repair_and_verify mode and supplies only the unchanged accepted original review receipt for the matching source and draft hash
- **THEN** the existing owner validates original raw bytes, payload and source binding before sending only the one combined repair and its final source verification
- **AND** the initial review is not repeated or relabelled as a new successful call; missing/mismatched history fails before sending, already performed repairs cannot be replayed through review-only mode, and the legacy final-only mode still requires both original receipts

#### Scenario: Rebinding a qualified display preserves the draft hash
- **WHEN** an existing source-backed workpaper already contains the program's monetary display qualifications and is rebound for review continuation
- **THEN** binding MUST NOT duplicate those qualifications or mutate its otherwise unchanged facts, metrics and hash
- **AND** original response hashes remain untouched and the display qualification does not establish semantic approval

#### Scenario: A retained repair failed an older local schema contract
- **WHEN** a hash-bound continuation explicitly requests local revalidation of a retained repair rejected only as schema_validation_error
- **THEN** the current typed repair decoder and all target/source constraints MUST validate the unchanged original response before any final request
- **AND** the original receipt MUST retain rejected status, model and usage; quality output separately identifies local revalidation, never treating it as a new successful model call
- **AND** absent explicit revalidation, tampered bytes/payload or unknown/duplicate targets MUST fail before a final send; still-invalid known candidates remain rejected and excluded from final verification without discarding independent valid candidates, and other rejection reasons are not implicitly eligible

### Requirement: Workpapers expose the actual model and reading scope
Each new LLM workpaper MUST bind locally generated model metadata from the existing gateway receipts, not model-authored attribution. Metadata MUST distinguish section selection from actual section-text interpretation, record the model/source label, logical and selected profiles when available, request and response identities, actual supplied section/page scope, prompt/preprocessing/guide versions and known usage. The successful returned model MUST be the content author; failed attempts and retries remain in the existing receipt/ledger and MUST NOT be mislabeled as successful readers. JSON, Markdown, query and export MUST expose this metadata without a new store or evaluation platform. Missing historical metadata MUST remain unknown, and old workpapers MUST stay readable without rewriting their original results.

#### Scenario: Navigation and extraction use different pool members
- **WHEN** the gateway selects one source for the heading tree and another successful source for the chapter-text extraction
- **THEN** the workpaper identifies both stages separately and links the extraction author to its actual source sections/pages
- **AND** a failed or merely preferred route is not credited with the delivered content

#### Scenario: A historical workpaper has no model metadata
- **WHEN** an old workpaper is read without generation metadata
- **THEN** it remains readable without fabricated attribution or an automatic paid rerun

### Requirement: Supply chain enrichment is optional and source grounded
For new workpapers under the 2026-11-12 user instruction, company_profile_workpaper.v1 MAY include an additive optional supply_chain projection. It contains position, upstream_inputs and downstream_customers elements using the same fact, metric, status and Evidence contract. Missing or ambiguous supply-chain information MUST NOT fail an otherwise deliverable company profile, trigger mandatory recovery, or lower historical scores. Existing workpapers without the field MUST remain readable. Company position describes its reported production, processing, trade, operation or service role; upstream describes explicitly disclosed materials, energy or service resources; downstream describes disclosed uses, industries or customer types. Banks use funding/resources and service customers rather than fictitious manufacturing inputs. Product uses MUST NOT imply a named customer relationship, and R&D MUST NOT imply procurement.

#### Scenario: The selected sections do not name raw materials
- **WHEN** supported business/profile facts exist but raw materials or downstream customers cannot be found in the selected source scope
- **THEN** the profile remains queryable/exportable with an absent or explicitly limited optional supply-chain element
- **AND** no automatic extra search, failed-company result or invented relationship is introduced to fill it

#### Scenario: Existing high-level supply-chain descriptions are sufficient
- **WHEN** the workpaper already describes supported company position, resources or downstream uses but lacks precise material/product lists or quantities
- **THEN** the workflow MUST retain that useful optional content and MAY stop without further supply-chain reading or repair
- **AND** finer granularity is not a completion criterion; source correctness and isolation of known errors still apply

### Requirement: Reading questions guide report-local section selection
The selector MUST receive a versioned general question-to-section-family guide inferred from reviewed reports. Business and operating-mode subsections are primary sources; revenue/cost and business review sections provide main segment weights; procurement/production/sales, product applications and customer sections may enrich supply chains. Financial or segment notes are targeted supplements, while honors, governance and minor technical details are deferred. Families are semantic hints, never exact heading/page whitelists. Numbered decimal headings such as 3.6 and 3.6.2 MUST preserve their report-local hierarchy so useful subsections can be selected without reading an entire management chapter. Extraction MUST prioritize a supported main segment's revenue or operating scale before minor organization/technology details; unknown values remain unknown.

#### Scenario: Business introductions omit the main income composition
- **WHEN** business descriptions and scale highlights do not substantively explain revenue sources or main mix
- **THEN** the question-to-section guide points to this report's substantive income-statement, financial-condition or revenue-composition subsections without using a fixed company, page or exact title
- **AND** TOC-only duplicates and assets/AUM/capacity do not substitute for those sources; gross business receipts and net operating revenue retain their separate headers, and minor omissions do not require the full financial report

#### Scenario: A bank uses decimal subsection numbering
- **WHEN** the report contains a business subsection with its own revenue/scale and a later financial analysis subsection
- **THEN** the heading tree exposes both with correct boundaries, and the model can select them using the same company-neutral questions
- **AND** guidance contains no target-company answers and no reusable physical page policy

### Requirement: Manual reference workpapers precede extraction calibration
A MUST first independently read one official report and deliver a human_reference workpaper, its readable rendering and an explicit concentrated source scope before B tunes a v9 extraction route against it. The workpaper MUST cover the eight content elements as far as the selected sources support, retaining gaps rather than searching every appendix for marginal detail. Source choices MUST identify chapter, section and physical page, prefer a jointly informative authoritative occurrence for duplicate facts, and distinguish fact correctness from selected-source coverage. The reference is a development example, not proof that most listed companies already pass and not a per-company production hand-authoring requirement.

#### Scenario: One section supplies several useful elements
- **WHEN** an official management-discussion subsection supports business structure, products, markets and current changes with correct subject and period
- **THEN** the reference may cite that common source once per needed span and leave low-value supplementary facts unfound without reading every alternate disclosure
- **AND** a known material conflict is not hidden to concentrate sources, and unsearched content remains unknown rather than not_disclosed

#### Scenario: Another company uses different chapter names
- **WHEN** the same profile questions are applied to a different report with different TOC labels and physical page numbers
- **THEN** selection identifies that report's equivalent chapter functions and real boundaries rather than copying the example company's pages or exact headings
- **AND** target-company reference answers are excluded from the model input; a same-company replay is labelled calibration rather than unseen validation

### Requirement: Structured company workpapers have one evidence-linked contract

The v9 input/output projection MUST use schema_version company_profile_workpaper.v1, origin, report, reading_scope, elements, evidence and limitations. Origin MUST distinguish human_reference, llm_generated and controlled_fixture. Report MUST bind the existing instrument/report identity, document version, period and publication time. Reading_scope MUST identify selected section IDs/title paths and actual source ranges, declared unread/deferred scopes, selection rationale and the stopping reason. Elements MUST contain business_structure, products_services, revenue_model, scale_and_mix, operations_and_inputs, customers_channels_markets, operating_drivers and material_changes; the first six are the substantive content target and the last two depend on material disclosure. This projection MUST reuse existing fact/Evidence types and owners rather than create a parallel canonical store, eight ChapterTask values or a schema registry.

Each element MUST carry status, facts and limitations. Status MUST map explicitly to existing observed/not_disclosed/not_applicable/extraction_failed/unclear semantics; partial extraction, unselected/unread sources, not located text, model omission and unresolved conflict MUST be distinguishable in limitations or reading_scope, not falsely converted into not_disclosed. Each nonempty fact MUST contain fact_id, text, subject_scope, period and evidence_ids, with metrics when quantitative. A metric MUST retain its original metric name/raw value, unit/currency, subject, period and comparison basis when present; any normalized decimal value MUST avoid binary-float money loss and retain the source value. Group operating revenue, total operating revenue, segment totals, parent-only and industry metrics MUST NOT be conflated or added across incompatible bases. Qualitative facts do not require invented amounts, counterparties or industry labels.

Evidence MUST bind evidence_id, document version, section_path, physical pages and exact source spans or existing Evidence references, with printed-page labels when available. Table evidence MUST preserve its owning header, row/column, unit, period and necessary footnotes through linked source spans; it need not force all context into one giant quotation. No fact may rely on an unsupported citation or an invented section title. Readable output MUST be rendered from the same structured facts, with evidence and uncertainty retained, not authored as an unrelated second answer.

#### Scenario: Researchers export the currently graded workpapers as machine-readable data
- **WHEN** the existing read owner exports profiles containing workpaper_delivery
- **THEN** it MUST also write one research_profiles.json with the same graded facts, report/source/reading scope and actual generation/review metadata, using the existing Fact/Metric/Evidence representation
- **AND** source_only, withheld and duplicate entries retain their status/reasons with null fact payloads; raw workpapers and original/revised audit candidates MUST NOT be copied into that research payload
- **AND** pending/limited facts retain their labels, qualifications and null values as applicable; research delivery is not formal acceptance or production authorization, legacy audit JSON/CSV/Markdown remain available, and no model call or parallel writer is introduced

#### Scenario: Export includes the same-report supplemental readings visible through the API
- **WHEN** the original read owner exports a selected workpaper base that has subsequent readings
- **THEN** it MUST additionally write annual_profiles.json containing the same safe annual-profile projection and exact report/hash-bound reading ancestry as that selected base, including each reading's work identity, facts, sources, actual review status, remaining gaps and acquisition suggestions
- **AND** it MUST write annual_profiles.md from that same projection, showing the current readable facts, basic elements, optional supply chain/features, remaining gaps, reading scopes and actual model/review attribution without synthesizing new claims or changing legacy single-reading exports
- **AND** readable facts and citations MUST be addressed by their reading work ID as well as local fact/evidence ID; superseded, withheld, source-only and duplicate bodies and their exclusively cited source quotations MUST NOT reappear as current human-readable conclusions
- **AND** existing per-reading JSON/Markdown/CSV and research_profiles.json remain compatible; the new combined file is listed in the existing export manifest, contains no raw rejected/original repair candidates, and preserves null payloads for withheld/source-only objects
- **AND** stock, work and processing-identity selection MUST NOT silently export a different latest base or a different report; missing, orphaned or hash-mismatched supplemental ancestry is not merged, repeated fact IDs remain reading-qualified, and no model calls or source writes occur
- **AND** this is a research delivery only, not cross-reading reconciliation, full-report completion, revised historical scores or production authorization

#### Scenario: A model discovers distinctive information outside the baseline
- **WHEN** the selected original sections contain company-specific information the model judges useful beyond the baseline eight elements
- **THEN** the model MAY populate optional company_features using the existing element/fact/metric/evidence contract, without a fixed feature taxonomy, mandatory number of extras or a parallel writer
- **AND** extraction, source binding, secondary review, bounded repair/recheck and JSON/readable export MUST preserve valid feature facts and their model attribution; novelty is not itself a rejection reason
- **AND** a malformed or unsupported optional feature is withheld locally without losing valid baseline or other feature facts, absence of extras is not failure, and old workpapers without the extension remain readable with unchanged historical hash inputs
- **AND** reviewers MAY suggest a genuinely informative source-backed feature through the existing bounded omission-repair path, but MUST NOT demand every possible enhancement or invent unsupported company traits

#### Scenario: Useful local corrections do not imply perfectionism
- **WHEN** a real defect affects core correctness or reusable reading capability
- **THEN** a minimal local correction remains permitted; avoiding unbounded per-company rules does not ban all local fixes
- **AND** source-backed uncertainty, small isolated defects and optional omissions MAY remain limited while independent content is delivered; known wrong facts are not silently published as reliable and formal thresholds or production authority do not change automatically

#### Scenario: Core facts remain independent of optional detail
- **WHEN** a new model reads a concentrated source packet to produce the shared eight-element workpaper
- **THEN** it MUST express independently usable company revenue and main business assertions separately from optional growth comparisons, subsidiary detail or unresolved event timing, using the existing fact structure rather than a new schema
- **AND** review MUST prioritize wrong content and omissions that change the main business interpretation; missing optional margin changes, rankings, counterparties or subsidiary profits alone MUST NOT require another repair, and a failed separate fact MUST NOT withdraw valid independent core facts

#### Scenario: A compact principal income composition is already supplied
- **WHEN** selected original sections disclose a compact composition explaining the company's main revenue sources
- **THEN** extraction and the existing source review MUST prioritize its material categories and supported amounts or shares before supplementary operating numbers, not substitute only the largest category or company total for the mix
- **AND** this is a source-led model judgment, not a fixed industry account list, exhaustive-row requirement, arithmetic fill-in, new call or deterministic completion gate; unavailable or ambiguous items remain limited

#### Scenario: Optional discoveries have differing explanatory value
- **WHEN** the model chooses optional company-specific information from the selected source
- **THEN** it SHOULD prefer concrete, supported explanations of the company over bare slogans, honors or subsidiary figures, while retaining freedom to choose unfamiliar characteristics without a taxonomy or count requirement
- **AND** review MUST distinguish factual defects from writing preference: low explanatory value alone MUST NOT demand another repair or discard valid independent content; no unsupported significance or causality is invented

#### Scenario: A disclosure item's applicability mark is summarized
- **WHEN** a source marks a change/event disclosure item as not applicable without an explicit assertion that the economic change/event did not occur
- **THEN** extraction, source review and any bounded correction MUST preserve that disclosure-item scope rather than turn it into a categorical absence assertion
- **AND** original-source uncertainty and unrelated business facts remain available, without a keyword-based semantic veto

#### Scenario: A qualitative financial absence asserts a zero without digits
- **WHEN** a new fact categorically asserts no revenue/cost/expense without a numeral, rather than saying that the value is unknown or not disclosed in the read scope
- **THEN** its cited source MUST explicitly support that scoped absence; a blank table cell, a zero in another column or a -100% comparison MUST NOT supply it
- **AND** the source binder MAY conservatively require that explicit absence clause to be retained in the original source wording; failure withholds that fact only, leaves valid independent facts available, and does not rewrite historical workpapers or claim complete semantic verification

#### Scenario: A numeric continuation includes a following text label
- **WHEN** a line begins with decimal table values followed by another cell's text label
- **THEN** the report-local section index MUST retain it as table source rather than terminate a true chapter at a fabricated heading
- **AND** genuine numbered headings remain selectable, with original physical coordinates and complete table continuations unchanged

#### Scenario: A narrative amount is absent from the metric list
- **WHEN** a new draft or repair states a monetary amount, percentage, percentage-point change or explicit financial zero in its narrative, including an assertion omitted from `metrics`
- **THEN** the existing source binder MUST check that assertion too; an omitted metric MUST NOT bypass numeric source constraints, and a blank cell or a -100% comparison MUST NOT supply a reported zero
- **AND** existing responses whose unstructured number is directly source-supported MAY retain the original sourced wording without inventing a metric name or normalization; this source-only check MUST NOT be represented as complete metric coverage or semantic/column verification
- **AND** a failed assertion withholds only its affected fact and dependent coverage, preserving independent content through the original writer, query and export; explicit source zeros, faithful qualifications and unaffected old files remain valid, and no company-specific business classifier or parallel writer is introduced

#### Scenario: A metric has separate header and amount spans
- **WHEN** the amount and its governing table header occupy separate source spans or continuation pages of the same identifiable table
- **THEN** linked Evidence retains the row/column, subject, period and unit as one traceable support context
- **AND** the program does not guess the column, require one unnecessarily long quotation or accept an amount merely because an ID exists

#### Scenario: Correction preserves the meaning of subject labels
- **WHEN** extraction, initial source review, local correction or final source verification handles a company-wide fact containing narrower business metrics
- **THEN** all four requests and their metric/fact field descriptions MUST use the same subject-label meanings: issuer requires explicit parent-only evidence, and a business or industry row remains business_segment rather than inheriting consolidated_group from the enclosing fact or table
- **AND** each metric is judged on its own source object, uncertainty remains explicit, and a locatable amount or similar sum alone does not establish scope or category equivalence
- **AND** this is an aligned model contract, not a keyword-based semantic validator or automatic rewrite of old labels, values, hashes or acceptance results

#### Scenario: Some elements are missing
- **WHEN** a selected section packet supports the main business and products but does not support a key input or customer detail
- **THEN** the workpaper publishes the supported facts with explicit missing scope/reason and no fabricated value
- **AND** eight populated keys or successful JSON parsing alone establishes neither content completeness nor a passing company score

#### Scenario: An important information item was not acquired
- **WHEN** a sourced profile explains the main business, principal products/services and earning mechanisms but an important supplementary item was not acquired or remains unverified
- **THEN** the profile MUST remain deliverable with explicit missing scope/reasons and suggested acquisition where useful, without treating importance, an open omission or an unfilled checklist item as a whole-profile rejection
- **AND** source-grounded company scale need not include both total operating income and operating revenue; their distinct names/scopes MUST remain intact, and specific product subclasses, subsidiary service details and small revenue tails are not mandatory completion items
- **AND** the existing query/export MUST carry element status/limitations and unresolved omission suggestions without promoting those suggestions to facts, withdrawing independent valid facts or automatically issuing more reading/paid requests
- **AND** a genuinely unanswered basic business question remains explicitly incomplete but its reliable partial content is still delivered; known false assertions remain locally withheld, while historical frozen scores, formal batch validation and production authorization stay separate
- **AND** future acceptance contracts MUST separate basic business questions from important supplementary targets; supplementary targets MUST NOT enter the mandatory denominator for basic profile completion, and prior frozen contracts/results MUST NOT be rewritten

#### Scenario: Layout reordering must not silently erase selected source sections
- **WHEN** an official selected section has nonempty native source text but layout remapping places its end boundary at or before its start, including a running footer moved before the body
- **THEN** the new preprocessing identity MUST recover that affected page from the existing native text with explicit layout/table limitations, retaining source coordinates and selected-section bounds rather than silently sending an empty section or guessing an enlarged span
- **AND** independent readable pages retain their layout, known table uncertainty remains visible, and old preprocessing identities, saved drafts, model calls and scores are not rebound or rerun

#### Scenario: API clients inspect gaps and request targeted subsequent readings
- **WHEN** a client reads an annual-report company profile through the HTTP API
- **THEN** the original read owner MUST expose graded facts, explicit missing/uncertain items, suggested acquisition and selected/unread scope without raw rejected candidate payloads or implicit model calls
- **AND** the API MUST allow a stock-specific, report/hash-bound enrichment plan naming concrete questions; planning is not execution
- **WHEN** an explicitly authorized enrichment is executed
- **THEN** the original TaskService MUST enqueue/drain only the requested stock using report-local chapter selection, extraction and existing source review under the supplied finite budget
- **AND** initial source review, any local repair and final recheck MUST receive the same targeted reading questions; deliberately unread non-target elements MUST NOT require whole-profile completion, while every asserted fact remains subject to source correctness checks
- **AND** every subsequent reading retains its base work identity/hash, actual model/source/review metadata and original writer record; API reads compose only the exact same-report lineage, keep prior valid facts on failure, and permit further explicitly requested readings
- **AND** newly acquired content MUST NOT automatically close every gap sharing an element; absence of discovered gaps is not proof of full-report completeness, batch first reads remain independent, and historical results/production authority stay unchanged

#### Scenario: A research API reads an explicitly selected persisted cohort
- **WHEN** a deployment explicitly selects a research output root for saved real workpapers
- **THEN** the API MUST use the same original read owner on that root and reject a missing configured root without silently falling back to another dataset
- **AND** original writer records, actual attribution and use decisions remain traceable; independently established acquisition gaps MAY be appended as local usage limitations on a successor research copy without changing source facts or historical artifacts
- **AND** selecting a research dataset MUST NOT enable production tasks, model calls, automatic replay or a production quality claim

#### Scenario: API clients discover saved research profiles before selecting a stock
- **WHEN** a client requests the annual-profile catalog on the explicitly selected research root
- **THEN** the same read owner MUST list the currently readable workpaper companies, report/work identities, visible fact/metric counts, per-reading origins and actual review statuses, and known gap/suggested-acquisition elements from the same-report detail projection
- **AND** hidden/source-only/duplicate payloads MUST NOT be exposed or counted as visible content; pending and limited facts remain distinguishable, and incomplete review MUST NOT be inferred complete from nonempty counts
- **AND** no readable workpapers yields an empty catalog, an unavailable configured root remains an explicit error, and the read performs no model calls, implicit root selection, business writes or change to historical/S1/production status

#### Scenario: A useful workpaper has no trusted core facts yet
- **WHEN** query/export reads a sourced workpaper whose trusted-core accepted_fact_count is still zero
- **THEN** it MUST also expose a separate workpaper summary with its original business description, fact/metric counts, origin, review status, unresolved-finding count and known actual extraction/review models
- **AND** CSV and a readable export overview MUST distinguish this draft content from trusted facts; legacy accepted counts, dimensions, core_complete, source contents and production authorization MUST remain unchanged
- **AND** old profiles without a workpaper remain readable with no fabricated workpaper counts or model attribution, and the view performs no model calls or production writes

#### Scenario: Existing research workpapers are presented together
- **WHEN** an explicitly read-only delivery view selects saved workpapers from multiple completed development windows
- **THEN** it MUST retain source query hashes, report versions, actual model/stage attribution and fact/evidence identities, and separate known withheld, source-only and duplicate objects from ordinary excerpts
- **AND** the current readable entry and a reference index MUST use the same selected facts without changing business values, copying a parallel fact store or rewriting historical source results
- **AND** cross-window content, review statuses and an intentionally pending core flag MUST NOT be represented as same-version acceptance, measured accuracy, formal company usability or production permission; the view sends no models and changes no business database

#### Scenario: Original query and export deliver research facts by reviewed scope
- **WHEN** the original read owner reads a workpaper with source-bound facts, actual model-review records and optional independently authored use-review decisions
- **THEN** query, Markdown and CSV MUST distinguish model-checked, independently source-checked, pending, limited/source-only and withheld scope without changing formal accepted facts, core completeness or production authority
- **AND** independent decisions MUST bind the report version and current fact-content hash with reviewer, evidence and reason; unknown/duplicate identities, stale facts or wrong versions are rejected, the extraction response cannot author this metadata, and old absent metadata preserves legacy serialization/hash inputs
- **AND** known withheld, source-only and duplicate facts MUST NOT enter ordinary business text or standardized delivery metrics, while their unchanged originals and audit metadata remain available; one company's incomplete review does not discard unrelated checked facts, and completed status alone never overrides a source restriction
- **AND** truthful source uncertainty MAY remain limited and readable; the existing writer/read owner serves persistence and all export formats without a new store, production dependency on development evidence or implicit model calls

#### Scenario: Human reference is rendered for review
- **WHEN** a human_reference JSON workpaper is used to check presentation and local mapping
- **THEN** the rendered facts, metrics and evidence remain the same as the JSON and their origin remains visible
- **AND** this does not change historical model results, authorize a call or claim llm_generated completion

### Requirement: Cost-aware section work stops without fabricating completeness
The v9 workpaper path MUST optimize useful business content per reading and model cost, not perfect appendix coverage. It MUST record the chosen source set and why the scope stopped, using a high-value section pack followed only by targeted material-gap recovery. Actual and unknown resource use MUST follow the current explicit execution authority; no complete-report hypothetical synthesis/verification reserve may be imposed as a prerequisite to a pending workpaper. This does not waive the real request/context/output limits or cumulative budget, recovery and permission caps. Formal evaluation thresholds remain versioned separately from basic workpaper display.

#### Scenario: Explicit local structural normalization consumes a retained failed response
- **WHEN** a new local-only task explicitly binds an accepted original section selection, matching official source and nonempty finish=stop failed content by hash
- **THEN** the original owner MAY append exactly one missing terminal object brace and relocate known root fields misplaced inside elements without key conflicts, provided strict unique-key finite JSON parsing and all eight baseline elements succeed
- **AND** it MUST NOT fill business values, missing strings/arrays/elements, unknown fields or ambiguous structures; existing typed/source/number checks and local quarantine still apply
- **AND** the original raw content, rejected status, actual author and usage remain unchanged; separate normalization metadata records policy, operations and original/normalized hashes, not a fabricated accepted model receipt
- **AND** this explicit local mode performs no model sends or automatic review, delivers an unreviewed candidate through the original writer/query/export and does not promote trust, core completeness, historical scores or production authority; old retries and continuation contracts are not retroactively relaxed

#### Scenario: A separately authorized review follows local normalization
- **WHEN** a new bounded authorization explicitly selects a saved, locally normalized LLM workpaper as a review seed
- **THEN** the existing seed/source binding and review owner MAY review it without rerunning extraction or carrying a local-only extraction continuation into the review run
- **AND** the original extraction remains rejected with its original content/model/usage attribution; new review/repair/final calls are separate, and rendering MUST distinguish unreviewed from actually reviewed candidates without implying trusted release
- **AND** the saved rejected bytes MUST match their original hash and reproduce the exact recorded strict normalization; ordinary unnormalized extraction still requires an accepted receipt
- **AND** subsequent recovery MUST preserve independent restrictions and any added source-display limitations only when they reconstruct the exact original review-input hash; it MUST NOT skip that hash check, invent missing source content or silently clear the limitations after a failed review

#### Scenario: An already allowed retry follows a malformed workpaper JSON response
- **WHEN** an explicitly opted-in workpaper request fails JSON or Schema validation and its existing profile/card still permits a physical retry
- **THEN** that retry carries the concrete validation error and previous response instead of blindly repeating the original request
- **AND** logical identity, deadline and physical/recovery/usage budgets remain cumulative, no extra retry is created, no local JSON repair fabricates a valid draft, and requests without the opt-in retain their prior behavior

#### Scenario: An explicitly authorized extraction continuation reuses navigation
- **WHEN** a new bounded authorization provides a hash-bound matching source, accepted original section selection and an original finish=stop malformed JSON response
- **THEN** the existing task owner may rebuild the same selected source packet and request extraction with the original response and precise JSON error, without reissuing navigation or locally completing the malformed draft
- **AND** the retained section-selection attribution and original failed usage remain separate from new extraction/review calls, the continuation survives the existing checkpoint, and missing/tampered/wrong-source or actually valid prior JSON is rejected before any send

#### Scenario: A response budget cannot hold an exhaustive numeric inventory
- **WHEN** selected chapters disclose more subsidiary, operating or supplementary figures than a useful core workpaper needs
- **THEN** extraction MUST prioritize a complete core-profile JSON over exhaustive numeric copying, keeping every material business line and its main revenue/scale basis before optional detail
- **AND** soft writing targets MUST NOT become company-completeness gates or remove source checks, uncertainty, required element keys or resource limits; a prompt-only correction is not proof that a new real extraction succeeds

#### Scenario: Additional sources offer only marginal detail
- **WHEN** the chosen chapters already explain the business and a further appendix would add only minor examples or a supplementary quantity
- **THEN** the owner may stop, declare the unsearched detail and deliver the sourced workpaper without a full-report sweep or another paid completeness loop

#### Scenario: No new real-call authority exists
- **WHEN** the three-company preview entrances have all been consumed and only document/manual/offline work is in scope
- **THEN** the workflow may prepare the reference and section-format contract but MUST NOT issue a model probe, reopen a company, repurpose leftover budget or infer a new call allowance from specification approval

### Requirement: Effect previews show sourced content before completeness acceptance
An explicitly authorized preview task MUST publish readable company draft content with report identity, source references and current trust or missing-field labels without requiring completed three-view coverage or a passing S1 score. Known errors MUST NOT be displayed as trusted facts, and preview display MUST NOT promote claims, change historical completion results or authorize production. User feedback MUST determine the next content priorities instead of making all possible fields mandatory in advance.

#### Scenario: The user requests a three-company demonstration within three rounds
- **WHEN** the current user authorizes bounded previews of three specified companies before selecting mandatory and optional content
- **THEN** the first handoff includes readable draft content, the second includes all three current-run sample results or explicit per-company execution failures, and the third closes presentation feedback without automatically starting a fourth engineering round
- **AND** one company's incomplete profile does not block displaying other independent company drafts, historical reused examples are labelled separately from new runs, and all real calls remain inside the current card's fixed cumulative resource bounds

### Requirement: New-report interpretation has a real semantic execution path
The new processing policy MUST use the existing real LLM gateway and company-profile owner for new-report open-language model extraction; human_reference workpapers and controlled fixtures MUST retain their distinct origins and MUST NOT count as model success. For v9, the selected section packet MAY directly yield evidence-linked structured profile facts and readable output without first generating a separate condition inventory. Real semantic acceptance still requires the applicable source and reliability review. Existing field IDs, rules, fake providers or absent providers MUST NOT establish model completion. Same-report reference answers and evaluation Gold MUST NOT be provided to an independent extraction; generic format instructions and clearly labelled different-company examples MAY teach the structure. Reuse and continuation require identical or explicitly revalidated report/source/code/model/prompt lineage and current resource authority; first-run evaluation declares cache conditions and excludes target semantic reuse.

#### Scenario: Existing overview does not bypass the model
- **WHEN** a new report already has a deterministic BusinessOverview record
- **THEN** semantic discovery, interpretation and coverage review still execute and retain real request/model/usage evidence

#### Scenario: Provider is unavailable
- **WHEN** no real provider is configured or calls exhaust their budget
- **THEN** reliable independent numeric facts remain available but missing semantics are explicit and the company cannot be declared semantically complete

#### Scenario: The first development report uses the existing configured pool
- **WHEN** the B1 card authorizes one bounded real development observation through the existing semantic pool and exact channel billing or tokenizer metadata is unavailable
- **THEN** the implementation retains configured load distribution and failover, freezes the pool policy and execution identity, reserves each physical request conservatively within the token/request/deadline ledger, settles only valid returned usage, and records unknown cost without inventing a price or claiming cost acceptance
- **AND** missing commercial metadata or an overlarge static byte projection alone does not prohibit that authorized observation; business quality remains source-audited and neither further reports nor production are authorized automatically

#### Scenario: Explicit development continuation reuses navigation only
- **WHEN** the current A card explicitly authorizes continuation with prior real navigation whose report, source coverage, model, prompt and navigation semantics are validated as compatible
- **THEN** those receipts may identify pending reading scopes, with original attempts and charges preserved, but cannot establish verified claims or be reported as a fresh first-run evaluation
- **AND** substantive reading, source-first omission checking, synthesis and verification still execute through the authoritative owner

#### Scenario: A later stage fails after a verified claim was saved
- **WHEN** later reading or provider failure interrupts the same frozen development task
- **THEN** existing checkpoints retain successful receipts, exact read ranges and independent verified claims for query/export, while unfinished claims remain unverified and the authorized continuation processes only valid unfinished work
- **AND** window and task charges, unknown reservations and logical-request recovery counts remain cumulative under the current card, without changing the default public gateway policy

#### Scenario: The second synthesis batch fails
- **WHEN** the first source-supported batch has yielded candidate claims and another batch is waiting
- **THEN** the first candidates are durably retained as unverified, their batch is verified and its independent supported results are committed through the existing writer and query/export before the next synthesis batch is sent
- **AND** a later failure or process restart preserves those verified results and explicit unresolved gaps; a draft's model-reported observed status does not make it a verified result
- **AND** later source conflicts invalidate affected claims and dependencies without clearing unrelated supported results

#### Scenario: A narrows a development slice to fresh semantic execution
- **WHEN** an explicit A card ends an unsuccessful semantic-resume implementation and authorizes a bounded fresh-generation business slice
- **THEN** the actual operator and persisted-binding boundary reject prior navigation, discovery, synthesis, verification and verified-claim inputs for that slice, while PDF parsing caches may remain reusable
- **AND** real reading, independent source-led omission checking, batch verification and existing-owner publication still satisfy unchanged business criteria; reliable partial query results remain available after failure without authorizing another entrance
- **AND** semantic continuation remains unaccepted capability work to verify before S3 rather than an implied success, and new execution uses only the card's explicit resource authorization without changing historical attempts or charges

#### Scenario: A batch's native dependency fails verification
- **WHEN** verification marks a referenced native record block or unclear while marking the dependent claim pass
- **THEN** the record is quarantined and every dependent claim remains unverified or is withdrawn from verified query/export results before another batch proceeds
- **AND** the same dependency rule applies at final publication and to later-discovered conflicts, without removing independent supported claims

#### Scenario: A local batch does not cover all report dimensions
- **WHEN** synthesis receives only a subset of the report's source conditions
- **THEN** it returns claims only for supported dimensions of that batch, without inserting global missing-dimension placeholders; completeness is evaluated after aggregation and source-first omission checks
- **AND** supported qualitative products, customer types and operating roles do not require concentration figures, transaction names or literal industry-role labels, while unsupported numbers and relationships remain unasserted

#### Scenario: A explicitly rebinds an unused development continuation
- **WHEN** the current A card authorizes a repaired snapshot to use an already allocated but unused development entrance
- **THEN** old raw receipts may be revalidated with explicit source ranges, old and new identities and derived-result lineage, without changing prior failures or reporting a same-version first-run score
- **AND** the new entrance freezes before sending, retains cumulative unknown charges and recovery counts, and does not gain extra entrances or resource limits

#### Scenario: A successful response is reloaded from a persisted JSON checkpoint
- **WHEN** an authorized continuation loads a previously valid raw response from disk with its source and execution lineage
- **THEN** the JSON boundary preserves the same strict schema meaning as live JSON ingestion, including JSON arrays for sequence fields, without disabling identity, reference, number or unknown-field constraints
- **AND** readiness evidence uses the actual operator, binding loader and disk-continuation branch; rebuilding the response as a new provider reply does not prove that branch works

#### Scenario: An offline complete-path check substitutes the external model transport
- **WHEN** a task card permits engineering validation without real model calls
- **THEN** fresh and disk-resumed execution use the authoritative owner and persistence/read path, retain nonempty linked views and independent valid verified-state claims under explicitly simulated verification, and isolate invalid objects and dependencies
- **AND** batched verification accounts for every required target and unresolved major omission, real semantic completion remains false, and mock results neither advance business acceptance nor authorize another real entrance

#### Scenario: The direct-reading path finishes its necessary core scopes
- **WHEN** navigation, requested necessary reading, source-led review and verification succeed for all major core conditions through the actual entry and owner
- **THEN** report-level completeness is derived from that evidence and the remaining necessary gaps, rather than a constant false value or the existence of one claim per dimension
- **AND** a second major business in the same dimension remains a separate omission target, ordinary unread supplementary notes remain explicit, and simulated transport never establishes real semantic completion

#### Scenario: The pool distributes a logical request across sources
- **WHEN** a bounded task permits an initial physical attempt and a limited recovery within its entry deadline
- **THEN** logical and physical attempt timeouts are consistent with that task's recovery policy and remaining entry time, without treating a routing log's logical remaining time as the entry's remaining time
- **AND** readiness uses actual nonempty verification payloads and avoids repeating expensive identical preparation in parent, worker and business paths

#### Scenario: A later navigation batch fails after an earlier batch selected readable core pages
- **WHEN** one navigation batch has selected usable contexts and a later navigation batch fails within its bounded recovery allowance
- **THEN** the existing owner reads, source-reviews, verifies and publishes independent supported claims from the selected contexts while recording the failed navigation range and dependencies
- **AND** this permits independent partial delivery after a navigation failure without imposing a per-fragment business loop; the current section policy still combines cross-chapter core evidence and unresolved major-business risk prevents company completeness

#### Scenario: One independent business package fails while the global budget remains usable
- **WHEN** bounded synthesis, source review or verification fails for one package and another independent core context is available
- **THEN** the failure is scoped and retained, unverified candidates cannot become reliable answers, and the existing loop may continue the independent package within the same frozen window and remaining global limits
- **AND** the LLM may prioritize revenue, product, input and role contexts from the full report structure rather than processing every supplementary selected page in natural page order

#### Scenario: Independent source review fails after candidates were generated
- **WHEN** synthesis returns observed candidates but source-led review fails before verification
- **THEN** retained candidates remain pending or unreliable in persisted query/export and no claim can be projected as verified merely from the model's observed status
- **AND** unrelated genuinely verified claims remain queryable, while another independent package can proceed within the original frozen global limits

#### Scenario: The final allowed source attempt receives the remaining logical deadline
- **WHEN** a task allows two source attempts and the first has consumed part of their shared deadline
- **THEN** timeout allocation accounts for the attempt already consumed and does not divide the remaining time as though two further attempts were still authorized
- **AND** physical profile limits, entry deadline, shared recovery policy and actual send accounting remain enforced without authorizing an extra source attempt

### Requirement: Discovery and omission checking are source-led
The v9 ordinary path MUST begin with each report's TOC, bookmarks or equivalent heading hierarchy, choose relevant sections or subsections by business questions, and restore their actual boundaries with source-bound title locations. Physical pages are coordinates, not reusable selection policies. The system MUST prefer a small set of information-dense, subject/period-correct section sources that jointly support many profile elements, without requiring a global set-cover optimizer or repeating all alternative occurrences. Selected paragraphs and tables MUST retain necessary governing context and continuations. Local indexing MAY span all pages, but only declared section text and necessary recovery context are supplied for model analysis. Candidate-free source grounding MUST NOT receive the target company's answers or Gold; it MAY be evaluated from a frozen independent manual reference instead of a compulsory separate paid inventory request. Missing secondary details MAY remain explicit at the stopping boundary; a material business omission, subject/number conflict or damaged table context warrants only bounded targeted recovery under current authorization, otherwise partial delivery. Selected-scope coverage MUST NOT be reported as full-report completeness; known major gaps still prevent formal company usability. Historical v8 and earlier runs remain unchanged.

#### Scenario: A running chapter label follows an annual-report page footer
- **WHEN** native page text ends with an annual-report page-number line followed by an unnumbered chapter label
- **THEN** the new versioned index excludes that page-furniture label rather than inventing a new chapter boundary, while retaining real body headings and original text coordinates
- **AND** saved navigation under older preprocessing identities keeps its original index behavior; it is not rebound to renumbered current sections

#### Scenario: One selected candidate has no locatable layout boundary
- **WHEN** the new versioned section path cannot locate a selected candidate's start or end title but another selected section is readable
- **THEN** the existing owner records the affected candidate as unread with its reason and delivers only the valid sections, preserving table headers, continuations, source coordinates and the unresolved coverage
- **AND** validation precedes ancestor deduplication so an unusable ancestor cannot suppress a selected usable child; no offsets or full-report fallback are guessed, all-unreadable selections still fail explicitly, and older strict processing remains unchanged

#### Scenario: Navigation omits a fee or purchasing disclosure
- **WHEN** source-led review, section references or major income composition reveal a missing fee mechanism or purchasing disclosure
- **THEN** the owner locates and reads its necessary context without candidate answers, including table headers and continuations, or retains the unresolved major gap
- **AND** it does not restart an unrelated full-report scan, infer absence from the initial selection or substitute a company-specific page whitelist

#### Scenario: Ordinary delivery uses only key sections
- **WHEN** selected sections and targeted expansions establish all major business conditions, valid support and coverage checks, and no unresolved necessary source risk remains
- **THEN** the existing owner may derive core completion while other pages remain not_read; it does not claim full-report review or overwrite their actual states
- **AND** both core quality and measured reading volume, time, physical calls, effective usage and unknown reservations are reported; fewer pages with lost major content do not establish an efficiency improvement

#### Scenario: Full-text samples calibrate a scalable section policy
- **WHEN** existing development reports have full-text or extensive source reading and independently identified core sources
- **THEN** they inform reusable business-question-to-section guidance and omission challenges rather than requiring another full-text pass for every company
- **AND** a fresh section-policy validation uses its own source reading and does not load prior conditions, candidate answers or verification results as its generated output; its independent core denominator remains unchanged

#### Scenario: The source inventory omits an explicitly disclosed core answer
- **WHEN** selected source text discloses a required core answer such as current-period operating revenue but the inventory omits it or only records principal-business revenue
- **THEN** candidate-free discovery and substantive coverage remain accountable to the declared business questions, not only the conditions the model happened to enumerate
- **AND** unresolved core omissions remain visible without substituting a different total, a future target or evaluation Gold; the frozen acceptance denominator and permitted minor omissions remain unchanged

#### Scenario: Source priority flags do not suppress an unanswered core obligation
- **WHEN** source discovery marks a disclosed input, operating mode or sales-channel condition supplementary while the corresponding report-level core question is still unanswered
- **THEN** the owner presents the relevant read evidence and actual candidates to that core question before spending remaining requests on repetitive supplementary detail, without automatically promoting a fact or marking coverage by dimension or priority alone
- **AND** existing verified claims may support multiple view projections through explicit valid coverage, while another major business and necessary unread sources remain blocking obligations

#### Scenario: Core synthesis is truncated while bounded recovery remains
- **WHEN** an actual core synthesis response is truncated and the current task still has authorized recovery, requests and ledger room
- **THEN** the owner preserves the failed attempt and any independent verified results, and may recover the failed logical request with a smaller complete core target under its original parent and cumulative recovery limits
- **AND** it does not parse partial JSON as success, resend the same oversized batch without change, reset recovery identity, or spend all remaining requests on secondary detail before the missing core is attempted

#### Scenario: Many supplementary conditions remain after major business is answered
- **WHEN** source-backed business grouping and current valid support and coverage checks establish the substantive core while repeated brands, regional examples or supplementary indicators remain
- **THEN** the owner stops core generation rather than generating and verifying one sentence for every remaining raw condition
- **AND** another real major business or monetization mechanism cannot be removed merely to reduce requests, and necessary context remains available for each verified group

#### Scenario: Source discovery requests a continuation after navigation
- **WHEN** body discovery requests necessary continuation outside the initial section selection even though existing dimensions already have passing checks
- **THEN** the requested scope becomes necessary and is read before core completion, or its unresolved unread status prevents completion
- **AND** navigation failure does not silently select the entire report as a full-text fallback

#### Scenario: Source references are presented for model selection
- **WHEN** a current processing identity asks the model to select provided source IDs
- **THEN** the request presents each selectable ID adjacent to its exact source wording and necessary local context, without candidate answers or Gold and without requiring the model to count character offsets in separate unlabelled text
- **AND** source offsets remain relative to the immutable parsed page, the actual wire presentation is included in admission metering, and a legal ID alone does not establish semantic support or condition coverage

#### Scenario: A full-context source response omits optional continuation suggestions
- **WHEN** an ordinary full_context discovery response contains its required semantic payload but omits requested_pages
- **THEN** the local contract treats that omission only as no additional page suggestion from this response and continues validating the source conditions, without erasing any already selected necessary page, prior continuation or major unresolved condition
- **AND** page_map still requires its selection payload, synthesis and verification still require claims and checks, and missing continuation metadata never establishes source completeness or not_disclosed

#### Scenario: Accumulated source conditions exceed a single synthesis request
- **WHEN** the actual company-wide source conditions and necessary evidence exceed either the physical payload-byte limit or the reservation limit
- **THEN** the authoritative owner adjusts the isolated development reservation or groups cross-chapter business evidence to fit actual service capabilities, preserving necessary context and major content
- **AND** self-imposed payload or reservation projections MUST NOT alone prevent producing a draft under the user's current resource authorization; keeping only the first package or silently dropping major conditions is not a substitute
- **AND** each real draft is persisted immediately with pending status and readable text before subsequent support and coverage verification; company usability still requires substantive major-business coverage and the unchanged quality thresholds

#### Scenario: A local payload rejection is found after valid full-text discovery
- **WHEN** the A card explicitly authorizes development continuation of identified same-report real source-discovery receipts after a terminal packaging change
- **THEN** the entry reloads persistent JSON and revalidates source identity, original sent ranges, evidence location, necessary context and version lineage before genuinely generating and verifying new claims
- **AND** unchanged successful discovery is not repeated over the whole report, raw conditions are not promoted to verified claims, earlier answers are not mixed in, and historical attempts and unknown charges remain unchanged
- **AND** the continuation is not a new first-run evaluation and uses the user's explicit current-task authorization rather than permission inferred from unused balance

#### Scenario: Readiness is tested against only a small source fragment
- **WHEN** a readiness plan uses the first source fragment, one synthetic verification target or simulated low usage instead of the actual accumulated company source conditions
- **THEN** it does not establish readiness for a real company run
- **AND** the first draft needs only the directly necessary input/send/pending-persistence checks; complete verification-chain readiness and projected recovery reserves MUST NOT postpone the first real answer, while simulations remain protocol evidence only

#### Scenario: Verification has not completed after a real draft was generated
- **WHEN** a real-LLM draft contains three-view text and source references but support verification is pending or fails
- **THEN** query and export preserve the readable draft with pending or unclear status and specific limitations rather than replacing it with empty views or a generic workflow message
- **AND** only genuinely verified content enters trusted projections; known errors are isolated or corrected, draft delivery does not count as company acceptance, and cost optimization follows content review

#### Scenario: A verified compound claim contains an unsupported relationship
- **WHEN** source review establishes an operating right but cannot support an added upstream relationship in the same claim
- **THEN** that object and dependent coverage MUST leave reliable query/export until a narrowed candidate is genuinely verified; the original frozen receipt and output remain unchanged
- **AND** acknowledging the unsupported clause only in a handoff report MUST NOT permit the object to remain a reliable published fact

#### Scenario: A new full-text development task replaces an exhausted selection strategy
- **WHEN** the current A card explicitly authorizes the v7 replacement under the user's existing execution and cost authority
- **THEN** the authoritative owner uses only the new task and per-entry limits, keeps all prior charges and failures intact, and does not treat old unused balance as permission
- **AND** offline readiness covers actual full-report payloads, nonempty company synthesis and verification, required context recovery and the card's failure reserve; simulated usage never proves real resource feasibility
- **AND** exhausted recovery prevents another recovery of the same logical request, not a genuinely new request still within authorized total limits, while retry and repair cannot reset logical recovery identity

#### Scenario: Supplementary notes remain unread after the core is verified
- **WHEN** real reading and source-first omission checks support all substantive core views and no known major-business gap remains, while supplementary notes have not been read in full
- **THEN** core usability may be evaluated under the unchanged acceptance thresholds and the unread notes remain explicit
- **AND** neither the navigation map nor the core result is reported as a full-text reading guarantee

#### Scenario: Navigation misses a potentially major business
- **WHEN** independent source review, revenue composition or omission checking identifies an uncovered major disclosure
- **THEN** bounded model-led recovery reads the necessary context or leaves the affected core limited, and first-run evaluation retains the omission rather than changing the sample contract

#### Scenario: An explicit replacement policy reads core sections directly
- **WHEN** A authorizes the D1 replacement within the same company-profile owner
- **THEN** a selected complete source context may directly yield evidence-linked candidates for the three views without a mandatory inventory-to-separate-synthesis stage
- **AND** source-led omission review still uses a separate request without candidate answers or Gold, followed by comparison and verification of the candidates and their actual native dependencies; one source-review receipt may serve only the source scopes it actually covers
- **AND** each successful verification group becomes durably queryable before the next verification group, while report-level usability still requires all necessary core reading, omission checks and the unchanged acceptance thresholds
- **AND** this replaces the superseded orchestration rather than adding a parallel writer or inferring real-call permission from old budget balances

#### Scenario: Core evidence spans early and late source chapters
- **WHEN** the explicitly enabled v6 policy identifies business descriptions in an early chapter and major revenue, monetization or input evidence in a later chapter
- **THEN** those core scopes receive reading opportunities before the owner exhausts resources inventorying and verifying supplementary details from the first page package, and company-level synthesis can use their combined source context
- **AND** transport partitioning does not impose a separate inventory, seven-dimension synthesis and complete verification loop for every page package; unread major scopes still prevent completeness and successful verification groups are persisted before later verification groups

#### Scenario: Requested necessary reading crosses a batch boundary
- **WHEN** a model requests necessary context in more than one batch and only the first batch or a fragment has been read
- **THEN** every outstanding necessary scope remains explicit and blocks affected core completeness; it cannot be relabeled as an ordinary unread supplementary note, nor may a fragment imply the entire page was read

#### Scenario: A financial continuation resembles a numbered heading
- **WHEN** a table continuation starts with a decimal fragment with leading zeros or a decimal followed only by numeric cells
- **THEN** that row remains original source text but does not terminate the containing section or become its title boundary
- **AND** genuine decimal subsection headings such as 3.7 and 3.6.2 remain selectable; an unlocatable boundary still fails explicitly rather than falling back to the whole report

#### Scenario: A major business uses an unfamiliar heading
- **WHEN** no historical heading regex matches a disclosed business
- **THEN** generic document discovery can still select and interpret the relevant context and the omission review checks its delivery

#### Scenario: A page is unreadable
- **WHEN** bounded existing OCR/layout recovery fails
- **THEN** affected coverage records extraction_failed and the summary cannot claim complete review of that content

### Requirement: Three linked research views preserve evidence and uncertainty
The system MUST deliver company profile, product/service output and input exposure, and evidence-supported supply-chain roles. Major business lines and revenue models MUST be substantive; product exposure MUST NOT be limited to commodities. Source facts, numeric transformations and model interpretations MUST be distinguishable. Conflicting evidence MUST be preserved, and unsupported customer names, commercial relationships, profit sensitivity and zero-exposure claims MUST NOT be invented.

#### Scenario: A service company has no commodity mapping
- **WHEN** its report supports service lines, fees and customer types
- **THEN** those remain available in the profile, exposure and supply-chain views without a commodity ID

#### Scenario: Important sources conflict
- **WHEN** two relevant passages disagree and a bounded reread does not resolve them
- **THEN** both remain traceable, affected claims are unclear or isolated, and unrelated reliable facts remain readable

#### Scenario: PDF layout whitespace differs from a returned quotation
- **WHEN** non-whitespace characters match a unique span within the exact source page and fragment supplied to the model
- **THEN** a character-position mapping may recover the contiguous original source quotation and offsets, retaining the model quotation and raw response unchanged
- **AND** altered non-whitespace characters, out-of-scope references and unresolved multiple matches remain ungrounded; quotation location alone does not establish semantic support
- **AND** contextual verification retains the relevant paragraph and heading to distinguish industry statements, company actions and plans; invalid objects and their dependencies are isolated without clearing independent valid results

#### Scenario: A PDF line break separates a quotation from its governing context
- **WHEN** an exact quotation belongs to an industry discussion, strategic target, different actor or negated statement whose governing source context crosses a layout line break
- **THEN** synthesis and verification receive that necessary source context together with the exact quotation, rather than only the quotation's PDF line
- **AND** insufficient context leaves the claim unverified or triggers an authorized bounded reread; keywords, company names, page numbers or quotation location do not replace semantic verification

### Requirement: Finite evaluation governs expansion and remediation
Implementation and review MUST follow the product requirements, acceptance program v2 and delivery plan in that order. Four historical development reports, twelve new-policy regressions, twenty unseen reports and another hundred controlled reports MUST remain distinct. S2 MUST stop at twenty or one authorized replacement confirmation group of twenty; S3 MUST stop at one hundred. Initial and remedied scores MUST remain immutable and separate. All three views, source-derived core conditions, bounded supplementary audits, resource limits and remediation costs MUST be evaluated under the acceptance program; uncertainty MUST NOT disguise unprocessed core information.

#### Scenario: Second twenty-company group fails
- **WHEN** the sole S2 confirmation group misses its frozen thresholds
- **THEN** expansion stops without a forty-first company and a scoped failure/capability decision is recorded

#### Scenario: Historical repaired tests pass
- **WHEN** rule-path regressions pass without real model execution
- **THEN** they remain useful regression evidence but do not advance real-LLM or unseen acceptance counts

### Requirement: Market rollout is measurable and bounded
After S3 and explicit research-release authorization, the system MUST retain every company in the frozen all-A-share universe N, including missing assets. S4 MUST follow acceptance program section 9 for wave sizes, complete status accounting, at least 98 percent technical delivery among readable valid reports, independent wave sampling, explicit unresolved limitations, cost/time limits and stop/resume behavior. Machine usability estimates MUST NOT be reported as human-audited market accuracy. Production, DCF and trading permissions MUST remain separate.

#### Scenario: One report is unavailable in a market wave
- **WHEN** official assets cannot be acquired within the authorized budget
- **THEN** that company remains in N with a specific asset status while other companies proceed and missing assets are not hidden in delivery statistics

### Requirement: Only hard constraints are deterministic semantic gates
Programs MUST enforce schema, exact identity/reference integrity, source numbers and unit conversions, resource and consumer permissions. Open-language actor, plan/negation, business significance, semantic completeness and summaries MUST use contextual semantic interpretation and source review. Heuristic rules MAY flag risks but MUST NOT become a closed vocabulary of permitted business meanings. Reports MUST be treated as untrusted data, not tool instructions. Model output MUST NOT write approved data directly.

#### Scenario: A valid new paraphrase appears
- **WHEN** a source-supported business explanation uses no historical acceptance regex
- **THEN** it is evaluated on contextual evidence rather than rejected solely for missing that phrase

### Requirement: Superseded execution rules cannot steer new work
The new-policy requirements MUST supersede legacy no-provider completion, fixed sample loops and old closure-based expansion instructions. Historical contracts and scores MUST remain unchanged and readable. Replaced code and one-off runners MUST be removed only after caller migration and representative validation, with exact cleanup records. Necessary source assets, historical evidence and pre-existing worktree changes MUST be preserved.

#### Scenario: Developer opens an old sample specification
- **WHEN** its rules require no-LLM completion under a frozen historical identity
- **THEN** its scope notice routes new work to this change and the current product/acceptance documents, without altering the historical result

### Requirement: Evidence support and report coverage are separate decisions
Only genuinely verified claims and valid dependencies MUST enter reliable query/export; evidence location, semantic support and omission coverage MUST remain separate decisions. The v9 workpaper may be saved and rendered as pending directly from selected source text without a candidate-free inventory call or fixed verification order. Formal company acceptance still requires independent review of material source obligations and unresolved dependencies under its applicable contract. For frozen v8 execution only, selected cross-chapter evidence precedes synthesis and bounded support/coverage groups; that legacy orchestration MUST NOT become a prerequisite to the v9 workpaper.

#### Scenario: The current request supplies exact source references
- **WHEN** an explicitly versioned task supplies candidate-free source text with deterministic references bound to its report version, physical pages and exact source ranges
- **THEN** the model may select supplied references rather than rewriting page numbers and exact quotes, and the existing owner restores continuous source wording and necessary headings, units and continuation context
- **AND** unknown, out-of-request or wrong-version references are rejected, historical protocols and failures remain unchanged, and a valid reference alone never establishes semantic support or major-condition coverage

#### Scenario: Identical totals appear under different tables
- **WHEN** two supplied source ranges contain identical total text under different governing table headings
- **THEN** their source references retain distinct physical ranges, and verification still checks the selected heading, subject, period and delivered answer rather than accepting an amount match

#### Scenario: A generated reference was not actually supplied
- **WHEN** a response returns a validly formatted same-version range that is absent from the current request's provided source-reference and input-evidence catalogs
- **THEN** the owner rejects that reference rather than accepting any arbitrary substring of a sent page

#### Scenario: A source page is sliced or compacted before another stage
- **WHEN** later requests carry a fragment or compacted context from a previously sent page
- **THEN** the source reference and restored Evidence preserve the original page coordinates through an explicit range mapping, and no fragment-relative zero or rewritten text is silently treated as an original page location

#### Scenario: Source inventory is unavailable after real candidates were saved
- **WHEN** a source-led inventory call fails but located candidates and their full semantic context remain available
- **THEN** bounded independent support verification can execute without waiting for an entire source-condition list, and successful units are published before the next unit
- **AND** missing report-level review or major conditions keep company completeness false

#### Scenario: Real generated targets exceed a package verification limit
- **WHEN** the actual claims, source conditions and context no longer fit one request
- **THEN** verification is repacked using real targets, required contextual qualifications and actual dependencies; every target has a check or an explicit pending disposition
- **AND** no synthetic representative target, truncated qualifier, silent omission or raised limit substitutes for actual send admission

#### Scenario: A new development card revalidates saved unverified candidates
- **WHEN** the card explicitly permits compatible real receipts with source, model, prompt, request and version lineage
- **THEN** old candidates remain unverified until new genuine verification succeeds, original failures and charges remain unchanged, and the result is reported as development continuation rather than a fresh first-run score
- **AND** this exception does not authorize other receipts, another entrance, production or a general semantic-resume acceptance claim

### Requirement: Omission coverage is checked against delivered business content
For formal trusted company acceptance, independent source-led review MUST compare actual delivered facts and conclusions with material source disclosures, using the applicable acceptance scope and valid object states. The v9 reference workpaper MAY supply the independently read comparison scope after extraction; a separate model-generated condition inventory and per-condition repair loop MUST NOT be prerequisites to generating or displaying the workpaper. A source condition pass, when used by an existing versioned protocol, MUST bind actual verified objects and MUST NOT be inferred from source presence, a shared dimension or an unverified ID. Workpaper omissions remain explicit and do not become automatic trusted completeness or full-report recall.

#### Scenario: A second major business is missing while the source condition is correct
- **WHEN** verified claims describe one business and a source-led condition identifies another major business that is absent from those claims
- **THEN** validating the condition's source wording cannot mark that omission covered, and the company remains limited pending substantive recovery
- **AND** the comparison receives the relevant actual delivered claims or facts, rather than only conditions and source text

#### Scenario: A coverage response cites an unrelated or unverified answer
- **WHEN** a condition pass has no coverage object, cites an unknown or unverified object, or cites a different business merely sharing the same dimension
- **THEN** it cannot clear the major gap; reference checks enforce identity and state, and semantic comparison determines substantive coverage

#### Scenario: A major source condition has a repairable evidence error
- **WHEN** an explicitly authorized company task receives a major source candidate whose quotation cannot be located but relevant official text can substantiate the same business obligation
- **THEN** the original candidate, failed evidence, request identity and source scope remain traceable while one bounded company-level evidence repair produces an explicitly linked correction
- **AND** the correction must locate in actually read source context and flow through synthesis or reuse of a valid delivered object and effective support and coverage checks before that original obligation can stop blocking completion
- **AND** sharing a dimension, omitting the failed quote, silently dropping a clause or returning no correction cannot resolve the original major gap; independent source inventory remains candidate-free and is distinct from repair

#### Scenario: A supported claim names an unrepaired source condition
- **WHEN** a major source candidate remains unlocated and no valid correction or condition coverage has resolved it, but an otherwise supported claim lists its ID
- **THEN** the ID declaration alone cannot remove the major gap or complete the company
- **AND** a repaired condition must retain its original business obligations and governing source context before effective coverage against a currently valid delivered object can resolve it

#### Scenario: The first-company experiment has ended
- **WHEN** the current card permits only offline correction and closeout of saved real receipts
- **THEN** the existing entry may prove engineering corrections in isolated storage, but the original real results and charges remain unchanged
- **AND** offline success does not create newly verified business content, authorize another real entrance or move to another company

#### Scenario: Coverage verification later rejects a delivered object
- **WHEN** a valid check in the current coverage request blocks or cannot verify a previously supported claim or a native record
- **THEN** that negative result updates the object's reliability and dependent answers through the authoritative owner, and coverage that depends on it cannot remain satisfied
- **AND** an earlier pass or an existing object ID cannot preserve completeness, while unrelated verified objects remain available

#### Scenario: A source-first core package shares support and coverage verification
- **WHEN** candidate-free discovery has independently identified the package's major source conditions before its candidates are generated
- **THEN** one verification request MAY return separate support, dependency and coverage checks, with support and dependency dispositions applied before accepting coverage against those delivered objects
- **AND** identical versioned source scopes do not require another full-page inventory solely because support checking has completed; new source scopes and unresolved major risks still require independent source review
- **AND** valid existing candidates may still be independently verified and published as partial results if source discovery fails

#### Scenario: Two evidence paragraphs share a physical page
- **WHEN** a verification package contains two claims citing different paragraphs on one page
- **THEN** packing preserves both source ranges and their necessary qualifiers, deduplicating overlap by source location rather than dropping a paragraph because its page number already exists

#### Scenario: Support and coverage targets cannot fit in one verification request
- **WHEN** actual claims, source conditions and necessary context exceed a request limit
- **THEN** the owner partitions support checks and coverage comparisons without replaying the whole condition inventory for every candidate or repeatedly checking identical support solely because coverage was split
- **AND** every valid candidate receives an explicit disposition, including product and operating-role candidates; all major conditions remain covered or explicit gaps, and cross-package comparisons wait for their actual relevant delivered objects
- **AND** candidate-free source discovery, source qualifiers, native dependencies and later negative-check retraction remain mandatory; one claim in a dimension cannot stand in for another major business

#### Scenario: The current card aggregates core evidence for the whole company
- **WHEN** the card replaces repeated per-page synthesis with one company-level core delivery
- **THEN** report-wide structural discovery selects substantive scopes for all major businesses, monetization, important inputs and operating roles before supplementary details consume their resource allowance, and candidate-free source conditions precede company-view generation
- **AND** source-backed company views are generated from that collected core evidence, with support and omission coverage assessed against actual delivered objects; neither a page-order loop nor one claim in a dimension establishes completeness
- **AND** verification batches retain one current state per unique claim, a later negative check retracts affected content and coverage, and a later failed batch preserves independent verified results
- **AND** the explicitly authorized fresh experiment reuses only PDF/layout caches and valid native structured facts, rejects prior semantic inputs and does not require cross-entry semantic continuation as B1 readiness; continuation remains to be accepted before S3 if needed

#### Scenario: One sourced fact supports more than one research view
- **WHEN** a company-level source fact or operating-role statement is relevant to multiple views
- **THEN** the views may reference the same evidence and share a support-verification group without independently regenerating and rechecking synonymous facts solely because their projection differs
- **AND** every projected conclusion retains valid evidence and dependencies; no pending legacy candidate is promoted merely because a similar statement exists in another view

#### Scenario: Transport packages feed one company-level synthesis
- **WHEN** early business descriptions and later monetization, revenue or input evidence require several source transport packages
- **THEN** company-view synthesis receives their collected source-linked core evidence and necessary qualifiers, rather than merely concatenating independently generated package answers
- **AND** different sourced business claims that share a model-local claim ID remain distinct with correct verification-reference mappings; only genuinely equivalent content may be explicitly merged, and conflicts cannot be silently overwritten

#### Scenario: Reservation fits but the physical payload is oversized
- **WHEN** a request fits its combined reservation allowance but its fully serialized client payload exceeds the card's separate byte or output limit
- **THEN** it is repacked before transmission using source and evidence boundaries, preserving necessary context and all actual targets, and every resulting request is checked against all applicable limits and provider framing
- **AND** a local oversized-package refusal does not poison the cumulative budget for otherwise admissible requests; actual cumulative budget, deadline, recovery and permission exhaustion still stop further transmission, with unknown usage retained

#### Scenario: Card-specific physical timeouts share a public client
- **WHEN** a company development card requires a per-attempt timeout cap
- **THEN** the cap is enabled only by the explicit card binding or request metadata, without changing unrelated routed requests' existing deadline allocation merely because their logical timeout is at least 180 seconds

#### Scenario: A explicitly reallocates unused company-evidence resources
- **WHEN** A ends section 38 after entry 21 and issues section 39 under the user's existing execution authority
- **THEN** only entries company22 and at most one root-cause-remedied company23 share the explicitly reallocated unused ledger, wall time, physical requests and recoveries, without increasing the phase ceiling or resetting prior charges
- **AND** only entry 21 navigation may be reused after source and semantic compatibility validation; prior business conditions, drafts and verification results are excluded, and each final-version business result receives genuine reading and verification
- **AND** prior failures remain immutable, actual and unknown usage remain charged, and an incomplete result authorizes neither another entry, another company nor production

#### Scenario: A company-evidence replacement experiment is explicitly authorized
- **WHEN** A ends the exhausted twentieth-entry card and issues execution card section 38 under the user's existing execution and cost authority
- **THEN** only that card's fresh single-entry limits apply, with local versioned budget compatibility rather than expansion of previous authorizations or global defaults
- **AND** actual payloads and necessary downstream checks are measured under the frozen algorithm, unknown usage remains reserved, and an incomplete or exhausted run does not authorize another entry, short slice, company or production use

#### Scenario: An explicitly authorized company task continues across work windows
- **WHEN** the current card has obtained explicit authority for a finite number of work windows and a cumulative company budget
- **THEN** existing checkpoints continue only unfinished work under the same frozen identity while preserving cumulative charges, unknown reservations and consumed recoveries
- **AND** exhausting a company limit stops the task, pending authorization creates no real attempt, and a result requiring expanded development resources does not establish ordinary single-window resource acceptance or authorize another company

### Requirement: Progressive delivery isolates temporary failures
The company-profile owner MUST scope a transient provider or navigation failure to its current logical request and dependent evidence. A source that is temporarily failed MUST remain eligible for a later independent logical request when the existing pool health and authentication policy allow it; one historical failure MUST NOT permanently blacklist that source for the whole task. The owner MUST persist each valid non-empty synthesis result and verified group before starting the next corresponding synthesis or verification group, so later failure cannot erase unrelated reliable results. This persistence rule MUST NOT force an inventory-to-verification cycle for each source transport package. Company completeness MUST remain false while a necessary major scope is unread or unverified.

#### Scenario: A later logical request can reuse a recovered source
- **WHEN** a source has succeeded earlier, then has a transient request failure, and a later independent logical request starts while the source is healthy under the existing pool policy
- **THEN** the later request may select that source again, while the current logical request's recovery exclusion and all cumulative unknown usage, request and recovery counts remain enforced

#### Scenario: A later batch fails after an earlier batch was persisted
- **WHEN** an earlier non-empty batch has been written and independently verified, and a later navigation, synthesis or verification batch fails
- **THEN** the earlier query/export result and its new-instance read remain available, the failed scope is recorded as an explicit gap, and no unrelated claim is cleared or promoted by the failure
- **AND** an offline replacement transport may prove this persistence path but MUST NOT advance real semantic completion or company acceptance
