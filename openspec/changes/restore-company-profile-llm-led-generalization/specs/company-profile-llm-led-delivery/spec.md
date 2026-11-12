Current policy: product v9 (2026-11-11), acceptance section 7.109 and company-profile-chapter-workpaper-card.md govern new work. The sourced manual reference and original-owner extraction path have been delivered; the user's continued-development authorization now permits the bounded report observations in that card, including optional supply-chain enrichment and actual-model attribution. Use each report's TOC/headings to build concentrated complete section contexts, extract one versioned eight-element JSON projection and render it through existing owners. Source-first means original-text-grounded discovery and independent review, not a mandatory inventory/claims/checks call sequence before a draft can exist. Historical named policies, stages and execution scenarios below apply only to their frozen identities; none revive exhausted previews or override the v9 workpaper requirements. Formal trusted release, historical scores and production boundaries are unchanged; updating a target spec does not mean implementation is complete.

## ADDED Requirements

### Requirement: Metric standardization preserves source meaning and audit history
New workpaper binding and repaired projections MUST clear currency on dimensionless ratios. A rate difference expressed as percentage points MUST remain distinct from relative percentage growth. Standardization MAY retain a source_unit beside the normalized unit, MUST preserve the raw source spelling and decimal value, and MUST leave an auditable note. It MUST NOT guess currency conversions, magnitudes, subject scope or rate-change meaning from a bare number. Historical workpaper files MUST NOT be rewritten by reading them.

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

#### Scenario: A review identifies a scope limitation without a fact ID
- **WHEN** an otherwise valid review flags limitations or an element-level ambiguity without an affected fact
- **THEN** that finding MUST remain unresolved and visible without discarding independent valid findings or inventing a new fact
- **AND** a limitation correction MUST NOT become a fact repair or approve release; disputed scope assertions are withheld in the limited projection while the original stays auditable

#### Scenario: A repaired amount still uses the wrong financial column
- **WHEN** a patch quotes a locatable amount but final source review rejects its metric meaning
- **THEN** the patch is not accepted, the affected fact remains withheld and independent business facts stay readable
- **AND** neither shared-dimension membership nor absence of a new issue alone resolves the original issue

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

### Requirement: Reading questions guide report-local section selection
The selector MUST receive a versioned general question-to-section-family guide inferred from reviewed reports. Business and operating-mode subsections are primary sources; revenue/cost and business review sections provide main segment weights; procurement/production/sales, product applications and customer sections may enrich supply chains. Financial or segment notes are targeted supplements, while honors, governance and minor technical details are deferred. Families are semantic hints, never exact heading/page whitelists. Numbered decimal headings such as 3.6 and 3.6.2 MUST preserve their report-local hierarchy so useful subsections can be selected without reading an entire management chapter. Extraction MUST prioritize a supported main segment's revenue or operating scale before minor organization/technology details; unknown values remain unknown.

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

#### Scenario: A metric has separate header and amount spans
- **WHEN** the amount and its governing table header occupy separate source spans or continuation pages of the same identifiable table
- **THEN** linked Evidence retains the row/column, subject, period and unit as one traceable support context
- **AND** the program does not guess the column, require one unnecessarily long quotation or accept an amount merely because an ID exists

#### Scenario: Some elements are missing
- **WHEN** a selected section packet supports the main business and products but does not support a key input or customer detail
- **THEN** the workpaper publishes the supported facts with explicit missing scope/reason and no fabricated value
- **AND** eight populated keys or successful JSON parsing alone establishes neither content completeness nor a passing company score

#### Scenario: Human reference is rendered for review
- **WHEN** a human_reference JSON workpaper is used to check presentation and local mapping
- **THEN** the rendered facts, metrics and evidence remain the same as the JSON and their origin remains visible
- **AND** this does not change historical model results, authorize a call or claim llm_generated completion

### Requirement: Cost-aware section work stops without fabricating completeness
The v9 workpaper path MUST optimize useful business content per reading and model cost, not perfect appendix coverage. It MUST record the chosen source set and why the scope stopped, using a high-value section pack followed only by targeted material-gap recovery. Actual and unknown resource use MUST follow the current explicit execution authority; no complete-report hypothetical synthesis/verification reserve may be imposed as a prerequisite to a pending workpaper. This does not waive the real request/context/output limits or cumulative budget, recovery and permission caps. Formal evaluation thresholds remain versioned separately from basic workpaper display.

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
