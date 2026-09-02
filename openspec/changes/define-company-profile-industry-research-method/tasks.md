## 1. Publish the stage-2 research artifacts

- [ ] 1.1 Create `docs/development/company_profile_industry_research_method.md` defining research roles, report-selection workflow, chapter-family reading, field-checklist construction, annotation review, benchmark acceptance, coverage-gap handling, and the stage-3 entry gate.
- [ ] 1.2 Create `docs/development/company_profile_industry_requirements_template.md` with every mandatory industry-contract section and field-level checklist column required by the spec.
- [ ] 1.3 Create versioned sample and annotation manifest templates for report identity, selection dimensions, business regime, chapter tasks, source-native values/units, physical evidence anchors, coverage states, reviewer disagreements, and explicit coverage gaps; mark them `research_contract_only` rather than production schemas.
- [ ] 1.4 Create `docs/development/company_profile_industry_benchmark_acceptance_template.md` covering field/chapter metrics, blocking failures, legal empty cases, unsupported inference, uncovered boundaries, reviewer sign-off, and the pass/hold decision.

## 2. Connect governance and stage boundaries

- [ ] 2.1 Update `docs/README.md`, `docs/development/README.md`, and the master company-profile requirements industry register so the stage-2 method/templates are discoverable while every concrete industry remains `not_researched`.
- [ ] 2.2 Add a terminology and requirement mapping that proves the templates use the master contract definitions for objects, `requirement_level`, `coverage_status`, assertion class, subject scope, period, unit ownership, chapter tasks, evidence, and business regime without introducing competing enums.
- [ ] 2.3 Add an explicit stage-3 entry checklist requiring reviewed templates, a proposed real-report sample manifest, named research owner/reviewer, and no production-code or LLM execution authorization.

## 3. Validate the research contract

- [ ] 3.1 Review the artifact set against every requirement scenario, including focus-sample bias, unavailable exchange coverage, differing section numbers, conditional customer disclosure, unreadable required pages, reviewer disagreement, and high average accuracy with a silent required-table omission.
- [ ] 3.2 Run strict OpenSpec validation and document that this change modifies only research documentation/templates and does not alter `research/`, `data_manager.py`, scheduler, Telegram, databases, or production configuration.
- [ ] 3.3 Perform a focused final review, resolve only blocking contract inconsistencies, and leave manufacturing/materials field research to the separate stage-3 change.
