## 1. Baseline And Classification

- [ ] 1.1 Inventory every `docs/` file with title, type, current owner, inbound references, related OpenSpec change, and proposed disposition.
- [ ] 1.2 Record the current code/configuration evidence for architecture, databases, public entry points, and scheduler job categories.
- [ ] 1.3 Identify same-capability document families and assign one target current document and any necessary runbook for each family.
- [ ] 1.4 Identify complete OpenSpec changes and record active cross-change references that block archive.
- [ ] 1.5 Partition status-complete changes into reviewable archive batches and record each batch's dependency scan; do not treat the full backlog as one bulk deletion.

## 2. Current Documentation

- [x] 2.1 Finalize `project_development_governance.md`, `framework_refactoring_program.md`, and their mandatory AGENTS references.
- [x] 2.2 Rewrite `docs/architecture.md` from current code/configuration, including modular boundaries, databases, entry points, and canonical paths.
- [x] 2.3 Replace the historical development guide with the concise current developer entry and verification workflow.
- [x] 2.4 Rebuild `docs/README.md` as the only current index and add lifecycle labels or sections that distinguish current, runbook, and active requirements.

## 3. Capability Consolidation

- [ ] 3.1 Consolidate quote download, historical backfill, single-instrument, and gap runbooks; verify every retained command against current entry points.
- [ ] 3.2 Consolidate instrument-master and A-share corporate-action/factor document families without changing domain rules.
- [ ] 3.3 Consolidate LLM, backup, and stable research-domain document families, deferring any family with an active overlapping change.
- [ ] 3.4 Delete each superseded document only after recording its replacement, preserved rules, reference scan, and rollback source commit.
- [ ] 3.5 Include root-level `implementation_plan.md` in the disposition matrix and absorb or retire it only after scanning repository and operator references.

## 4. OpenSpec And Validation

- [ ] 4.1 Archive status-complete OpenSpec changes whose durable requirements are current and whose live artifacts have no active dependency.
- [ ] 4.2 Run Markdown link/path checks and repository reference scans for all deleted or renamed documents.
- [x] 4.3 Verify production code, configuration, scheduler catalog, and database files are unchanged by the documentation-only change.
- [ ] 4.4 Update `framework_refactoring_program.md` W1 status and document unresolved cleanup candidates with concrete blockers.
- [ ] 4.5 Record the completed `triage-announcement-only-xdxr-candidates` change's archive decision separately from the broader W6 implementation baseline.
