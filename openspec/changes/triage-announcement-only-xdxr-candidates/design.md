## Context

The daily CNInfo workflow already persists unmatched exceptional announcements in `announcement_scan_state.metadata_json`, acquires official attachments, reconciles CNInfo and TDX events, rebuilds isolated factor paths, and runs governed event-level LLM analysis when a structured `source_event_key` exists. Announcement-only items cannot enter that LLM contract, so they remain active indefinitely. The scheduler and canonical factor owner must remain unchanged, and the current versioned title classifier must remain a complete fallback.

This change is a W6 / FR-10 vertical slice. It adds an announcement-only resolution stage beside the existing structured-event stage, without extracting the broader corporate-action application-service program or creating a second canonical state machine.

## Goals / Non-Goals

**Goals:**

- Classify unmatched official announcements by reading bounded official document text when LLM processing is active.
- Preserve deterministic title routing when semantic processing is disabled.
- Aggregate related notices into one provisional event case and choose the best available primary announcement.
- Remove confidently non-XDXR cases from the active daily queue without losing their history.
- Reactivate inactive cases when later CNInfo, TDX, or factor-path evidence appears.
- Keep model failures and ambiguity conservative and operationally visible.

**Non-Goals:**

- Do not fabricate a CNInfo `source_event_key` or source observation.
- Do not let announcement-only LLM output create, promote, or alter adjustment factors.
- Do not replace structured CNInfo/TDX reconciliation or existing event-level semantic resolution.
- Do not build a generic document-classification platform or migrate the broader corporate-action lifecycle.
- Do not calibrate production thresholds from live writes in this change.

## Decisions

### Use an explicit three-mode contract

Add `disabled`, `shadow`, and `active` modes. `disabled` does not instantiate or call the announcement-only LLM and derives the active queue solely from the current deterministic title policy. `shadow` records classifications but cannot change queue membership. `active` may move a case between active, uncertain, and inactive-watch states.

An LLM execution failure is not equivalent to `disabled`: failures retain the case as active and make semantic execution partial. This keeps intentional fallback behavior distinct from an unavailable dependency.

Alternative considered: one boolean. It cannot support a safe calibration rollout and makes it difficult to distinguish disabled behavior from failed behavior in reports.

### Persist provisional cases in existing scan metadata

Store bounded `announcement_xdxr_cases` in the existing per-exchange announcement scan metadata. A case has a stable ID, instrument, action family, first/latest announcement dates, announcement identities, selected primary/supporting keys, semantic disposition, likelihood, confidence, lineage hash, and source-reactivation state.

The case ID is derived from instrument, action family, and the first announcement key. A new announcement joins the most recent non-terminal case for the same instrument and family within a bounded association horizon; otherwise it starts a new case. This identity is internal workflow state and is never written as a CNInfo event identity.

Alternative considered: a new database table. Existing metadata already owns this queue and supports atomic replacement; a migration is unnecessary for the current bounded daily workload.

### Process one case with a bounded announcement bundle

Deterministic title-role ranking selects a bounded evidence bundle, preferring correction/supplement notices, implementation and record-date notices, completion notices, then plans and resolutions. The official document loader reuses stored corporate-action document bundles or archives and parses the source attachment.

One LLM request receives the case and its bounded announcements and returns one case disposition, XDXR likelihood, judgment confidence, event stage, action family, primary announcement key, supporting keys, and rationale. A later, better announcement updates the same case and supersedes the prior primary; it does not create another event or independently process every notice.

The primary is not simply the latest document. A correction overrides only information it explicitly addresses, while an implementation notice may remain the best source for terms omitted from a completion notice.

Alternative considered: classify each announcement separately and merge scores. That duplicates model work and can create contradictory event decisions.

### Use two thresholds and retain an inactive watch

`active` mode applies thresholds only when judgment confidence meets the configured confidence floor:

- likelihood at or above the high threshold remains active as probable XDXR;
- likelihood at or below the low threshold becomes inactive watch;
- middle likelihood or insufficient confidence remains uncertain and active.

Model-provided scores are routing scores, not calibrated statistical probabilities. `shadow` deployment and historical fixtures validate threshold behavior before operators enable active routing.

Inactive watch removes daily retry pressure but retains the complete case. No time-based silence alone terminalizes a case.

### Reactivate from authoritative later evidence

New or changed CNInfo event instruments, TDX event instruments, and material factor-path reconciliation evidence form a bounded reactivation set. An inactive case for such an instrument returns to active review and is reclassified with the new source context. Association is case-centric and does not create one event per announcement.

When a real structured CNInfo event later exists, the existing structured-event workflow remains authoritative. Announcement-only case state supplies candidate announcement lineage, not economic terms or canonical writes.

### Keep orchestration and reporting on the existing job

The existing daily job passes mode, profile, thresholds, case cap, and evidence-bundle cap into the current maintenance command. A dedicated source-domain service owns grouping, document loading, LLM schema validation, and routing decisions; `DataManager` only supplies current scan/source context and persists the returned queue projection. The scheduler remains a parameter adapter and report formatter.

## Risks / Trade-offs

- **[LLM score is poorly calibrated]** -> Start in `shadow`, keep a two-threshold uncertainty band, and validate against historical known XDXR/non-XDXR cases.
- **[Two unrelated actions share one broad family]** -> Bound association by instrument, family, and time; start a new case outside the horizon and keep announcement identities auditable.
- **[Completion notice omits economic terms]** -> Keep primary and supporting evidence distinct; do not force the latest notice to replace a more complete implementation notice.
- **[Document download or parsing fails]** -> Retain the case active, report the failure, and do not apply a low-likelihood exit.
- **[TDX/factor correction causes a false reactivation]** -> Reactivation only reopens semantic review; it cannot create or promote an event by itself.
- **[Scan metadata grows]** -> Persist only bounded normalized case lineage and capped rationale; full announcement/document evidence remains in existing audit and document stores.

## Migration Plan

1. Deploy with `announcement_xdxr_llm_mode=shadow` and existing title behavior unchanged.
2. Verify representative non-XDXR, probable XDXR, multi-announcement supersession, and source-reactivation fixtures.
3. Observe scheduler reports and compare shadow decisions with deterministic queue membership.
4. Enable `active` only after threshold review; rollback immediately by setting mode to `disabled`.
5. Preserve stored cases across mode changes; `disabled` ignores LLM suppression and re-applies deterministic title routing.

## Open Questions

None for this change. Production threshold calibration can adjust configuration without changing the case or canonical contracts.
