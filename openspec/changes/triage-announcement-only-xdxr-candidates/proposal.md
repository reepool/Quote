## Why

The daily A-share corporate-action workflow keeps exceptional CNInfo announcements pending indefinitely when no structured `source_event_key` exists, even when the full announcement clearly describes a non-XDXR transaction. Title enumeration cannot reliably cover new wording, while processing every related announcement independently can duplicate one economic event and select inferior evidence.

## What Changes

- Add configurable `disabled`, `shadow`, and `active` announcement-only LLM modes; `disabled` preserves the current versioned deterministic title policy as the authoritative fallback.
- Give unmatched announcements a governed announcement identity and group related notices into one provisional corporate-action case without fabricating a CNInfo source event.
- Classify one case, not each announcement, using XDXR likelihood and judgment confidence with configurable high/low thresholds.
- Select one primary announcement plus supporting evidence, and supersede the primary when a more authoritative implementation, completion, correction, or source-backed notice arrives.
- Move low-likelihood cases out of the active daily queue while retaining a durable inactive watch record that CNInfo, TDX, or adjustment-factor changes can reactivate.
- Keep uncertain or failed semantic work conservative, bounded, auditable, and unable to alter raw observations or canonical factors.
- Extend daily results and reports with mode, case, classification, inactive-watch, reactivation, and failure counters.

## Capabilities

### New Capabilities

- `announcement-only-xdxr-semantic-triage`: Event-centric semantic classification, multi-announcement evidence selection, inactive watch, and source-triggered reactivation for announcements without structured CNInfo event keys.

### Modified Capabilities

- `scheduler`: Configure announcement-only semantic mode and thresholds on the existing A-share corporate-action daily job and report its event-centric outcomes without changing the job identity.

## Impact

- Affects CNInfo announcement candidate classification, daily semantic orchestration, announcement persistence metadata, and A-share corporate-action daily reporting.
- Reuses the official announcement acquisition/assets and common LLM client; no new external dependency or alternate scheduler job is introduced.
- Preserves the current deterministic title policy when LLM processing is disabled and preserves existing structured-event semantic governance.
- Does not create synthetic CNInfo observations, does not let LLM output write canonical factors, and does not change adjustment-factor formulas, public APIs, database paths, or scheduler identity.
- Maps to corporate-action application lifecycle workstream W6 / FR-10 and remains compatible with `extract-corporate-action-application-services` by keeping canonical promotion ownership unchanged.
