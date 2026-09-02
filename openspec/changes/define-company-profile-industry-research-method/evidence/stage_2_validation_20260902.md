# Stage 2 research-contract validation

> Date: 2026-09-02
> Change: `define-company-profile-industry-research-method`
> Scope: research documentation and templates only

## Scenario review

| Requirement scenario | Evidence | Result |
|---|---|---|
| New industry research instantiates mandatory artifacts | research method §3 and four published templates | pass |
| Focus report cannot define the contract | method §4 requires >=3 reports, >=2 companies and >=2 challenging non-focus reports | pass |
| No representative exchange issuer | method §4/§10 and sample manifest `coverage_gaps` prohibit irrelevant padding | pass |
| Different section numbering | method §5 and requirements template §4 use chapter family × task and observed aliases | pass |
| Conditional customer disclosure | method §6 and requirements template §5 require task activation before coverage | pass |
| Required page unreadable | method §7 requires `extraction_failed`, never `not_disclosed` | pass |
| Reviewer disagreement | method §7 and gold manifest `review_log` retain both positions; unresolved is `unclear` | pass |
| High average accuracy with silent required omission | method §8 and acceptance template §5 force `hold` | pass |
| Stage transition remains research-only | method §11 requires named roles, proposed real sample manifest and explicit no-production authorization | pass |

## Terminology review

The method §9 maps objects, `requirement_level`, `coverage_status`, assertion class, subject scope, period, unit ownership, chapter tasks, evidence and business regime directly to the master requirements. The templates reuse those names and do not introduce competing business enums.

## Scope review

Files delivered by this implementation are limited to:

- `docs/README.md` and `docs/development/README.md`;
- the authoritative company-profile master requirements industry register;
- stage-2 research-method and template documents;
- this OpenSpec change's tasks and validation evidence.

No file under `research/`, no `data_manager.py`, scheduler, Telegram adapter, database, migration or production configuration is modified by this change. No LLM or annual-report batch is executed.

## Validation commands

- JSON parsing for both manifest templates: pass.
- Artifact, terminology, `research_contract_only`, and concrete-industry `not_researched` checks: pass.
- `openspec validate define-company-profile-industry-research-method --strict`: pass (telemetry upload failure did not affect exit status).
- `git diff --check`: pass.

## Stage conclusion

Stage 2 artifacts are complete and internally consistent. This conclusion authorizes only creation of the separate stage-3 research change `research-manufacturing-materials-profile-package`; it does not authorize concrete industry conclusions or production implementation.
