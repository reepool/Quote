## Context

The immutable owner-closure replay completed all twenty reports with no transport failure and 97.5% sampled precision, but all reports remained `hold`. Source review isolated three Evidence-planning defects that are independent of the LLM contract: `000055.SZ` selected a subsidiary financial table as BusinessOverview, `920016.BJ` treated management-plan prose containing the substring `分产品` inside `部分产品` as a segment owner, and `920076.BJ` stopped a material-input scope before an adjacent page-spanning disclosure about `役后耐火材料`.

The authoritative business chain remains `ShadowEvidencePlanner` → `Stage5EvidencePreparer` → `ManufacturingMaterialsProfileSliceService`. This change must close those source-routing defects before another provider-bearing replay, without changing semantic fields, prompts, model parameters, historical bundles, or production authorization.

## Goals / Non-Goals

**Goals:**

- Reject subsidiary/associate financial-analysis tables as issuer BusinessOverview unless the same page independently contains issuer-level substantive business or operating-model prose.
- Require `segment_dimension` scopes to contain a real segment/revenue-cost owner and prevent the lexical collision `部分产品` → `分产品`.
- Keep a material-input disclosure and its adjacent page continuation in one continuous scope when the material statement crosses the page boundary.
- Rebuild the same frozen twenty-report cohort provider-free under a new plan version and publish a hash-bound correction/preparation audit.

**Non-Goals:**

- No LLM call, model comparison, prompt/schema/token/deadline change, Gold change, readiness-threshold change, or semantic adjudication.
- No mutation or reinterpretation of the owner-closure replay or another historical bundle.
- No new planner/service framework, public API, database writer, scheduler/backfill, Stage 6, commodity exposure, value-chain publication, or DCF path.

## Decisions

1. **Use closed source-shape guards inside the existing planner.** A BusinessOverview candidate is rejected when the page owns the `主要控股参股公司分析` subsidiary table shape (`公司名称`/`公司类型`/`主要业务` plus financial columns) and lacks an independent issuer-business sentence. This is narrower than rejecting every table containing `主要业务`, which would remove legitimate issuer product tables.

2. **Make segment ownership textual and boundary-aware.** The existing segment-owner expression will treat `分产品` as a heading/table phrase only when it is not the suffix of `部分产品`. `_scope_field_ids` will retain segment measurements only when the combined scope also passes the same closed segment-owner shape. This prevents selector reasons alone from manufacturing a segment owner while preserving valid `分行业`/`分产品`/`分地区`/`分部` and revenue-composition tables.

3. **Bind material page-break continuations through existing bounded range selection.** Material owner detection will recognize issuer disclosures about material substitution, reuse, recycling, consumption, and strategic reserves. When such an owner sentence crosses an adjacent physical-page boundary, the planner keeps the owner and continuation within the existing maximum three continuous pages. It does not generally append neighboring pages and does not increase request limits.

4. **Add a narrow versioned audit, not a generalized validation platform.** The current twenty-report manifest, v4 plan, and owner-closure review artifacts remain immutable inputs. A v5 plan, preparation audit, and three-case correction audit will record the exact before/after scope assignments, positive controls, hashes, zero provider calls, and `production_authorization=not_authorized`.

5. **A later replay remains a separate decision.** Passing provider-free closure proves only that the three reviewed routing defects are removed. It does not claim semantic precision, report usability, scale readiness, or permission to run another cohort automatically.

## Risks / Trade-offs

- **A page contains both a subsidiary table and valid issuer overview prose** → reject only when no independent issuer-level substantive sentence is present, and add a positive-control test.
- **Boundary-aware segment matching excludes unusual valid headings** → retain all existing governed segment alternatives and test current valid table shapes while closing only the observed `部分产品` collision.
- **Material continuation logic absorbs generic management prose** → require an issuer/material action phrase and preserve the existing three-page continuous bound; do not add unconditional context pages.
- **Provider-free success is mistaken for readiness** → audit fields explicitly record zero provider calls, no cohort replay, research-only status, and no production paths opened.
