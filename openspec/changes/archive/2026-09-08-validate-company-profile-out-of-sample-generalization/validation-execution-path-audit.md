# Out-of-sample validation execution-path audit

Date: 2026-09-08

## Result

The frozen `manufacturing-materials-oos-600019-2025` sample is admitted only by
`manifest_kind=out_of_sample_validation`. The default four-report manifest keeps
its original closed sample set. Both modes call the same preparation, semantic,
provider, projection, and atomic file-bundle owners.

The validation command remains research-only and has no reachable production
write or publication path.

## Owner map

| Responsibility | Existing owner used by validation |
|---|---|
| Manifest and Evidence validation | `research.company_profile.stage5` |
| Evidence preparation | `Stage5EvidencePreparer` |
| Extract/repair/verify and disposition | `CompanyProfileSemanticService.run_task` |
| Task-specific LLM adaptation | `CommonGatewaySemanticProvider` |
| Research usage restrictions | `project_research_view` |
| Report and run status | `ManufacturingMaterialsProfileSliceService` |
| Immutable output | `Stage5RunBundleStore` |

`scripts/run_company_profile_stage5_slice.py` only parses arguments, constructs
the existing owners, enforces the validation run boundary, and forwards the
request. It does not implement sample iteration, semantic loops, disposition,
projection, benchmark, or persistence.

## Closed validation boundary

- The validation manifest must contain exactly the frozen `600019.SH` sample.
- Its schema, kind, sample identity, PDF hash, and Evidence plan version must
  match the committed freeze artifacts.
- Mixed samples, in-sample replacement, unknown kinds, schema/kind mismatch,
  and a post-output replacement marker fail before provider invocation.
- A validation semantic run rejects `--scope-id`; only the one complete report
  run authorized by this change may call the gateway.
- The original four-report mode continues to require its original four-sample
  closed set.

## Production-unreachability audit

The operator dependency path is limited to company-profile contracts, Stage 5
preparation/service/provider/projection/bundle modules, the common LLM client,
and project configuration loading. The Stage 5 bundle store writes only beneath
the explicitly supplied isolated output root.

The path imports or calls none of the following:

- production databases or approved-table repositories;
- scheduler, API, or Telegram handlers;
- legacy backfill commands;
- CommodityExposure or ValueChainRole publication;
- DCF consumers;
- Stage 6 state or reset operations.

Every manifest, prepared scope, research projection, report bundle, run bundle,
and failure manifest retains `production_authorization=not_authorized`.

## Immutable in-sample references

Recomputed on 2026-09-08:

- authority manifest:
  `00fa20045d4df037a8984cff4b56af3f19ac533f4c4b9e311e7f130bf5158687`
- authority post-run Benchmark:
  `ce417e81039a863d0c9093fd2bd2fa2bb2e12d81cd2df0f440790210e6084188`
- offline Gold 18/24 evaluation:
  `4151aed0bc75119c9d4c3338d699e5b2f4c8d30437b8e2796300d24efc1469aa`

These artifacts are references only and are not runtime record inputs for the
out-of-sample run.

## Offline verification

- Focused validation path: 4 passed.
- Expanded Stage 5 semantic regression: 196 passed.
- Ruff on changed Python files: passed.
- `openspec validate validate-company-profile-out-of-sample-generalization --strict`:
  valid.

The OpenSpec CLI emitted a best-effort telemetry DNS warning for
`edge.openspec.dev` after returning the successful local validation result. It
does not affect the contract result or the LLM gateway.
