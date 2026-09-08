# BaoSteel semantic-mapping correction baseline

Frozen on 2026-09-08 before implementation or any new LLM call.

## Immutable references

- Four-report authority manifest: `00fa20045d4df037a8984cff4b56af3f19ac533f4c4b9e311e7f130bf5158687`
- Four-report post-run benchmark: `ce417e81039a863d0c9093fd2bd2fa2bb2e12d81cd2df0f440790210e6084188`
- Offline Gold 18/24 evaluation: `4151aed0bc75119c9d4c3338d699e5b2f4c8d30437b8e2796300d24efc1469aa`
- BaoSteel optimized-run manifest: `05fc6c750c643bd333ae955f58fd09b53e17987b7ac3cd66ec8f408888a70df0`
- BaoSteel optimized-run report: `dd21f159599957f1f3059455aee908d7f1a4c3fba9eade93f44eb4db7198faef`

All five files are read-only inputs for diagnosis. This change does not edit, rescore in place, or splice any of them.

## Frozen semantic targets

1. Page 22 capacity-utilization rows `铁 96%` and `坯材 96%` are distinct facts because the measured objects/source rows differ.
2. Page 16 supplier purchase amounts `951.7亿元` and `652.2亿元` must use supplier rather than customer field semantics.
3. Pages 69-70 related-party transaction amounts and purchase/service ratios are not top-five concentration facts and remain outside that checklist.
4. Page 14 `分部间抵消` remains an adjustment row, but its subject cannot be promoted to `consolidated_group` without affirmative group wording or reconciliation Evidence.

No source value, unit, period, Evidence anchor, Gold expectation, production authorization, or historical disposition may be changed to satisfy these targets.
