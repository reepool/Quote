## 1. Unseen freeze, not yet authorized

- [x] 1.1 Pass all fourteen observed companies through `delivered_ids`, freeze one service company and one manufacturing company in SSE, SZSE, BSE and instrument-id order under cutoff `2026-09-17`, the full v13 identity, fixed report versions and PDF bindings, the independent directory `reports/m4_v13_unseen_small_batch`, and the shared 50000-token budget. A second read returns the same plan, both reports are unseen, and the historical files keep their recorded bytes. This card stops at the freeze; do not enqueue. The fourteen observed ids were passed through `delivered_ids`; the freeze selected `600017.SH` (service) and `600038.SH` (manufacturing), both outside the set, plan `3a9793e0…` read back identically, and the historical snapshot bytes were unchanged.

## 2. Controlled delivery, query, and export, not yet authorized

- [ ] 2.1 After 1.1, run the existing owner service first and manufacturing second under the full v13 identity, with query and export on the same identity and both companies in one merged observation. Measure whole-round time from the first execute to the second export return; keep environment retries, record timeouts and failures honestly, and never delete a failed result in favor of a faster one. Do not start 3.1 in that task.

## 3. Independent review, not yet authorized

- [ ] 3.1 After 2.1, rebuild the recall checklist from the two new reports and judge the six dimension answer texts plus every delivered Activity, Overview, Segment, and Measurement for subject, action, object, column, amount, and unit, one finding per fact. Keep an explicit check scope for a company without commodity roles. Record measured elapsed seconds and tokens. Passing requires recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 tokens, and at most 300 seconds; a failure pauses the next batch and a pass only allows the next scope review. Do not authorize production or claim scale quality.
