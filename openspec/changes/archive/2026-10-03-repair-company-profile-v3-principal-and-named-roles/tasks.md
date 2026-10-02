## 1. Source-sentence repair

- [x] 1.1 Keep the company business sentences on `600008.SH` page 10 and `600019.SH` page 9. Exclude “销售模式”, “销售区域”, and “销售部” by sentence, while keeping the page 15 sales-volume row for “其他钢铁产品”. Deliver scrap only from page 24 “国内采购”, and keep page 69 energy medium in both directions under its source name without splitting a mixed amount. Same-page dedup is name plus action. Prove acceptance, query, and export. Do not run a successor or change the recorded 6/11 review in this task.

## 2. Isolated successor

- [x] 2.1 After 1.1, rerun the frozen `600008.SH` and `600019.SH` reports with the new repair marker and a new snapshot directory. Keep cutoff `2026-09-17`, the shared 50000-token budget, and the measured-duration rule. Query and export must select that identity. Leave this round and the historical artifacts unchanged.

## 3. Independent review

- [x] 3.1 After 2.1, deduplicate recall by the source checklist and score each delivered record once. A checklist hit MUST NOT add another accuracy sample. Check subject, direction, source name, and evidence, then record the score and the gate. Archive from that result. Do not authorize production or scale quality.
