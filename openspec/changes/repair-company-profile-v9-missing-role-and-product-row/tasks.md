## 1. Chemical feedstock oil internal sale, not yet authorized

- [ ] 1.1 After scope acceptance, extend the existing sentence binding so 部分化工原料油内部销售给化工事业部 delivers 化工原料油 with the refining-division subject, the 部分 internal-sales scope, and 化工事业部 as receiver; keep the value empty and the catalog-pending status. Verify acceptance, query, and export. Do not start 2.1 in that task.

## 2. Wrapped product row and acceptance chain, not yet authorized

- [ ] 2.1 After 1.1, make the in-table label join tolerate in-column spaces so the product/电力及热力 row is restored with its original amount and unit, and separate column contexts in the reconciliation slot so the industry revenue Measurement stays accepted while the product record is added. Verify the revenue regression: no occurrence_semantic_conflict for either record. These changes protect business results directly and belong to this same card. Do not start 3.1 in that task.

## 3. Unified-identity isolated review, not yet authorized

- [ ] 3.1 After 2.1, fix the review rules before observation and rerun the two frozen reports under the new `revenue_sentence_repair=v10` identity in its own directory; query and export with the same identity. Do not write v9 directories or prefill scores in that task.
- [ ] 3.2 After 3.1, score recall from the source under the fixed rules, map every dimension answer, role, Segment, Measurement, and ordinary Activity to its own finding, measure whole-round elapsed time, and compute the fixed thresholds. Keep a miss as a miss. Do not authorize production, do not claim scale quality, and do not freeze the next unseen batch in that task.
