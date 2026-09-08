# Out-of-sample Evidence freeze audit

Frozen on 2026-09-08 before any extract or verify call for `manufacturing-materials-oos-600019-2025`.

## Identity and preparation boundary

- Sample: 宝钢股份 (`600019.SH`) 2025 annual report.
- PDF SHA-256: `9d910b7b6a78fe2ccae1345c8651ca2e592d83b04bc2705f51d611ec0e1545a8`.
- Plan: `evidence-plan.v1.json`, version `manufacturing_materials_oos.2026-09-08.1`.
- Exactly six chapter tasks and nine request scopes are frozen.
- The scopes reference 13 unique one-based physical pages.
- An earlier working count of 14 was arithmetic only; page 11 is not used by a bounded scope and was not added to satisfy a count.
- Every page is existing `native_text` with quality `usable`; no OCR, parser, table engine, page expansion, or automatic selector was added.
- `preparation_gap_count=0`; all selected anchors, headers, source units, footnotes, row dimensions, and continuation pages are present.
- No LLM request has been issued for this sample as of this freeze.

## Physical-page audit

Text hashes are SHA-256 of router-selected text after outer whitespace stripping.

| Page | Text hash | Method | Quality |
| ---: | --- | --- | --- |
| 9 | `de7c256b622ab83f0f689ec85bc01fae41e3228595334df7dac862adf6ced7c6` | `native_text` | `usable` |
| 10 | `9b477b3df30578107f97fc6450422c6fdfe631cc4eea2db31f84d44551d8ac0d` | `native_text` | `usable` |
| 12 | `460c80a117c3c4b6a865a8b8002eee3fad1c073a1463cb4d0fc34b7ae4d34129` | `native_text` | `usable` |
| 13 | `9501c7c3e6acb66a14b95a2934028934c18f4a2cc98386144197793c7a6fd485` | `native_text` | `usable` |
| 14 | `ce0cb3d468b74d5859ced55ae9f63759f808791bc8a3ae634dcb89f2bcccf340` | `native_text` | `usable` |
| 15 | `20450ad28ad979771c41fb29035e375203587b07bc0e4d743cfa07f1b9c127d5` | `native_text` | `usable` |
| 16 | `5acf44ff28c743e560b93f89be22f85d2c811a2cc591e81812aad6abfb4403df` | `native_text` | `usable` |
| 17 | `8f9e442d747fc62877b830caac72f6065b29414a1bfd84bdfb29fd991e9bea52` | `native_text` | `usable` |
| 22 | `eba1239e2d8189cf64d278ce8a29144000debc7ef3f80400e286acbd4302af24` | `native_text` | `usable` |
| 23 | `64b0ec56d382a27d1c728d8092df685bce4bdb384447d1ab0c804c04b1ac4884` | `native_text` | `usable` |
| 24 | `f4cbf62ab0c67ea7bdb2e7834d8a1959417fbed49ce4bc91efc528dd32e20aa0` | `native_text` | `usable` |
| 69 | `f8349fbd7dd31bdfa9f58613b2b0ad25beb991e2f7a23e3df8009b5394823a17` | `native_text` | `usable` |
| 70 | `370727cebc20e99ad889020b28ebe02f083cc941643ca97d2783f9024c03c6dd` | `native_text` | `usable` |

## Six-chapter audit

- Overview plus Activity, pages 9-10: explicit main-business and activity wording is readable; no product-use inference is pre-authored.
- Segment, pages 14-15: revenue/cost/margin headers, `百万元`, row dimensions, and the segment-definition continuation are retained.
- Operating quantity/capacity, pages 15 and 22-23: product volume, capacity/utilization, and steel-classification headers and units are retained.
- Material/energy, pages 12-13 and 24: named inputs and supply tables are retained, including the internal-recovery scrap footnote.
- Counterparty/concentration, pages 16-17 and 69-70: aggregate totals remain distinct from legal-empty names; related-party headers and subsidiary footnote are retained.
- Business regime, page 16: explicit not-applicable business change and separately disclosed consolidation events remain separate propositions.

## Frozen legal-empty and inference boundaries

- Page 17 legal-empty top-five names do not erase page 16 aggregate sales, purchase, and concentration disclosures.
- Page 16 business/product/service `not_applicable` does not erase or merge with the separately disclosed consolidation-scope events.
- Page 24 internal-recovery scrap cannot create an external purchase amount or counterparty.
- Pages 69-70 saying listed companies include subsidiaries does not pre-promote runtime facts to `consolidated_group`.
- The plan contains no Gold, expected value, semantic answer, subject conclusion, actor answer, source-verb answer, or runtime disposition.
- Preparation remains research-only with `production_authorization=not_authorized` and later accepted output limited to `accepted_for_review`.
