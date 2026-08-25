# PDF Consumer Inventory

The shared owner is `research.document_processing.pdf`. Production modules
now call `PdfRouter`/`PdfParseRequest`; vendor imports are confined to shared
adapters.

| Consumer | Domain owner | Shared path | Compatibility condition |
| --- | --- | --- | --- |
| CNInfo corporate-action documents | `data_sources` | page results preserve text/hash/method/status | retain injected OCR adapter until canary passes |
| Business-profile PDF artifacts | `research` | page results preserve artifact and heading gates | `pypdf_native` remains rollback profile |
| Broker risk-control reports | `research` | page text feeds fixed-order/table parser | fail closed on partial/low-quality pages |
| Official index lifecycle | `data_sources` | page text feeds title/date parser | source row semantics unchanged |
| HKEX suspension reports | `data_sources` | page text feeds row parser | raw snapshot hash unchanged |
| Announcement classifier | `research.announcement_assets` | first-page target result | classification remains domain-owned |

The only production `PdfReader` import is in
`research/document_processing/pdf/core.py`. Benchmark-only legacy pypdf code
in `research/business_profile_pdf_benchmark.py` remains explicitly named and
does not serve production consumers.

## Rollout Evidence

The 600036.SH report is a mandatory mapping-corruption fixture. A bounded
native comparison is stored in
`docs/development/pdf_engine_evaluation_600036_20260825.json`; it does not
contain source text, only hashes and timings. PaddleOCR is installed in the
Quote environment and model files are cached outside the repository.
