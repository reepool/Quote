# PDF Inspector Role Inventory

The production native chain no longer resolves `pdf-inspector`. The package is
retained in `requirements.txt` because these non-native roles still exist:

| Role | Current owner | Status |
| --- | --- | --- |
| `PdfInspectorNativeAdapter` | shared adapter module | retained for explicit evaluation/compatibility calls only; not reachable from production profiles |
| `PdfInspectorOcrAdapter` | shared adapter module | retained as an optional evaluation/OCR adapter; not selected by current profiles |
| `detect_pdf_bytes` | `research/document_processing/pdf/evaluation.py` | retained for read-only corpus classification metadata |
| `pdf_inspector` distribution | requirements/runtime | retained until classifier/evaluator/OCR owners approve a separate retirement change |

The authoritative native production route is `pypdfium2 -> pypdf`. Removing the
package in this change would silently break the remaining non-native roles, so
uninstallation is intentionally deferred.
