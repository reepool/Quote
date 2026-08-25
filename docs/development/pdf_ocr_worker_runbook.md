# Isolated OCR Worker Runbook

For persistent local installation, use
[pdf_ocr_worker_deployment.md](pdf_ocr_worker_deployment.md) and
`scripts/deploy_pdf_ocr_workers.sh`. The commands below describe the runtime
contract and manual environment variables.

The Quote process sends rendered PNG images to `research.document_processing.pdf.ocr_worker`.
Workers never receive PDF bytes or paths. Configure:

```bash
export PADDLE_PDX_CACHE_HOME=/var/cache/quote/paddlex
export QUOTE_PDF_CPU_OCR_WORKER='/opt/quote-pdf-cpu/bin/python /opt/quote/research/document_processing/pdf/ocr_worker.py'
export QUOTE_PDF_GPU_OCR_WORKER='/opt/quote-pdf-gpu/bin/python /opt/quote/research/document_processing/pdf/ocr_worker.py'
```

The CPU and GPU environments must both use Paddle/PaddleOCR 3.3.1/3.7.0 and
the same model/inference configuration. The GPU environment uses the CUDA 11.8
wheel and must pass `--probe` before the `pdfium_paddleocr_gpu` profile can be
approved. A missing worker, failed probe, model-cache permission failure,
protocol mismatch, crash, or timeout returns a typed OCR diagnostic; it never
imports CUDA Paddle into Quote or silently falls back to unbounded OCR.

The worker must be started by its file path as shown above. Do not use
`python -m research.document_processing.pdf.ocr_worker` in the isolated
environment unless that environment also contains every Quote package: Python
module startup imports the `research` package first and can fail before the
worker protocol is reached. The file entry point has no Quote package
dependency and only receives rendered PNG images.

The existing Paddle 2.6.2/OCR 2.7.3 lab environment is comparative-only. It
must not be configured as either worker because it reproduced the known
`inflateReset2`/IR failure and is not version-compatible with this contract.
