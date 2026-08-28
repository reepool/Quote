## 1. Shared PDF contract

- [x] 1.1 Add explicit target-page propagation and verify returned page identity, page count, text hash, quality status, and diagnostics.
- [x] 1.2 Add explicit `ocr_mode` and `recovery_policy` semantics; reject `force_ocr` with empty target pages and preserve native-first default behavior.
- [x] 1.3 Add mode budgets for `toc_probe`, `section_extract`, and `table_extract`; combine request and profile budgets by minimum and enforce per-page timeout.
- [x] 1.4 Include profile, mode, policy, DPI, model version, parser config, and page number in OCR cache identity and expose cache hit/miss metadata.
- [x] 1.5 Add optional cache backend protocol (`get`/`put`) with explicit no-cache behavior when omitted.
- [x] 1.6 Persist selected candidate semantics (`none` when no usable candidate), candidate diagnostics, OCR method, confidence, model, device, elapsed time, and runtime provenance.
- [x] 1.7 Reserve optional `structured_payload`/`structured_format` for table extraction without requiring structured output in the first implementation.
- [x] 1.8 Add read-only capability and performance probes for CPU/CUDA, model warm-up, page throughput, P50/P95, and quality metrics.

## 2. Business-profile recovery flow

- [x] 2.1 Add a recovery state machine that performs native quality assessment and alternate-native recovery before creating OCR work.
- [x] 2.2 Implement bounded TOC probing using early candidate pages, stop conditions, and `toc_unresolved` status.
- [x] 2.3 Map field families to business chapters and create deduplicated section/table target pages with bounded boundary expansion.
- [x] 2.4 Consume mixed native/alternate/OCR pages only when each page has valid evidence metadata; fail closed on partial required sections.
- [x] 2.5 Persist resumable page-level OCR work and reuse successful artifacts without redownloading PDFs or repeating OCR.

## 3. Runtime and rollout configuration

- [x] 3.1 Add configurable TOC/section/table page and time budgets, OCR cache directory, and rollout status without changing the default native profile.
- [x] 3.2 Record Paddle/PDFium/PaddleOCR versions, CPU/GPU device, CUDA availability, cache path, warm-up, and page timings in evaluation reports.
- [x] 3.3 Add an explicit GPU canary profile that cannot activate unless frozen-corpus Chinese, numeric, section, latency, and resource gates pass.

## 4. Verification and canary

- [x] 4.1 Add unit tests for native-ready, alternate-native recovery, TOC early stop, TOC unresolved, section boundary expansion, cache reuse, and budget exhaustion.
- [x] 4.2 Extend a separate evaluation manifest (without modifying existing user worktree data) with the existing paths/hashes for 600036.SH, 001322.SZ, 002376.SZ, and a native baseline; record physical page count and classification.
- [x] 4.3 Confirm gold metadata in a read-only probe before adding it to the manifest: 600036 directory page 2, expected headings `第三章 管理层讨论与分析` and `3.6 分部经营业绩`; scanned/mixed target ranges and numeric/table gold must be measured, not guessed.
- [x] 4.4 Run read-only evaluation on the hash-bound 600036.SH mapping-corrupt report plus scanned and mixed fixtures; record quality and latency without source text.
- [x] 4.5 Run a 600036.SH business-profile canary that confirms目录/业务章节恢复并且不对全文 OCR，再决定是否扩大 rollout。
- [x] 4.6 Review existing PDF development changes for compatibility, keep user pre-existing modifications isolated, and document remaining PDF-team interface gaps.
