## 1. Confirm Baseline and Acceptance Evidence

- [ ] 1.1 Confirm the in-flight shared page-recovery contract is implemented or reconcile this change against its final request, candidate, budget, cache, and provenance models before changing router behavior.
- [ ] 1.2 Create a read-only, hash-bound evaluation manifest for 600036.SH, 001322.SZ, 002376.SZ, and 000717.SZ with explicit physical pages and native fidelity/read-order checks; do not modify production PDF assets.
- [ ] 1.3 Add a reproducible native benchmark that runs identical selected pages through `pypdfium2` and `pypdf` and records engine versions, text hashes, quality status, Chinese/numeric gold results, page order, table/read-order observations, and elapsed time.

## 2. Deliver PDFium Native Extraction

- [ ] 2.1 Implement the `pypdfium2` native-text adapter with one-based physical pages, page count, deterministic text/hash output, engine version, elapsed time, and typed open/page/extraction failures.
- [ ] 2.2 Add focused adapter tests for normal text, malformed mapping recovery, scanned/empty pages, out-of-range pages, stable ordering, and no OCR side effects.
- [ ] 2.3 Run the frozen-corpus benchmark and write the versioned selection result; promote PDFium only if every mandatory fidelity and compatibility gate passes before applying the latency comparison.

## 3. Migrate the Native Router and Profiles

- [ ] 3.1 Replace hard-coded primary/alternate native routing with a validated ordered engine tuple that short-circuits per page at the first technically usable candidate.
- [ ] 3.2 Preserve each attempted engine's name/version, method, hash, quality, usability, diagnostics, and elapsed time, and expose the actual selected native engine in page provenance.
- [ ] 3.3 Version cache identity and serialization by native chain/order/configuration and relevant engine versions; add tests proving profile-order or engine-version changes cannot hit incompatible cached results.
- [ ] 3.4 After PDFium passes promotion gates, configure the canary and then production native profile as `pypdfium2 -> pypdf`, retaining a configuration-only rollback to the prior order.
- [ ] 3.5 Add router tests proving `native_first` never creates OCR work, `selective_recovery` reaches OCR only after every native engine fails for that requested page, and bounded `force_ocr` semantics remain unchanged.

## 4. Retire pdf-inspector Native Paths

- [ ] 4.1 Inventory production imports, profiles, configuration, supported operator tools, and tests for `pdf-inspector`, and document any real capability that still blocks removal.
- [ ] 4.2 Migrate remaining supported callers to the ordered native chain and remove `pdf-inspector` native profiles and adapter wiring after focused integration tests pass.
- [ ] 4.3 Remove the `pdf-inspector` package dependency only when the verified caller inventory is empty, then run dependency/import smoke tests.

## 5. Deliver the Isolated GPU OCR Worker

- [ ] 5.1 Define and test the versioned bounded-page worker protocol, including physical page, input reference, settings/deadline, text/hash/confidence, quality status, diagnostics, elapsed time, and engine/model/device/runtime versions.
- [ ] 5.2 Provision a version-pinned GPU worker environment outside the Quote conda environment using the P4-compatible Paddle GPU, PaddleOCR, PP-OCRv4, CUDA, writable persistent model cache, and tested IR-fusion setting; record exact versions and reproducible setup in the runbook.
- [ ] 5.3 Implement worker startup and capability reporting for device visibility, driver/CUDA compatibility, compute capability, memory, model health, required inference settings, and available GRID licence status.
- [ ] 5.4 Implement the Quote-side GPU worker adapter with protocol validation, startup/inference timeout handling, process-crash isolation, cancellation, and typed diagnostics without adding another PDF business entry point.
- [ ] 5.5 Add worker and adapter tests for valid output, malformed/mismatched output, startup failure, model failure, timeout, crash, empty OCR result, and partial multi-page completion.

## 6. Add GPU-First and CPU-Fallback Routing

- [ ] 6.1 Add validated OCR runtime order and allowed-fallback-failure configuration with GPU-first production, CPU-only rollback, and OCR-disabled profiles.
- [ ] 6.2 Enforce one shared effective page/request budget across GPU and CPU attempts, retain completed pages, and prevent fallback from expanding target pages or resetting deadlines.
- [ ] 6.3 Extend OCR provenance and cache identity with runtime/device, Paddle/PaddleOCR, model, and inference-configuration versions; test that GPU and CPU results cannot collide.
- [ ] 6.4 Add integration tests for healthy GPU selection, permitted pre-inference CPU fallback, default no-retry after GPU page timeout, exhausted-budget behavior, bounded `force_ocr`, and no OCR after usable PDFium text.

## 7. Canary, Rollback, and Closeout

- [ ] 7.1 Run the isolated GPU worker on the hash-bound mapping-corrupt, scanned, mixed, and normal baseline corpus and record fidelity, selected pages, failures, cache behavior, per-page/P95 latency, and GPU-versus-CPU comparison.
- [ ] 7.2 Verify the production-host canary meets selective-page, output-fidelity, typed-failure, and latency gates before enabling GPU-first OCR; confirm 600036.SH is not sent to full-document OCR.
- [ ] 7.3 Exercise configuration-only rollback to `pypdf`-first native routing and CPU-only/OCR-disabled recovery, confirming caller APIs and native parsing continue to work.
- [ ] 7.4 Run focused shared-PDF and integrated caller regression suites, update the current PDF module documentation and GPU operations runbook, and record the worker/model-cache and GRID licence renewal owners.
- [ ] 7.5 Review the final change for duplicate PDF execution paths, obsolete profiles/dependencies, unbounded OCR, and unrelated scope; resolve only blocking findings and archive the completed OpenSpec change.
