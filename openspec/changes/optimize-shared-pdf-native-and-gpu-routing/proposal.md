## Why

The shared PDF module currently defaults to `pypdf`, but the 600036.SH lab corpus shows that it can preserve page access while losing almost all usable Chinese text on malformed `ToUnicode` mappings; the configured `pdf-inspector` alternate fails the same pages and is materially slower. The same lab also demonstrates that selective PaddleOCR on the available GRID P4 is practical only when GPU inference is the production path, while installing the proven GPU stack inside the Quote conda environment creates native-library conflicts.

## What Changes

- Add a `pypdfium2` native-text adapter and evaluate it against `pypdf` on the frozen native, mapping-corrupt, scanned, and mixed PDF corpus before changing the production default.
- Replace hard-coded native/alternate selection with an ordered, configurable native-engine chain that stops at the first candidate passing shared technical quality gates and preserves provenance for every attempted engine.
- Promote `pypdfium2` to the primary native engine only after it passes accuracy, page-order, table/read-order, compatibility, and latency gates; retain `pypdf` as a configurable rollout fallback.
- Remove `pdf-inspector` from native extraction profiles and remove its dependency after verifying that no production caller still requires it.
- Make GPU PaddleOCR the primary production OCR runtime behind the existing selective page-recovery contract, with an explicitly configured CPU runtime as fallback.
- Run GPU OCR in an isolated local worker environment/process, preserving page budgets, timeouts, cache identity, typed diagnostics, and provenance across the process boundary.
- Keep OCR as recovery: all configured native candidates must fail before OCR is scheduled, except for explicit bounded `force_ocr`; implicit full-document OCR remains prohibited.
- Add frozen-corpus and GPU canary evidence that supports promotion, rollback, and device-specific cache separation without changing company-profile page selection or business quality rules.

## Capabilities

### New Capabilities

- `shared-pdf-native-engine-routing`: Ordered native parser configuration, candidate selection, corpus-based primary-engine promotion, provenance, and retirement of ineffective native adapters.
- `shared-pdf-ocr-runtime-routing`: Isolated GPU-first PaddleOCR execution, explicit CPU fallback, capability checks, budgets, diagnostics, provenance, cache separation, and rollout controls.

### Modified Capabilities

None. This change builds on the in-flight `shared-pdf-page-recovery-contract` without changing its one-based physical page, selective recovery, candidate, or business-ownership semantics.

## Impact

- Affected shared code: `research/document_processing/pdf/` adapters, profiles, router, models, cache identity, capability reporting, and tests.
- Affected configuration: ordered native-engine profiles and isolated OCR worker runtime/device/fallback settings; existing external PDF request semantics remain stable.
- Affected dependencies: `pypdfium2` becomes a native extraction dependency as well as a renderer; `pdf-inspector` is removed after caller verification; GPU Paddle/PaddleOCR dependencies remain outside the Quote conda environment.
- Affected operations: a version-pinned, writable-cache GPU worker environment and canary/rollback procedure are required; GRID P4 driver/CUDA capability and licence status must be reported before enabling the GPU profile.
- Existing company-profile logic continues to own TOC/section selection, printed-to-physical page mapping, recovery state, and business quality gates. No second PDF parsing workflow or database is introduced.
