## ADDED Requirements

### Requirement: GPU-first OCR runtime routing
The shared PDF module SHALL support an ordered OCR runtime configuration whose production target uses GPU PaddleOCR first and whose CPU runtime is an explicit configurable fallback. A CPU-only profile SHALL remain available for rollback or hosts without a qualified GPU.

#### Scenario: Qualified GPU is healthy
- **WHEN** a page is eligible for OCR and the configured GPU worker passes capability and model-health checks
- **THEN** the module sends the bounded page job to the GPU runtime before considering CPU

#### Scenario: CPU-only rollback profile is selected
- **WHEN** operations select the CPU-only OCR profile
- **THEN** OCR runs without attempting to initialize or contact the GPU worker

### Requirement: Isolated Paddle GPU worker
GPU Paddle and PaddleOCR dependencies MUST run outside the Quote conda environment in a version-pinned local worker process or environment. The worker SHALL implement a versioned bounded page protocol and SHALL NOT own PDF business selection, cache persistence, recovery policy, or final result assembly.

#### Scenario: Worker returns a valid page result
- **WHEN** the Quote process submits a page number, rendered input or input reference, OCR settings, and deadline
- **THEN** the worker returns the protocol version, physical page number, text, text hash, confidence, quality status, elapsed time, engine/model/device/runtime versions, and diagnostics

#### Scenario: Worker response is malformed
- **WHEN** the worker omits a required field, returns a mismatched page number, or returns an invalid hash
- **THEN** the shared module rejects the response as a typed worker-protocol failure and does not select its text

### Requirement: GPU capability and model health gate
Before receiving production OCR work, the GPU worker SHALL report and validate device visibility, driver and CUDA compatibility, compute capability, available memory, model load health, runtime versions, and the required P4 inference settings. Licence status SHALL be reported when exposed by the host tooling.

#### Scenario: P4 worker passes canary
- **WHEN** the isolated worker loads the pinned model on GRID P4, uses the validated inference settings, and completes the frozen canary within its fidelity and latency gates
- **THEN** the GPU runtime is marked available for production-profile routing

#### Scenario: Incompatible IR path or model load fails
- **WHEN** worker startup detects an unsupported optimization path, native crash, or model load failure
- **THEN** the worker is marked unavailable with a typed diagnostic and no OCR page is silently reported as successful

### Requirement: Budget-preserving CPU fallback
CPU fallback SHALL occur only when enabled, when the GPU failure class is allowed by the configured fallback policy, and when the page and effective request budgets still permit another attempt. GPU and CPU attempts SHALL consume the same effective page/document budgets, and completed pages SHALL be retained if another page fails.

#### Scenario: GPU is unavailable before inference
- **WHEN** the GPU worker fails capability, startup, availability, or model-health checks, CPU fallback is enabled for that failure class, and budget remains
- **THEN** the page is attempted once with the configured CPU runtime and both runtime attempts are recorded

#### Scenario: GPU page times out under default policy
- **WHEN** GPU inference exceeds the page deadline and timeout is not an allowed CPU fallback class
- **THEN** the page returns `ocr_timeout` without silently repeating the page on CPU

#### Scenario: No budget remains for fallback
- **WHEN** a GPU failure is eligible for CPU fallback but the page deadline, mode budget, or profile budget is exhausted
- **THEN** the module returns the typed GPU failure and does not start a CPU attempt

### Requirement: Device-specific OCR provenance and cache identity
Every OCR candidate SHALL report runtime, device class and identifier, Paddle and PaddleOCR versions, model name/version, confidence, text hash, elapsed time, and diagnostics. Cache identity MUST distinguish GPU and CPU runtime/device/model/configuration combinations that can affect output.

#### Scenario: CPU follows failed GPU startup
- **WHEN** CPU fallback produces the selected OCR text after a GPU startup failure
- **THEN** the page result identifies CPU as selected, retains the GPU failure provenance, and uses a CPU-specific cache identity

#### Scenario: Runtime or model version changes
- **WHEN** the GPU runtime, OCR engine, model, device class, or inference configuration version changes
- **THEN** results produced by the new combination do not reuse incompatible cached page results

### Requirement: OCR remains bounded recovery
The OCR runtime router SHALL preserve the shared recovery contract: `native_first` creates no OCR work, `selective_recovery` sends only requested pages for which all configured native candidates fail, and `force_ocr` requires non-empty explicit target pages. Runtime fallback MUST NOT enlarge target pages or reset the effective budget.

#### Scenario: PDFium recovers a malformed mapping page
- **WHEN** an earlier native adapter fails but PDFium returns usable native text under `selective_recovery`
- **THEN** neither GPU nor CPU OCR is invoked for that page

#### Scenario: Force OCR omits target pages
- **WHEN** a caller requests `force_ocr` with empty `target_pages`
- **THEN** the request is rejected before either OCR runtime starts

#### Scenario: GPU fallback cannot trigger full-document OCR
- **WHEN** GPU initialization fails during a request with a bounded target-page whitelist
- **THEN** CPU fallback, if allowed, is restricted to the same eligible target pages and remaining budget

### Requirement: GPU OCR production canary and rollback
The GPU-first profile SHALL NOT be enabled until a production-host canary on the hash-bound scanned, mixed, mapping-corrupt, and normal baseline corpus passes output fidelity, typed-failure, cache separation, selective-page, and latency gates. Rollback SHALL be possible by selecting the CPU-only profile or disabling OCR recovery without changing business callers.

#### Scenario: Canary passes
- **WHEN** the isolated GRID P4 worker meets the agreed selected-page fidelity and P95 latency gates without unbounded OCR or undiagnosed failures
- **THEN** operations may enable the GPU-first production profile

#### Scenario: Canary or runtime health regresses
- **WHEN** the GPU canary fails, driver/model health changes, or production latency exceeds the rollback gate
- **THEN** operations can switch to CPU-only or OCR-disabled configuration while native parsing and caller APIs remain unchanged
