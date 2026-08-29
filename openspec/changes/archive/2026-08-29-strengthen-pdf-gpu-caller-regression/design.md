## Context

The shared PDF router now separates static GPU profile approval from live OCR
worker readiness. Adapter tests cover that separation, while the two affected
business callers only assert their router wiring with a mocked router. This
change closes that evidence gap without changing routing behavior.

## Goals / Non-Goals

**Goals:**

- Exercise the HKEX and official-index public PDF entry points with an approved
  GPU profile, a deliberately unavailable GPU worker, and text that native
  extraction can use.
- Prove no worker probe or OCR fallback is reached on the native-only path.
- Make the operating guide match the existing static-versus-live readiness
  contract.

**Non-Goals:**

- Changing OCR routing, fallback behavior, approval rules, or worker protocol.
- Requiring a physical GPU, CUDA package, or external OCR worker in unit tests.
- Changing business outputs, public request models, profile names, or
  environment variables.

## Decisions

### Test real caller construction with controlled shared adapters

The tests will invoke each public caller with `pdfium_paddleocr_gpu`, configure
a valid temporary static approval artifact, and set a GPU worker command that
would fail if called. They will retain the real `resolve_profile()` and
`build_router()` path, while controlling the native worker result at the shared
adapter boundary so the test remains deterministic and does not need CUDA or a
subprocess PDF engine. This proves the caller does not add an eager readiness
dependency. Mocking the router itself is rejected because it bypasses the
behavior under test.

### Assert absence of OCR work directly

The test will instrument the worker-probe/worker-invocation boundary and assert
zero calls, alongside the caller's existing native text outcome. A failed
worker alone is insufficient evidence because a test that never observes the
boundary could pass after an accidental fallback.

### Correct one profile-list sentence

The profile list will state that an approved GPU profile needs static approval
to resolve/select; a healthy worker is needed only for uncached GPU OCR pages.
The detailed explanation later in the same document remains the authoritative
operational behavior and needs no new deployment mechanism.

## Risks / Trade-offs

- [A controlled native adapter could hide a real native-engine regression] ->
  existing shared native tests continue to cover engine behavior; these tests
  are limited to caller-to-router readiness sequencing.
- [Approval fixture drifts from validator requirements] -> build it from the
  existing unit-test approval helper/required fields and keep it test-local.
