## Why

The lazy GPU worker probe is covered at the adapter boundary, but the HKEX and
official-index caller tests currently replace the router and therefore do not
prove that an approved GPU profile remains usable for a native-only document
when its OCR worker is unavailable. The profile list in the operating document
also still describes worker health as a profile-selection precondition.

## What Changes

- Add caller-level regressions for HKEX and official-index text PDF parsing
  under a statically approved GPU profile with a deliberately failing GPU
  worker. The tests prove native extraction completes without a worker probe or
  OCR fallback.
- Clarify the shared PDF operating documentation: static GPU approval is
  required to resolve/select the profile; live worker readiness is required
  only when an uncached page is routed to GPU OCR.
- Preserve all public PDF request, profile, result, environment-variable, and
  business-caller interfaces.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `shared-pdf-processing`: Require native-only business callers using an
  approved GPU profile to remain independent of live GPU worker readiness.

## Impact

- Tests: HKEX suspension PDF and official-index lifecycle PDF entry points.
- Documentation: `docs/development/shared_pdf_processing.md`.
- No production runtime implementation, dependency, configuration, or caller
  interface changes.
