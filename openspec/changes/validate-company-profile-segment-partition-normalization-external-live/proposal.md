## Why

The first two-scope validation was consumed inside the restricted sandbox and returned zero upstream responses: both requests ended as `dns_failure` during connect and then `deadline_exceeded`. The user has now explicitly authorized transferring the frozen task content to `scorpio.reepool.com`, so a new-identity validation must run from the sandbox-external network path without changing any semantic or model input.

## What Changes

- Bind the immutable September 11 shadow batch and the same two prepared segment scopes used by the failed sandbox validation.
- Execute the existing Stage 5 application service once under a new validation identity, with the same Gemini profile, Evidence, output budgets, deadline, and shared provider-call ceiling.
- Launch the provider-bearing process outside the sandbox from the start and record that the transfer is limited to the frozen task requests sent to `scorpio.reepool.com`.
- Persist case-local semantic results or typed execution failures without modifying the historical batch or recomputing readiness.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: permit one isolated external-network repetition after the prior validation produced no upstream response due to sandbox DNS/connect failure.

## Impact

- One change-local contract, thin operator, focused tests, and empirical result.
- No model substitution, parameter tuning, prompt/schema change, historical candidate splice, twenty-report replay, or production activation.
