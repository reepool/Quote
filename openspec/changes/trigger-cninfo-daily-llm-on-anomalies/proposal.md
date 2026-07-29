## Why

The CNInfo daily corporate-action task reliably refreshes ordinary dividends and
allotments from official structured endpoints, but it does not send newly
observed exceptional actions through the existing announcement-document and LLM
governance path. Restructuring, compensation, share reform, debt conversion, or
structured-source gaps can therefore remain semantically unresolved until a
manual historical run is requested.

## What Changes

- Classify daily corporate-action outcomes into ordinary structured events and
  bounded anomaly candidates after the structured CNInfo refresh and
  source-isolated factor reconciliation.
- Keep `structured_complete` ordinary distributions on the deterministic fast
  path without an LLM call.
- Trigger the existing CNInfo document and semantic-resolution workflow only
  for special-action announcements, incomplete or missing structured events,
  newly changed events with unresolved fields, and material CNInfo/TDX
  reconciliation conflicts.
- Persist and report stable anomaly reasons, selected event keys, processing
  counts, failures, and remaining review workload.
- Preserve raw CNInfo observations. LLM output remains in the governed
  analysis/resolution overlay and can be promoted only through existing
  deterministic validation gates.
- Keep TDX as reconciliation evidence only; TDX economic values never overwrite
  CNInfo source observations.

## Capabilities

### New Capabilities

- `cninfo-daily-anomaly-llm-governance`: Selective anomaly detection and bounded
  LLM resolution orchestration for the CNInfo corporate-action daily task.

### Modified Capabilities

- `scheduler`: The existing daily CNInfo task reports and configures selective
  anomaly governance while preserving its task name and production isolation.

## Impact

- `data_manager.py`: anomaly selection, existing LLM-governance delegation, and
  daily result/status composition.
- `data_sources/cninfo_corporate_action_incremental.py`: deterministic special
  title classification and anomaly reason helpers.
- `scheduler/tasks.py` and `config/05_scheduler.json`: bounded anomaly-LLM
  runtime parameters and reporting.
- Unit tests for ordinary bypass, each anomaly trigger, bounded delegation,
  failure isolation, and report output.
- No schema migration, raw-observation rewrite, canonical-factor promotion, or
  new LLM provider dependency.
