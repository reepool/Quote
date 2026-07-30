## ADDED Requirements

### Requirement: Tencent-first A-share factor routing
The AkShare A-share factor adapter SHALL request Tencent raw and adjusted daily histories
first and SHALL use Eastmoney only when Tencent is unavailable or structurally invalid.

#### Scenario: Tencent succeeds
- **WHEN** Tencent returns overlapping valid raw and adjusted histories
- **THEN** the adapter emits `akshare_tencent_price_ratio_v1` observations and does not call
  Eastmoney

#### Scenario: Tencent fails
- **WHEN** Tencent raises an error, returns no overlap, or fails factor-path validation
- **THEN** the adapter calls Eastmoney and labels successful observations
  `akshare_eastmoney_price_ratio_v1`

#### Scenario: Both providers fail
- **WHEN** neither Tencent nor Eastmoney can produce a valid path
- **THEN** the adapter returns an indeterminate result and preserves prior observations

### Requirement: Stable adjusted-price ratio extraction
The AkShare adapter MUST derive factor events only from persistent changes in aligned,
positive adjusted-to-raw price ratios and MUST reject excessive rounding noise.

#### Scenario: Piecewise stable ratio jump
- **WHEN** aligned ratios form two stable levels separated by a persistent material change
- **THEN** the adapter emits one event ratio at the first trading date of the new level

#### Scenario: Rounded daily jitter
- **WHEN** daily ratio changes remain within the configured level dispersion and jump
  thresholds
- **THEN** the adapter emits no factor event

### Requirement: Shared proxy patch bootstrap
The Eastmoney fallback SHALL rely on the process-level `akshare_proxy_patch` bootstrap and
MUST NOT create a local proxy, embed credentials, or install a second patch.

#### Scenario: Eastmoney is protected by configured patch
- **WHEN** the runtime has enabled the shared AkShare proxy patch
- **THEN** the fallback uses the already patched AkShare request stack

### Requirement: Explicit provider lineage
Every AkShare factor observation SHALL preserve the actual upstream provider, extraction
version, requested range, overlap coverage, and quality diagnostics.

#### Scenario: Eastmoney fallback succeeds
- **WHEN** Eastmoney supplies the path after Tencent fails
- **THEN** no result field identifies the observation as Tencent
