## ADDED Requirements

### Requirement: Commodity publication SHALL use the complete promotion service

任何 `company_commodity_exposures` approved publication MUST 通过 `BusinessProfilePromotionService.process` 并提供完整 promotion manifest/gates。`publish_basic` 不得直接调用 `system_promote_record` 绕过 gates。

#### Scenario: Missing executable market series

- **WHEN** an approved exposure fact maps to a commodity identity without a unique active price series or spread
- **THEN** the fact remains queryable, the mapping/publication is reported as unlinked or fact-only, and no approved publication is created

#### Scenario: Complete executable mapping

- **WHEN** the fact, evidence, catalog, temporal scope, numeric checks, conflict checks, runtime identity, semantic proof, and active market mapping all pass
- **THEN** publication is promoted through the promotion service and audit metadata contains the complete gate decision

### Requirement: Candidate diagnostics SHALL be opt-in and permission-bounded

正式画像和商品暴露 API MUST 默认只返回 approved records。候选事实、LLM 诊断和异常队列只有在显式请求诊断参数并满足现有可信身份/权限边界时才能返回。

#### Scenario: Default profile query

- **WHEN** a caller omits `include_candidates`
- **THEN** the response excludes candidate facts/exposures and internal exception details

#### Scenario: Explicit diagnostic query

- **WHEN** an authorized caller explicitly requests candidates
- **THEN** the response includes candidate diagnostics without changing any review state
