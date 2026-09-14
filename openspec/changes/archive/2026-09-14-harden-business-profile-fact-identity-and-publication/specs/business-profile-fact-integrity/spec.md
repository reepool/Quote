## ADDED Requirements

### Requirement: Source rows and contracts SHALL be part of fact identity

活动、经营事实和具名供应链关系的稳定身份 MUST 包含来源证据身份、主体范围、客体身份以及可用的 `source_row_key` 或合同引用。两个同报告期、同公司、同动作但不同合同/客体/主体的披露 MUST 生成不同记录 ID，并在 `get_approved_as_of` 中同时返回。

#### Scenario: Two supplier products in one report

- **WHEN** one supplier relationship names two distinct products in the same evidence section
- **THEN** the two relationship IDs are different and both approved records remain queryable

#### Scenario: Two contract rows with same product and value

- **WHEN** two contract rows share product/value/unit but have different contract references or source rows
- **THEN** activity and operating-fact identities remain distinct and no row is overwritten

### Requirement: Unknown units SHALL fail closed

商品暴露事实 MUST 通过版本化单位目录解析原始单位。金额、数量和其他维度 MUST 由解析结果确定；未知或维度不确定时 `value_normalized`/`unit_normalized` MUST 为空并保留原始值，记录 machine-rework 原因，不得默认归类为 volume。

#### Scenario: Chinese currency unit

- **WHEN** a purchase fact has raw unit `亿元`
- **THEN** its fact type is a monetary purchase fact and normalized conversion uses the unit catalog

#### Scenario: Unknown dimension

- **WHEN** a fact has an unrecognized unit
- **THEN** the raw value/unit remain visible as candidate data and the fact is not approved as a volume fact

### Requirement: Temporal identity SHALL preserve independent scopes

时点查询 MUST NOT collapse records that differ by subject scope, source row, contract, counterparty, or object. Business validity intervals MUST use half-open `[valid_from, valid_to)` semantics consistently.

#### Scenario: Same activity with different subject scopes

- **WHEN** issuer and consolidated-group activities share all other fields
- **THEN** both are returned at a cutoff where both are valid

#### Scenario: Mapping expires on cutoff

- **WHEN** an industry mapping has `valid_to` equal to the cutoff
- **THEN** the mapping is excluded as expired

### Requirement: Entity resolution SHALL be governed

只有唯一官方标识、本地主数据唯一法定全称或已批准别名 MAY 自动解析。仅凭名称格式生成的 local identity MUST 保持 unresolved 或进入人工队列，不得作为唯一依据晋升具名供应链关系。

#### Scenario: Directory-missing legal name

- **WHEN** a disclosed legal-looking name is absent from local entities and aliases
- **THEN** the relationship remains unresolved with its raw name and evidence reference
