## Why

2026-11-09 对消费方反馈的线上只读复核及代码检查确认：行情复权字段混用、缺失换手率被写成零、已声明过滤未执行，以及财务载荷过大、利率查询无界等问题会让调用方静默算错或拿不到完整结果。本 change 修复这些已证实的消费合同缺陷，保留现有 canonical 复权因子和 PIT 可得日体系；修复评估、证据边界和工期见 `design.md`。

## What Changes

- B-02/B-03：行情价格、前收参考价和涨跌额使用一致复权口径；提供 `raw_pre_close`、`raw_change`、`volume_raw`、`volume_adj` 和口径说明。**BREAKING**：`volume` 统一为真实成交股数，复权量由 `volume_adj` 获取；复权请求的 `pre_close/change` 修正为复权值。
- B-04：保留已有百分数字段的值，补 `pct_change_pct` 和 PIT 百分数字段的 `_pct` 别名，将含糊旧键标为 deprecated；新增统一小数比例入口 `pct_change_ratio` 和财务 `ratios`，提供可程序读取的 `units`，避免在现有字段上无提示除以 100；不批量改写存储单位。
- B-05/B-07：TDX/Tencent的未知换手率返回 `null` 和原因，不再以零伪装有效值；质量分同时覆盖旧零占位与新采集的空值。最新日线标记 EOD、观测交易日及装配时间，并补齐 `symbol`；批次类型按真实来源声明，已核实的下载进度身份不能当采集时点，来源不明时标为 unknown。
- B-09：真正执行已声明的停牌和质量过滤；本次涉及的读端点拒绝未知查询参数并返回稳定错误码。
- B-06：提供本地财务摘要和字段投影，省去默认摘要中的原始 JSON 与重复明细；保留显式完整响应，历史报告期沿用现有 history 接口。**BREAKING**：单报告期接口新增 `view=summary|full`，默认 `summary`；旧完整载荷使用 `view=full`。
- B-08：补充利率抓取时点及历史未知标识；新增真正生效的排序和分页，返回区间边界及下一页；说明 `revision_id=latest` 是覆盖槽位。**BREAKING**：HTTP 默认返回最新 100 条并按日期倒序，显式日期区间仍受分页控制；DCF 等内部读取不沿用 HTTP 默认截断。
- B-10/B-11/B-12：同行基准排除非正/非有限倍数，保留原始对照和剔除诊断；正值极端样本通过显式可选上限处理，不随意制定行业阈值。PIT 质量过滤在最新可知期选择之后、分页之前执行，回显过滤前后数量与分布；相关读端点错误结构统一且保留原 `detail`。
- B-01：纠正“前复权完全未实现”的误判，只补查询区间基准说明与防回归验证，不改 qfq 默认值、不改复权锚定算法。成交量和字段口径修复仍覆盖 qfq。

不包含：实时行情源、复权因子重算、PIT 时点重建、全库换手率/利率历史时点回填、通用缓存/治理框架、全端点扫描、全仓路由/存储重构。未确认的停牌状态错误及期货/商品/外汇/行业模块不按已证实缺陷开发。

原反馈的若干验收已按调查结果调整，不能声明为原稿逐条全部实现：保留区间锚定的 qfq 和旧单位兼容键；单期财务不新增行分页、HTTP 缓存后置；历史抓取时点无证据仍为空；正极端估值通过显式阈值处理；质量白名单替代没有严格全序的 min_available_at_quality。逐项去向及理由见 `design.md` 的“原始请求处置边界”。

## Capabilities

### New Capabilities

- `risk-free-rate-query-semantics`：有界利率读取、最新页、区间元数据及诚实的采集/修订语义。
- `data-api-query-validation`：限定读端点的未知参数拒绝和兼容结构化错误。

### Modified Capabilities

- `quote-api-query-semantics`：复权字段一致性、真实量与复权量分离、换手率缺失、过滤执行、EOD 标识和查询区间基准说明。
- `research-data-engine`：财务摘要/投影与小数比率投影、技术收益单位声明及同行基准排除规则。
- `financial-pit-panel`：小数比率投影、可选质量过滤和过滤前后统计，同时保留 strict 时点选择。

## Impact

- 权威 owner：`utils/adjustment.py`；行情查询和质量处理现行链；`research/storage.py` 财务/利率查询；`research/risk_free_rate_sync.py`；`research/technical_service.py`；`research/valuation_service.py`；`research/financial_pit_panel.py`。HTTP 适配在 `api/models.py`、`api/routes.py`、必要的窄错误转换处；业务逻辑不复制到路由。
- 兼容：默认变化和字段改义必须体现在 API 版本、OpenAPI 和 `docs/api/restful_api.md` 的迁移示例中；旧单位保留为明示兼容字段。改变已声明过滤的实际效果属于正确性修复，响应总数可能变化。
- 不新增依赖，不改变数据库路径、表结构、canonical key、写入 owner、调度配置或生产启用状态；源端未知值通过现有 owner 和读投影表达，避免生产批量迁移。
- 与 `add-quote-api-data-capability-improvements` 相比，本 change 只接续已确认的消费缺陷，不追改旧合同或任务成绩；与 `cut-financial-disclosure-read-amplification`、`accelerate-financial-readiness-queries` 分工为公共响应语义 vs 同步/就绪度读取效率，不重复实现投影和存储逻辑。
- 总纲映射：FR-09/W9 的 API 合同修复，关联 FR-06/W4 与 FR-07/W5 的现行读 owner。实现限定 API 合同修复；保持现行读写 owner，不以本 change 为由启动整体框架迁移。
- 当前交付为实现、定向验收与迁移文档；具体结果见 design.md 的实施验收记录。未部署，不授权生产写入或服务重启。
