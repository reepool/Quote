## Why

沪深股票主数据只接入官方当前名单，名单消失后的自动停用又依赖“退市××／××退”简称，漏掉了不设退市整理期的主动退市等情形。已核实的 `000016.SZ / *ST康佳A` 于 **2026-10-23** 摘牌，但截至 2026-11-15 仍为 active，持续进入财务周度对账；官方终止上市名单已有准确日期，缺口在证据接入、状态写入和治理结果闭环。

## What Changes

- 在现有 A 股主数据刷新链中接入 SSE、SZSE 官方终止上市名单，按证券身份核对本地 A 股；不再要求简称带“退”，不以行情为空、停牌或当前名单消失单独确认退市。
- 将正式终态、未来生效、证据不足、来源失败及冲突分开处理。采用实际摘牌／终止上市生效日，不用公告日、决定日或最后行情日替代；已确认完成退市但日期未知时保留空日期并保护终态。复用既有证券代码沿革核验所属主体／上市区间，历史前身终态不能停用同码现存主体。
- 复用现有生命周期 writer，保证 `status=delisted / is_active=false / trading_status=0` 一致，重复证据不清空日期，普通官方或备源当前名单不能使已确认终态复活。正常模式下的名单缓存写入同样受官方终态证据约束，不能在后置核对前把BaoStock单独给出的退市字段落库。替换名称判断旁路；北交所保留既有来源，仅修正直接相关的日期与公共写入约束。
- 在现有治理报告及 freshness 复用结果中保留确认、未确认、来源失败、冲突、未来生效及写入结果；未解决事项不能被整体成功或本轮 `updated_at` 刷新掩盖。已知计划到生效日必须在读取当前股票池前重判，不能继续复用未生效状态；audit只读约束贯穿source factory，禁止取名单时隐式写库。
- 当前财务增量、周度对账及新建全量目标清单使用治理后的股票池；已有 pending、accepted、blocking 状态保留，但不再回灌已退出当前范围的股票。
- 修正“退市日在报告披露截止日前，因而可以自动接受上一报告期缺失”的无证据启发式。退出当前任务不等于财报完整或依法免披露；历史查询、显式 readiness、既有内部定向修复及冻结清单保持其原有语义。
- 为康佳建立可复现验收，并覆盖吸收合并等普通简称退市、停牌、未来日期、未知日期、名单不完整、A/B 证券隔离、幂等、防复活和历史保留。
- 修正主规范中仍写 BaoStock 优先的旧条款，与已经实现的官方源优先路线一致；不另建调度任务、黑名单、治理框架或通用状态平台。

## Capabilities

### New Capabilities

- `a-share-official-delisting-reconciliation`: 沪深官方终止上市证据读取、证券范围、生效判断、证据保留、幂等写入及可见闭环。

### Modified Capabilities

- `instrument-master-sync`: 官方来源顺序、正式终态停用、未知日期保护、未来日期约束及现有 policy 的一致合同。
- `instrument-master-governance`: fresh 复用保留终态核验结果、官方源权威、不可交易约束、任务报告及财务治理前置。
- `research-data-engine`: 当前财务维护排除正式退市股票、保留旧运行记录和历史事实、禁止无证据豁免退市前报告期。

## Impact

- **现有执行链**：`run_master_governance_for_job()` → `a_share_stock` policy → `DataManager.sync_instrument_master()` → 官方 provider／窄域核对逻辑 → `DatabaseOperations` 生命周期写入；财务任务在其后解析当前股票池。
- **预计代码范围**：`data_sources/a_share_official_stock_master.py`、`data_sources/source_factory.py` 的结果接线、`data_manager.py` 的局部编排及旧旁路替换、`database/operations.py`、`instrument_master_governance.py`、`research/financial_disclosure_incremental_sync.py`、全量清单生命周期 helper 和现有报告渲染路径。只在必要处修改，不默认改动全部文件。
- **存储**：复用 `instruments`、`instrument_master_metadata`、`instrument_master_discrepancies` 及财务事件／运行表。未来计划、应用闭合及核验结果只使用差异表的固定三类reason，已确认终态证据保存在受保护metadata；协议与事务边界见D6，不再留待实施时选择载体。不迁移表、不合并数据库、不删除历史事实或失败记录。
- **兼容**：保持 job id、调度时间、启用状态、公共 API、数据库路径、canonical ID、历史查询及原有 fresh/force/audit/backfill 模式。报告仅增加字段。旧全量 import resume 使用冻结 manifest，不承诺它自动改用新当前股票池。
- **依赖**：`prioritize-a-share-official-stock-master-sources` 的代码已存在但 change 尚未归档。本 change 是其终态闭环补充；不得通过修改它的完成项或成绩制造完成。本轮只对主规范已存在的 requirement 使用 MODIFIED，新终态能力使用 ADDED，归档前核对两个 change 的规范合并一致性。
- **实施状态**：依据用户后续实施授权，已完成代码与针对性验证，并仅受控纠正康佳生命周期；未主动重启生产服务、未执行全市场生产刷新，也未归档其他change。业务、测试、限制与清理证据见 `verification.json`。
