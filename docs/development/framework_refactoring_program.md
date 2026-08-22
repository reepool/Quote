# Quote 框架改造总需求与实施纲要

> 状态：框架改造权威需求源
>
> 维护方式：每个关联 OpenSpec change 创建、完成或归档时同步更新
>
> 永久开发约束：`docs/development/project_development_governance.md`

## 1. 目的

本纲要把框架审计、复核结论和后续讨论统一为一个有限、可追踪的改造计划。它解决两个相反风险：

- 大问题不拆分，形成一次无法验证、无法回滚的全仓重构；
- 小问题拆得过散，各 change 只优化局部，最终忘记单一主链、生产稳定和代码退出的总体目标。

所有框架 change 必须登记在本文件中。未登记的新问题先判断是否影响当前验收；不影响时进入候选清单，不直接扩充在途 change。

## 2. 总体目标

改造完成后，项目应满足：

1. 模块边界按业务能力和数据所有权组织；
2. CLI、API、Scheduler、Telegram 和脚本可以多入口，但同一动作只有一个权威实现；
3. `DataManager`、`ResearchStorageManager`、`ScheduledTasks` 收敛为薄兼容门面或被明确的服务替代；
4. 研究、行情、主数据、公司行为、财务、估值、期货、外汇和商品域之间依赖方向清晰；
5. 证券标识、股票交易日和公共传输配置具有唯一边界合同；
6. 共享代码只来自已经证实的相同语义，不建设万能平台；
7. 过时入口、脚本、兼容方法和文档在替代完成后删除；
8. 日常数据收集、维护、查询和调度在整个改造期间持续运行。

## 3. 生产不变量

以下条件适用于全部工作流，优先级高于文件整理目标：

- 不停止或延误当前自动数据采集任务；
- 不无意改变 scheduler job id、时间、启用状态、依赖和参数；
- 不改变公共 API/CLI/Telegram 契约，除非独立需求明确要求；
- 不合并现有数据库，不改变数据目录和 canonical 表语义；
- 不在生产数据库上进行重构试验；
- 不改变复权、交易日、可得日、生命周期和回测时点口径；
- 不在迁移期运行两套会同时写同一业务数据的实现；
- 每个 change 在合并前具备可回滚点和代表性业务验证。

任何工作流如果无法证明这些不变量，应暂停实施，不能以“只是重构”为由继续。

## 4. 已确认问题总表

### 4.1 最高优先级：执行正确性和依赖方向

| ID | 问题 | 改造要求 |
|---|---|---|
| FR-01 | 缺口检测和修复存在多套算法及 Telegram subprocess 旁路 | 建立唯一行情维护 command/service，所有入口复用相同 universe、日历、写入验证和失败语义 |
| FR-02 | 生产模块反向 import `scripts/dev_validation` | 生产逻辑迁入正式模块，脚本只保留 operator 参数适配，增加禁止反向依赖检查 |
| FR-03 | 证券代码规范化存在 `.BSE/.BJ`、`.HKEX/.HK` 等不一致 | 建立结构化 canonical identity 和边界 renderer，数据库 key 保持兼容 |
| FR-04 | 股票交易日仍可能回退到启发式假期判断 | 所有写入、缺口和日更决策只读权威交易日历，启发式不得参与生产写入 |
| FR-05 | 过期架构文档引导新功能沿错误路径扩展 | 建立当前架构、总纲、文档生命周期和唯一索引 |

### 4.2 高优先级：核心文件和应用边界

| ID | 问题 | 改造要求 |
|---|---|---|
| FR-06 | `DataManager` 约 3.8 万行、433 个方法，兼具门面和业务实现 | 按行情、主数据、研究、公司行为等能力迁出应用服务；旧方法只转发并限期删除 |
| FR-07 | `ResearchStorageManager` 约 1.4 万行、212 个方法，跨多个数据域 | 保留连接/事务协调，按数据库和表 owner 拆 repository；外部服务依赖窄接口 |
| FR-08 | `ScheduledTasks` 约 1.3 万行、111 个方法，任务内包含业务循环和报告 | 按域拆 task adapter，业务循环进入应用服务，报告格式化独立 |
| FR-09 | `api/routes.py` 集中且部分路由直接依赖全局门面 | 随各业务服务迁移按域拆路由，新路由依赖 query/command service |
| FR-10 | 公司行为约占 `DataManager` 1.7 万行，阶段边界隐含在方法和 job 名中 | 显式拆分观察、决议、审核、canonical 因子和重建状态流 |

### 4.3 中优先级：复用、生命周期和仓库卫生

| ID | 问题 | 改造要求 |
|---|---|---|
| FR-11 | Storage/HTTP/限流/日期处理存在部分重复，也存在被误判的合理差异 | 逐调用方证明语义相同后抽最小原语，不强制统一客户端或域规则 |
| FR-12 | 兼容入口、一次性脚本、根目录现场测试和完成后的迁移代码长期保留 | 建立 production/operator/migration/compatibility/obsolete 生命周期并按证据删除 |
| FR-13 | 大量 completed OpenSpec change 未及时归档 | 完成即归档，当前总纲只保留在途 change 和依赖状态 |
| FR-14 | 需求稿、回执、调查、运行手册和当前设计混在 `docs/development` | 合并同类 current 文档，删除已吸收的历史材料，重建索引 |
| FR-15 | 共享能力可能因新需求再次被各域自行实现 | 在 AGENTS 和总纲中强制 owner/复用/单路径检查 |

## 5. 工作流与 OpenSpec change

### W1 文档和开发总纲

**Change**：`consolidate-project-documentation`

**覆盖需求**：FR-05、FR-13、FR-14、FR-15

具体要求：

- `docs/README.md` 成为唯一当前文档索引；
- 重写当前架构和开发入口，删除过时版本、虚构目录和手工规模数字；
- 文档按 current、runbook、requirements、historical 分类；
- 同一功能保留一份 current 文档和必要 runbook；
- 已完成需求稿、调查、回执和迁移记录在内容吸收后删除；
- completed OpenSpec 按依赖和当前工作区状态逐项归档；
- AGENTS 强制引用长期开发总纲。

验收：从索引可以找到每个生产域的当前架构、运行入口和权威规范；索引不存在失效路径或历史材料冒充当前设计。

### W2 主数据、证券标识和股票日历边界

**Change**：`unify-instrument-master-and-identity-boundaries`

**覆盖需求**：FR-03、FR-04、FR-06、FR-11

具体要求：

- 定义 `InstrumentKey(symbol, exchange)` 或等价结构化身份；
- 明确 storage id、exchange id 和 vendor alias renderer；
- 兼容现有 `.SH/.SZ/.BJ/.HK/.US` 数据库 key，不做全库迁移；
- 股票主数据治理成为所有相关 universe 的权威 owner；
- 写入、缺口和日更只使用 `quotes.db` 权威股票交易日历；
- 主数据刷新作为行情命令显式前置步骤和报告字段，不在入口自行实现。

验收：常见别名映射到唯一 key；同一 universe 在 CLI/API/Scheduler 中一致；调休日不产生假缺口。

### W3 行情维护单一执行链

**Change**：`unify-quote-maintenance-command-paths`

**覆盖需求**：FR-01、FR-02、FR-06、FR-12

具体要求：

- 建立 daily、target-date、range、historical、gap-repair 的显式 command 合同；
- 共用 source routing、canonical identity、交易日历、universe 和保存验证；
- 合并 `DataManager`、scheduler 和两个缺口脚本中的写入逻辑；
- Telegram 不再 subprocess 执行缺口或因子生产工具；
- 脚本只作为 operator adapter，生产模块不得 import 脚本；
- 保持现有命令、API 和 job id 兼容。

验收：同一输入从不同入口得到相同候选、写入集合和失败结果；迁移期不存在双写。

### W4 研究应用服务边界

**Change**：`extract-research-application-services`

**覆盖需求**：FR-06、FR-09、FR-11

具体要求：

- 复用已存在的 query/read/sync service，移除 `DataManager` 中重复投影和编排；
- 优先迁移只读查询，再按行业、股东、估值、财务、期货、外汇、商品做纵向切片；
- 新 API 和 scheduler adapter 直接依赖窄服务；
- `DataManager` 只保留装配和兼容转发；
- 每个切片必须删除原门面中的真实逻辑，而不是只增加 wrapper。

验收：迁出域的新调用方不依赖全局 `DataManager`；结果、错误和本地只读语义保持兼容。

### W5 研究存储 repository 边界

**Change**：`decompose-research-storage-repositories`

**覆盖需求**：FR-07、FR-11

具体要求：

- 保持 `research.db`、`financials.db`、`valuation.db`、`interests.db` 等隔离；
- 保留现有连接协调、database scope 和事务行为；
- 按数据库和表 owner 拆 financial、valuation、industry、shareholder、signals、ingestion 等 repository；
- 把超大建表/迁移逻辑拆入现有 migrations 或明确 schema owner；
- `ResearchStorageManager` 在过渡期只聚合转发；
- 不创建万能 CRUD base 或跨库事务框架。

验收：业务服务依赖窄 repository；数据库路径、表、SQL 结果和并发语义不变；旧聚合方法有删除清单。

### W6 公司行为应用服务和状态流

**Change**：`extract-corporate-action-application-services`

**覆盖需求**：FR-06、FR-10、FR-11

具体要求：

- 记录 observation → resolution → review → canonical selection/promotion → factor rebuild/read 的权威状态流；
- provider 保留公告/源协议，应用服务拥有阶段编排；
- 按观察、决议、人工审核、canonical 因子拆服务；
- 迁移 `DataManager` 中千行级方法，保持当前 TDX/CNInfo/canonical 表语义；
- scheduler job 继续作为阶段触发器，不成为第二套状态机；
- 所有回测和查询继续读取同一 canonical 因子口径。

验收：状态和权威表可从一份文档及代码入口解释；原方法成为薄转发或删除；因子结果与迁移前一致。

局部纵向切片：`triage-announcement-only-xdxr-candidates` 在现有公司行动日更
owner 内增加公告-only 案例聚合、可切换 LLM 分流、inactive watch 和权威证据
唤醒；不创建合成 CNInfo 事件、不写 canonical 因子，也不提前实施本 W6 的
应用服务整体拆分。

### W7 Scheduler 域适配器

**Change**：`split-scheduler-domain-task-adapters`

**覆盖需求**：FR-08、FR-09、FR-11

具体要求：

- 保留 APScheduler、job id、配置、DAG、并发和通知合同；
- 按 quotes、master、corporate-actions、financials、research、market-data、operations 拆 handler；
- handler 只解析 JobConfig、构造 command、调用应用服务和发送格式化结果；
- 千行历史回补、缺口循环和其他业务编排不得原样搬入 handler；
- `ScheduledTasks` 在兼容期按 job id 转发；
- 报告格式化不参与写库和业务决策。

验收：配置中的每个 job 仍可解析；自动任务集合、时间和依赖不变；handler 无复制业务循环。

### W8 遗留入口和工具退出

**Change**：`retire-obsolete-entry-points-and-tools`

**覆盖需求**：FR-02、FR-12、FR-13

具体要求：

- 生成 DataManager、ResearchStorageManager、ScheduledTasks 兼容调用方清单；
- 迁移调用方后删除归零的方法，不保留永久 alias；
- 清理根目录现场 `test_*.py`、无用途 dev_validation、旧备份入口和一次性迁移；
- 保留的 operator 工具必须调用权威应用服务并有 runbook；
- 归档全部完成且不再作为在途依赖的 OpenSpec change；
- 更新总纲最终状态并删除临时审计/改造材料。

验收：无生产模块 import `scripts`；无 Telegram 生产 subprocess 旁路；obsolete 清单归零或有明确外部阻塞。

## 6. 依赖与实施顺序

```text
W1 文档与总纲
 |
 v
W2 标识/主数据/股票日历
 |
 +-----------> W3 行情维护单链
 |
 +-----------> W4 研究应用服务 ---> W5 研究存储
 |
 +-----------> W6 公司行为服务
                                  |
          W3 + W4 + W6 ----------> W7 Scheduler 适配器
                                  |
          W1-W7 -----------------> W8 遗留清理
```

允许 W3、W4、W6 在 W2 稳定后并行规划，但同一文件同一时间只允许一个实施 change 修改。W7 依赖应用服务已经存在，不能只做机械拆文件。W8 必须最后执行。

## 7. 文档清理需求

### 7.1 保留并重写为 current

- `docs/architecture.md`；
- `docs/api/restful_api.md`；
- `docs/configuration/config_file.md`；
- `docs/database_guide.md`；
- `docs/financial_data_system.md`；
- `docs/features/scheduler_system.md`；
- `docs/telegram_task_manager.md`；
- `docs/troubleshooting/faq.md`；
- `docs/development/project_development_governance.md`；
- 本纲要。

### 7.2 合并主题

| 目标 current 文档 | 待合并主题 |
|---|---|
| 行情维护与历史回补 runbook | historical download、single instrument、A 股历史/退市回补、gap usage、历史总账 |
| 证券主数据治理 | A 股官方主数据、HKEX baseline、指数治理、现有 instrument master 文档 |
| A 股公司行为与复权 | stock factor framework、factor governance、canonical operations、CNInfo pipeline/LLM requirements |
| 公共 LLM 架构与使用 | gateway architecture、requirements、usage、routing、orchestration requirements/benchmark |
| 公司业务画像 current 设计与 runbook | acquisition、semantic、benchmark、rollout、pilot 文档；等在途 change 完成后处理 |
| 期货/外汇/特殊商品 current 设计 | 各自 requirements、scope、master 和 enhancement 文档，保留域间差异 |
| 数据库备份 runbook | backup implementation、workflow requirements、database guide 中重复部分 |

### 7.3 删除候选

只有内容已进入 current 文档/OpenSpec spec 且无当前运行引用后才删除：

- 一次性修复说明：`docs/INSTRUMENT_DOWNLOAD_UPDATE.md`；
- API 需求回执：`quote_api_data_capability_response.md`、`quote_api_data_confirmation_response.md`；
- 已完成迁移计划：`quote_data_volume_cleanup.md`、`quote_data_volume_migration.md`；
- 阶段性 shadow/baseline/pilot/benchmark 文档；
- 已完成且已有运行手册的 `*_requirements.md`；
- 被本纲要吸收的框架审计和建议文档；
- 不再引用的日志、备份和历史回补需求稿。

删除不是简单按文件名批处理。W1 必须为每个文件记录 replacement、有效规则迁移位置和引用扫描结果。

## 8. 每个 change 的统一验收

所有工作流除自身验收外还必须通过：

1. 任务开始前后的 scheduler 自动启用集合一致；
2. 公共 API、CLI 和 Telegram 兼容测试通过；
3. 使用临时数据库的代表性写入等价测试通过；
4. 相关数据行数、业务 key、watermark 和报告字段无非预期差异；
5. 无生产数据写入型 live 验证，除非用户单独授权；
6. 核心大文件新增业务行数为零，完成切片后应净减少；
7. 被替代实现同步删除或登记限期兼容；
8. 文档、OpenSpec tasks 和本纲要状态同步更新。

## 9. 进度矩阵

| 工作流 | Change | 状态 | 依赖 |
|---|---|---|---|
| W1 | `consolidate-project-documentation` | apply-ready（0/16） | 无 |
| W2 | `unify-instrument-master-and-identity-boundaries` | apply-ready（0/16） | W1 |
| W3 | `unify-quote-maintenance-command-paths` | apply-ready（0/20） | W2 |
| W4 | `extract-research-application-services` | apply-ready（0/19） | W2 |
| W5 | `decompose-research-storage-repositories` | apply-ready（0/19） | W4 |
| W6 | `extract-corporate-action-application-services` | apply-ready（0/18） | W2 |
| W7 | `split-scheduler-domain-task-adapters` | apply-ready（0/19） | W3、W4、W6 |
| W8 | `retire-obsolete-entry-points-and-tools` | apply-ready（0/16） | W1-W7 |

Program 状态使用 planned、apply-ready、active、blocked、complete、archived。OpenSpec CLI 会把“工件已齐全但尚无 task 完成”的 change 显示为 `in-progress`；本计划将 `completedTasks=0` 且尚未进入实施的 change 记为 apply-ready，而不是 active。任一时刻原则上只允许一个直接修改同一核心文件集合的 change 处于 active。

## 10. 非目标

- 不拆微服务；
- 不引入消息队列；
- 不合并 SQLite 数据库；
- 不重写全部 repository/provider；
- 不建设通用 ingestion SDK、schema registry、全局 provenance 或审计平台；
- 不为了达到行数指标机械拆文件；
- 不在本计划中改变业务数据覆盖范围或投资模型；
- 不把所有理论改进都变成阻塞项。

## 11. 计划完成定义

框架改造计划完成意味着：

- W1-W8 全部完成并归档；
- 当前生产采集和维护任务持续稳定；
- 核心业务动作具有可追踪的唯一执行链；
- 三个核心大类只剩薄门面或已被移除；
- 研究存储按 owner 拆分且数据库语义不变；
- 过时代码、脚本、兼容和文档已经删除；
- 所有未来 AI 开发由 AGENTS 和长期总纲约束。

计划完成后，本文件转为最终迁移记录；长期规则继续由 `project_development_governance.md` 承担。
