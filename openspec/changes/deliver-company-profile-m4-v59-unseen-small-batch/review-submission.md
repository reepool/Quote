# v61 S0正式后继与固定数量准备交接（待A有限审核）

## 结果与请求

v61是原合同最后一次后继正式复验，已用满授权 **2/2**。在A对v60发现的四条无依据服务分类裁决后，后继合同采用 **350项成立条件（6正文＋195收入＋149动作）**、4项歧义证据保留检查及44项负例。本轮请求A审核v61正式结果；在A有限通过前，本change不归档、不扩批，5.1不执行，生产继续 `not_authorized`，规模声明继续 `false`。

正式结果：**350/350召回、729/729动态唯一对象准确（723事实＋6正文）、数字错误0、4/4歧义检查、44/44负例通过**。两家五阶段成功，无复用/前驱；重新查询、正式查询、导出和磁盘一致。`formal_execution.json`记录进程启动（含registry/service构造）至第二次导出返回 **146.214989秒**，业务 token **0/50000**；已有PDF解析缓存命中，耗时结论限于该条件。正式writer完成后立即保护22份制品，空路径0。

四项亚盛p120关联交易仅保留真实交易、收入、交易对手、金额和原Evidence；不再生成确定 `provides_service` Activity，也不进入产品服务答案支持链。其余合法物业服务、工程材料销售、采购方向及亚美特公司原称保留。与v60 native delta相比，明星事实无变化；亚盛仅移除四条错误服务Activity，六正文逐字一致，其余原生字段/Evidence语义一致。

## 证据入口

- 合同与裁决：[v61-source-scope.json](v61-source-scope.json)、[v61-contract-correction.json](v61-contract-correction.json)、[v61-adjudicated-requirement-supplement.md](v61-adjudicated-requirement-supplement.md)。
- 局部闭环：[v61-local-source-review-evidence.json](v61-local-source-review-evidence.json)、[v61-local-business-audit-script.txt](v61-local-business-audit-script.txt)、[v61-validation-receipts/targeted-final.log](v61-validation-receipts/targeted-final.log)。
- 正式执行：[v61-source-freeze-receipt.json](v61-source-freeze-receipt.json)、[v61-formal-supervisor-receipt.json](v61-formal-supervisor-receipt.json)、[v61-formal-delivery-preservation.json](v61-formal-delivery-preservation.json)。
- 正式业务核验：[v61-formal-source-review-evidence.json](v61-formal-source-review-evidence.json)、[v61-formal-native-equivalence-receipt.json](v61-formal-native-equivalence-receipt.json)、[v61-v60-native-delta.json](v61-v60-native-delta.json)、[v61-readonly-verification-receipt.json](v61-readonly-verification-receipt.json)。
- 正式制品：[../../data/exports/m4_v61_same_source_repair/](../../data/exports/m4_v61_same_source_repair/)。

## 首次与历史保护

v59/v60首次失败、原合同、首次制品和原保护轨迹保持原字节。v60原review已保存为[历史原件](v61-history-originals/review-submission-v60-original.md)，其SHA256为 `a7fcd645958b4533866e9eb7592749224f2d52c54ceb1b388dc536376a08cb1d`；不会以v61结果覆盖历史成绩。v61前置保护核验确认4174项历史、19项既有工作区改动、首次22份/复核6份、v60正式22份/复核180份及原执行快照78份一致。生产和规模边界未改变。

## S1历史回归与S2准备状态

按固定数量计划，S1十二家公司已在同一v61代码快照下运行，实际 **10/12通过、2/12真实失败**，阶段未通过，不能以测试数量或后续局部成绩覆盖：

- 600082.SH海泰：同一Evidence ID对应不同 `section_title`，查询 `commodity_exposure` 抛ValueError；
- 600000.SH浦发：完整runtime接受输出未交付Segment/Measurement，冻结的金融收入事实未进入运行时结果。

两处均已留证于[v61-preflight-protection-verification.json](v61-preflight-protection-verification.json)、[v61-s1-baseline-diagnosis.json](v61-s1-baseline-diagnosis.json)及S1验证回执。它们不是本次S0四条分类修复的新增范围，当前不扩大S0。

S2已完成20家公司名单、候补顺序、官方版本、cutoff、核心页/收入布局读取、合同草案与切片预算准备，业务观察为 **0/20**。合同状态明确为 `source_grounded_preparation_contract_pending_S1_and_A_S0; not_execution_freeze`：必要续页/布局逐条件终核、最终合同冻结、代码冻结均未完成；未调用业务owner、未预演输出，不启动S2/S3。准备change为[prepare-company-profile-s1-s2-bounded-acceptance](../prepare-company-profile-s1-s2-bounded-acceptance/)。

固定数量计划唯一进度表在[company_profile_acceptance_program.md](../../../docs/development/company_profile_acceptance_program.md)，已记录S0 v61、S1 10/12和S2 0/20，S3 0/100。S2/S3的首发/补救质量门槛、分层底线、每切片300秒/50000 token、S2总首发3000秒/500000 token及含补救总上限6000秒/1000000 token、停止规则均已写入计划与准备change；不因本次S0结果提前授权执行。

## 验证与边界

v61局部定向测试 **7 passed**；Ruff、格式、diff和当前change/准备change OpenSpec严格校验通过。未运行pytest全套；不声明全套测试通过。v61正式结果仅覆盖两份冻结官方年报及350项条件/44项负例，耗时仅限已有PDF解析缓存。原v60四条服务分类错误及首次成绩均保留，v61只记录修复后正式结果。

本轮B活跃时间跨中断无法精确统计；本次续作开发/材料时间为估算，未将业务运行token或A审核时间混作开发成本。A/B流程v1继续生效：B已完成卡内实现、局部闭环、直接回归和最后一次正式复验；合同/权限/预算变化交A，达标后等待A有限审核。A通过后才可按授权执行5.1规格同步与有限归档；在此之前不提交下一对样本或S2/S3业务运行。
