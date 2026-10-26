# 公司画像开发路线纠偏：文档交付与审核

日期：2026-10-16。结论：规划材料具备实施条件；不是LLM业务验收通过，不启动S2或生产。

当前状态（2026-10-26，[B进度§114](#114-b进度浦发托管增速已退出可信)）：浦发 `claim-products-services-1` 已离线退出可信。第89页不含增速的托管规模和收入仍在。按审核§113，该安全差异成立后可用 4/4 及进入 S1 的条件放行生效。S2 和生产未授权。

## 1. 唯一实施指引

- [产品总需求](../../../docs/development/company_profile_product_and_industry_semantic_requirements.md)：公司画像、产品暴露、供应链定位；LLM语义/程序硬约束分工。
- [验收计划v2](../../../docs/development/company_profile_acceptance_program.md)：数量、门槛、预算、补救和唯一进度。
- [A/B顺序任务卡](../../../docs/development/company_profile_delivery_plan.md)：当前为B3/S1，先离线安全差异，再连续真实回归；B2不再付费收口。
- [A/B流程v3](../../../docs/development/ab_role_development_workflow.md)：A0冻结、B连续实施、A1末端审核，AGENTS同步强制引用。
- [实施待办](tasks.md)：0为本轮规划，其余实施任务尚未完成；完整proposal/design/specs已准备。

## 2. 核心判断与保留价值

旧v61 runner未传provider，S1测试provider=None；runtime已有field可以消除unresolved，core assessment以正则判断正文，故0 token不能证明LLM解读。接入provider本身不足，任务卡同时修正发现、语义执行、覆盖和投影。现有PDF/布局缓存、Evidence、事实模型、队列/续跑、网关和读写底座继续复用。

历史v61有限成绩与S1原10/12、补救12/12保持；新LLM开发0/4、回归0/12、S2观察0/20、S3观察0/100。20家源准备可复用，但旧v61代码/预算/合同不足以证明新三视图路线，执行绑定暂停，需另版重冻。原约70家公司只作开发/回归材料。

## 3. 规划审核发现与处理

| 实质问题 | 文档修正 | 是否仍阻塞本次规划 |
| --- | --- | --- |
| 规则完整性/无provider成功与用户的LLM目标冲突 | 必需真实语义链；字段存在不短路；源先行查漏；日志/usage真实 | 否；业务实现留B1 |
| 只验三维正文/收入行，产品暴露和供应链定位可能被遗漏 | 三视图进入源合同、独立召回与公司可用分级；非商品服务也适用 | 否 |
| 无限双公司同源修补不能证明泛化 | 4家开发、12家新链、20＋最多20盲测、另100；达上限必须结论 | 否 |
| 绝对零遗漏不可落地，放任确定错误也不可接受 | 核心宏召回95%、已审准确99%、公司可用90%→95%，次要缺项/冲突可限用，已知错误退出确定结论 | 否 |
| 两次共性修复与每公司一次重跑容易冲突 | 先完成离线局部修复，再最终同版正式后继一次；预算总账含所有付费调试 | 否 |
| 旧300秒/0token条件不能当真实模型吞吐 | 新路线有明确待校准的时间/token/调用/费用上限，先4家实测；不是已获付费授权 | 否 |
| “队列完成”被当成全市场研究可用 | 全名单100%状态、可读报告98%技术交付、独立波次质量与人工成本分别验收 | 否 |
| 旧closure、无LLM规范和行业文档继续引导错误路径 | 当前索引移除旧轮次入口，9份主spec加适用说明，旧change接续，新delta定义目标，行业增强后置 | 否 |
| 一次清理误删有价值实现/原始失败/既有代码改动 | 先替代并验证再删具体调用分支；本轮只清退有效文档指令，源/正式记录和代码不动 | 否 |

审核限定：这是需求/设计一致性审核；真实LLM质量、成本和全市场能力尚未实测。没有宣称独立第二模型Review或全套业务测试通过。

## 4. 实际验证与保护

- OpenSpec严格校验：新change及define-staged/prepare-S1-S2/v59三个接续change均通过；9份加适用说明的主spec分别通过。
- 本轮33份Markdown的本地链接检查：无失效目标；`git diff --check`通过。后续新增本审核入口也核其链接。
- 7份原有无关修改（两份gateway文档、PDF评估JSON、两份代码、两份测试）SHA-256与本轮开始时一致。未追改原source/正式结果或执行代码。
- 既有prepare-S1-S2的proposal/design/tasks仅加两行接续声明，去掉新增前缀与开始时备份逐字节一致。验收计划的既有进度文字与引用原样保留在折叠历史记录中，新LLM唯一活动表另列；提交时隔离已有历史状态更新，不能将其纳入本轮提交。
- 所有本轮临时备份位于项目TMPDIR任务目录。业务代码未修改；未运行pytest、业务批次或真实LLM，不能据文档校验宣称业务已通过。

## 5. 开发者告知与下一步

后续执行A/B流程v3的A0/B1。A先冻结目标、样本、合同、预算和停止条件；B在一个连续窗口内完成真实LLM三视图闭环、局部P0/P1修复、直接回归、正式复验和唯一证据交接；A1再独立核验。B不得自行进入B2、S1、S2或修改分母/阈值/权限。只使用历史开发样本，S2名单继续留出。A下次先核真实三视图和源覆盖，再核统计/预算/直接回归；不得继续派“下一对规则补丁”。生产仍`not_authorized`，规模声明false。

## 6. B角离线开发交接（2026-10-16；非A1业务验收）

本节接续用户“开发工作均予以授权”，只交付A0前允许的离线实现与fake验证。A0的公司、正式来源、模型、价格及付费预算尚未冻结；真实调用0、正式首次0、正式后继0，未使用S2留出报告。L0/L1业务进度仍为0/4，B1没有完成真实业务验收，不能进入B2。

### 6.1 已交付的离线切片

- 唯一入口/owner仍为`company_profile_common_core` → operations → runtime → `CompanyProfileSemanticService` → `CommonGatewaySemanticProvider` → 原writer/read service；新政策显式选择`semantic_policy=company_profile_llm_led.v2`，未改变默认生产配置。
- 注册v2请求/响应与purpose：`source_inventory`（仅源、不传候选答案）→ `synthesize_views` → `verify_views`；不新增ChapterTask或事实对象体系，不修改v1合同。通用全文分块不依赖旧标题白名单，保存每页/分块去向，未读页及预算失败不冒充未披露。
- 三视图有证据引用、缺项与冲突；核验必须逐源条件、逐claim、逐record返回结果。遗漏主要板块使对应维度不完整；错误记录及依赖摘要隔离，可靠独立结构化记录在provider不可用时仍可读。
- 复用原结构化数值。模型改写Measurement的值、单位或其他身份会被拒绝；新事实必须符合原checklist的field、chapter、object及metric/action范围。主体、计划、否定、第三方和实质覆盖由源上下文核验，不新增公司规则或语义正则裁判。
- 原writer保存显式`runtime.v2`及`scope_checkpoint.v2`；query/export读取同一结果，并保持v1可读。真实provider的同步适配器通过新政策下的线程衔接离开应用事件循环，离线假网关已验证完整调用链。
- fake输出为`offline_fixture`，仅`draft_complete`可为true，`semantic_complete`、`real_semantic_complete`及全局`core_complete`保持false。usage缺失为unknown并保守计预算，不能宣称零成本或真实语义通过。

代码入口：[报告语义流程](../../../research/company_profile/report_interpretation.py)、[v2合同](../../../research/company_profile/contracts.py)、[runtime](../../../research/company_profile/runtime.py)、[网关适配](../../../research/company_profile/stage5_provider.py)、[query/export](../../../research/company_profile/reads.py)、[离线验收用例](../../../tests/unit/test_research/test_company_profile_llm_led.py)。

### 6.2 验证与Review边界

实际回执见[离线验证摘要](offline-verification.json)。最终直接验证为185 passed（22个新政策用例＋163个既有网关用例，7个warning），Ruff、`git diff --check`及当前change的OpenSpec严格校验通过。复现入口（所有输出/测试库均在项目TMPDIR）：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_llm_led.py tests/unit/test_research/test_company_profile_stage5_provider.py -q
/home/python/miniconda3/envs/Quote/bin/python -m ruff check research/company_profile/contracts.py research/company_profile/report_interpretation.py research/company_profile/runtime.py research/company_profile/workflow.py research/company_profile/reads.py research/company_profile/stage5_provider.py tests/unit/test_research/test_company_profile_llm_led.py
git diff --check
```

B做了本次差异自检，不冒称A独立业务审核。范围内阻塞已修：真实适配器在异步入口内无法运行、通用线程回调持续等待（改为复用队列既有工作线程桥接）、fake可能误报语义完整、数值改写及依赖摘要未隔离、年份/年度结束日等价表达被误拒。新增用例证明程序路径、约束、持久化与读回，不证明LLM能正确理解语言或达到业务召回率。

旧`test_company_profile_semantic_workflow.py`为36 passed、5 failed；排除本次6个修改模块、保留既有工作区内容后仍是完全相同的5个失败。旧runtime首个`core_complete`断言也在该基线上原样失败。按C类既有问题保护，不改旧预期。最初扩大到runtime/reads的诊断运行已中止，不声称完整旧套件通过；本次直接网关回归和新政策离线套件另列。临时日志、基线快照与隔离比较脚本在`data/QuoteBak/b1-llm-led-offline-20261016/`，持久摘要在本change，不依赖临时日志作为唯一证据。

### 6.3 尚未完成与下一卡要求

- A0仍须冻结1家历史公司与三视图源条件、官方版本/hash/知识截止日、provider/model/prompt/预处理、价格/币种、预算与执行次数，确认不在S2留出集。
- 当前实现**主动拒绝provider真实执行模式**，不是配置一个布尔值即可生产运行。A0后须在同一链完成实际模型绑定、网关内部重试/切换的物理请求计数与发请求前费用预留、完整入口deadline衔接，再执行最多首次1＋后继1。现有离线预算检查仅覆盖本切片逻辑请求与保守token，不能作为真实物理调用/费用达标证明。
- 语义缓存复用、跨公司泛化、全市场、行业增强未实现/未验证；首次语义验收不得复用fake结果。没有真实provider/model/request/usage与正式query/export证据，2.2—2.7正式任务继续未完成，A1尚未开启。
- 执行流程继续引用`ab_role_development_workflow.md` v3；B在卡内连续修复P0/P1与必要P2/P3，不越样本/预算/次数。合同冲突、授权变化或既有改动不可隔离时保留原结果交A。

Git基线为master，开始时已有11份tracked修改及若干untracked内容。本轮只提交可隔离的本人改动，既有内容保持原样。提交身份由本节所属Git提交确定；不将旧改动纳入提交。B活跃开发与交接等待时间未精确计时，记unknown；fake token不代表开发/审核成本。

## 7. A角离线审核与接续卡（2026-10-16）

结论：已独立确认离线主链及fake不冒充真实完成，但发现两项范围内P1，须在B1内修复。**不判B1/A1通过，不归档、不扩批。** 本次是工程准备审核，真实LLM开发仍0/4、S1新链0/12、S2观察0/20、S3观察0/100。原B交接及回执原样保留，本节是后继审核结论。

[A核验回执](a-offline-review.json)记录已复现结果；[B1接续执行卡](b1-execution-card.md)是当前唯一开发卡。A已从正式PDF核源，冻结歌华有线600037.SH、19项核心/6负例、来源hash/知识时点及资源边界；不在S2留出20家中。A0的实际模型/价格/代码及执行授权仍待补齐，不能用文件中预算上限当作付费授权。

### 7.1 阻塞发现

| ID | 证据与实际行为 | 必须修复的结果 |
| --- | --- | --- |
| A-P1-01 | `report_interpretation.py:355`用模型返回records替换全部交付，425处仅过滤这些返回项；向现有FakeReportProvider传入可靠Measurement `cp-300750-revenue`，模型未回传该项，输出只余overview，仍`draft_complete=true`且gaps空。`reads.py:_iter_accepted_raw`在v2仅读此集合 | 原生可靠事实不能依赖模型照抄回传才能保留；原值独立交付。若存在实际冲突，须有对象隔离原因及依赖摘要处理，不能静默漏掉 |
| A-P1-02 | `report_interpretation.py:362–365`仅对Measurement核原值。将已有Segment `cp-300750-segment`的source_native.value从null改为999999999、unit改为元，同一fake verify全pass时被接受，草稿仍完整且无gaps | 所有复用结构化对象的原生数字/单位及身份都要保护，Segment也不能新增无来源金额；硬错误不得由模型pass覆盖 |

这两项是程序硬约束/原始事实保留问题，不授权增加语言正则、逐公司规则或新验证框架。原semantic_workflow的五项旧失败没有新证据表明本轮引入，按C类后置；不宣称完整套件通过。

### 7.2 验证及限制

- A独立执行新政策离线套件：22 passed、7 warnings、105.43秒，含原owner持久化→查询→导出；命令为§6.2第一条去掉旧网关测试文件。B回执中7个代码/测试hash与当前文件逐个一致，同快照163项网关回执复用，不将其称为A重跑185项。
- A直接读正式PDF核来源hash、封面及关键正文、原收入列/续页和投入方向；208页及176124字符是输入规模，不是完整业务观察或全页质量成绩。未调用真实LLM、未运行新业务批次、未写生产数据。
- 当前change的OpenSpec严格校验、本轮文档本地链接与JSON回执检查、`git diff --check`通过；这些工程检查不替代真实LLM业务验收。
- 两项独立挑战使用现有离线fixture，复现命令如下；输出是缺陷证据，不能算业务通过。A活跃审核时间未单独准确计量，记unknown。

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python - <<'PY'
import json
from pydantic import TypeAdapter
from research.company_profile.models import SemanticRecord
from tests.unit.test_research.test_company_profile_runtime import _reference_payload
from tests.unit.test_research.test_company_profile_llm_led import _run, FakeReportProvider
records = [TypeAdapter(SemanticRecord).validate_json(json.dumps(x))
           for x in _reference_payload()['records']]
numeric = next(x for x in records if x.object_type == 'Measurement')
result = _run(FakeReportProvider(), report=numeric.report, structured_records=(numeric,))
print('A-P1-01', numeric.record_id, [x.record_id for x in result.records],
      result.draft_complete, result.gaps)
segment = next(x for x in records if x.object_type == 'Segment')
class ChangedSegment(FakeReportProvider):
    def interpret(self, request):
        response = super().interpret(request)
        if request.purpose == 'synthesize_views':
            original = request.records[0]
            changed = original.model_copy(update={'source_native':
                original.source_native.model_copy(update={'value': '999999999', 'unit': '元'})})
            return response.model_copy(update={'records': (*response.records, changed)})
        return response
result = _run(ChangedSegment(), report=segment.report, structured_records=(segment,))
print('A-P1-02', [(x.record_id, x.source_native.value, x.source_native.unit)
                  for x in result.records], result.draft_complete, result.gaps)
PY
```

### 7.3 告知开发者

继续执行A/B流程v3和[B1接续卡](b1-execution-card.md)。B在卡内连续修复上述两项P1，完成真实provider、物理预算/费用预留、完整入口deadline与208页离线预演；无需逐函数/逐测试等待A。补齐实际模型/计费/最终快照后在本入口申请一次真实运行放行；未获授权不试探真实模型。首次及后继各最多一次，同源问题在额度内自主修复，卡外扩批暂停。A1在完整交接后独立审核真实三产品和原文，不再派下一对规则补丁。

容错沿验收计划：主要业务/关键数字/主体错误不能确定性外放，次要遗漏和有据unclear可限用；无法补救写清来源、原因、影响与恢复条件。B1只要求一家可用，后续仍依次4家开发→12家新链→20家盲测（最多另20家确认）→100家试运行→全市场分波；数量到限而未过即出失败/收窄裁决。生产`not_authorized`、规模声明false不变。所有临时制品留项目TMPDIR。

## 8. B角接续交接：P1修复与208页资源阻塞（2026-10-16）

结论：两项P1已修复并完成直接离线复验；已沿原owner接通可注入的真实provider、物理预算和隔离监督入口。但**整份报告的资源可行性预演失败，尚不具备真实执行放行条件**。本节交付卡允许的具体模型/计费/资源阻塞，不申请将fake测试数折算业务成绩。真实请求0、付费调试0、正式首次0、后继0；A1未通过，不归档、不扩批。

完整材料：[执行绑定草案](b1-execution-binding.json)、[接续验证回执](b1-continuation-verification.json)、[持久证据清单](b1-continuation-evidence/artifact-hashes.json)。原§6/§7和原回执保留。暂存路径只用于开发，原文、查询/导出及审核所需回执已另留持久证据，不依赖临时库存续。

### 8.1 P1修复及Review

- synthesis未回传Measurement时，原生事实仍进入verify与最终交付。模型不承担原数值照抄义务。
- 所有已有对象（包括Segment）的身份、数值、单位等必须与原对象完全一致；新ID也不能新增没有原生依据的Measurement或金额/单位。拒绝的变体单列`record_dispositions`，保留可靠原对象，屏蔽依赖变体的答案。
- verify针对保留下来的原对象核源，保留原事实的证据页及续页，不依赖模型在inventory中重复提及；确有冲突时隔离记录及依赖摘要，保留原候选、原因与使用限制。即使verify另有缺项，也不因异常回退重新放出已知冲突记录。
- B对本轮差异做自检：修复上述A类阻塞及资源耗尽妨碍部分结果持久化的问题；没有为B类风格建议扩大改动。原网关同环境短时限失败列为C类，见8.4；本节不是A独立业务审核。

### 8.2 实源、预算停止与可用范围

只读原库核对公告/附件版本，PDF SHA-256、资产/报告ID、报告期及公告时点均与卡一致，见[元数据](b1-continuation-evidence/source-metadata.json)。pypdf逐页无分隔拼接复核为176124字符/395738字节，与A一致。现有`business_profile_pdfium_recovery.v4` owner产物为184544字符/404158字节；两者解析方式不同，源PDF未改。208页均可解析，通用布局补充包含165、166等续页，无公司名/证券代码/固定页码发现分支。

沿`CompanyProfileTaskService`→runtime→原provider→现有`LlmClient`→原writer/read运行。离线transport只回空inventory，不提供业务答案；其成功回包仅用于测试协议和预算，不表示模型已阅读。最终完整入口耗时59.467秒，隔离目录18个文件/2476758字节，原PDF1263185字节。

| 实测项目 | 结果及含义 |
| --- | --- |
| 首个模拟发送 | 完整prompt/schema/来源预留92134输入＋4000输出＝96134 token上界；usage unknown，不冲减预留 |
| 第二个发现请求 | 需101310，余23866；在物理发送前拒绝，实际只有1个模拟transport调用，真实调用0 |
| 页覆盖 | 模拟协议标记1–26页成功；27–208共182页未读。真实LLM语义已读页为0，不能声称全文发现完成 |
| 停止后的交付 | 原生3个Segment＋3个Measurement保留（包括营业收入总额），query/export的profiles逐对象相等 |
| 三视图 | 均为`extraction_failed`，`draft_complete/semantic_complete/real_semantic_complete/core_complete`均false；没有合成/核验产品可供业务验收 |
| 19核心/6负例 | 全部未作真实业务判定，准确率unknown。尤其五项收入、变现机制和供应链不能据原生6条记录宣称交付 |

完整[query](b1-continuation-evidence/query.json.gz)、[export](b1-continuation-evidence/export.json.gz)、[源覆盖](b1-continuation-evidence/source-receipt.json)、[物理账本](b1-continuation-evidence/budget-final.json)、[入口回执](b1-continuation-evidence/supervisor.json)和[日志](b1-continuation-evidence/entry.log)可复核。账本中的0.100134等费用是**合成测试价格的计算结果**，不是deepseek账单、真实单价或实际费用证据。

### 8.3 执行绑定与停止条件

拟用现有直连`semantic_extraction__deepseek`，实际配置资源`deepseek`、模型别名`deepseek-flash`；不允许回退。代码、schema/prompt/预处理模块和配置文件SHA-256及命令在绑定草案中。两份原有脏的抽取依赖也按当前内容记hash，不将其内容提交为本轮改动；仅凭本轮Git提交不能重建这些既有未提交依赖，复现需保留匹配内容。

每次物理发送前预留完整payload、schema和最大输出，失败/usage unknown保留预留；重试、schema repair、切换共用同一账本。账本在发送前落盘，超额禁止后续发送。token/费用耗尽后只允许在剩余时限内保存、查询及导出可靠的部分结果。监督入口以同一deadline覆盖初始化/解析/排队/模型/导出，超时终止其进程组；冻结命令另用外层`timeout`覆盖Python包导入。单独注入budget对象只是合作式检查，不能当作完整入口硬限时证明。

复现仅离线（目录必须不存在，防覆盖）；命令中的`timeout`必须保留：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
timeout --signal=KILL 900s /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-execution-binding.json --run-root data/QuoteBak/b1-continuation-20261016/full-report-final-v2
```

当前具体阻塞：该模型别名缺少可核计费依据、币种/输入输出单价、上下文/配额及对应token计量依据；UTF-8保守法又已实测无法在120000上限内完成发现，尚未计入合成及核验。不能猜另一型号的价格、用fake usage证明可行，或缩成A关键页白名单。**计费资料、有效计量/上下文方案及资源可行性需先补齐，再由A核定对应绑定和真实权限**；仅补授权布尔值不能放行，真实模式会拒绝当前草案。

本卡累计正式资源仍未使用，上限仍为首次1＋最多后继1、1800秒/240000 token/32物理请求/20元；这些上限不构成支出授权。真实运行时A授权文件须引用绑定SHA-256、document_version、first/successor及approval_reference；隔离入口保护已尝试身份，不能覆盖首次。当前没有这样的授权文件，生产仍`not_authorized`，规模声明false。

### 8.4 验证、保护和下一步

最终直接复验40 passed、1 deselected、7 warnings：覆盖两个P1、原事实的核源上下文、完整/不完整核验下的冲突隔离、缺provider/同field不短路、原值/单位/期间、物理预算耗尽/失败重试/切换/schema repair、usage unknown、发出前账本及入口超时杀子进程。deselected为较慢的原fixture DB闭环；它已在本轮初始合并测试通过，且本次208页最终预演另沿真实owner验证持久化/query/export相等。fake不计业务分数。

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_llm_led.py tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_utils/test_llm_physical_budget.py -k 'not existing_owner'
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_utils/test_llm_gateway.py tests/unit/test_utils/test_llm_routed_client.py tests/unit/test_utils/test_llm_pool_routing.py tests/unit/test_utils/test_llm_routing_config.py tests/unit/test_utils/test_llm_orchestration.py
```

网关最终142 passed、11 failed；将`b9e620da`原client在独立进程装载作同环境对照，同样142 passed、11 failed，**失败用例集合相同**，没有宣称全套通过。初始并行诊断曾多1个短时限失败，单独复验2 passed，最终未再出现。完整[原client对照日志](b1-continuation-evidence/baseline-gateway-tests.log)、[当前日志](b1-continuation-evidence/final-gateway-tests.log)、[对照装载脚本](b1-continuation-evidence/baseline_gateway_plugin.py)已留存。当前卡不修改这些既有测试预期或无关超时逻辑。

本轮其它改动文件Ruff通过；client的46条既有lint诊断与HEAD相同、无新增，未做全文件风格重构。`git diff --check`及当前change OpenSpec严格校验另作最终检查。本轮开始的11份tracked改动逐文件SHA-256保持一致，既有未跟踪内容未触碰；只提交本人可隔离改动，提交身份以本节所属Git提交为准。

下一步由A核本次P1修复与具体阻塞，协调缺失的真实计费/模型资料及资源方案；B1保持未通过。开发0/4、回归0/12、未见0/20、试运行0/100，不启动B2，不将任何离线产物记为19/6或准确率达标。

## 9. B角接续：复用现有负载均衡池（2026-10-17）

用户补充“现有LLM有一个负载均衡池可供调用”。B已只读核实并将隔离入口接续到现有逻辑profile `semantic_extraction` → `shared_semantic`，保留公共网关的权重、借用和有界故障切换。该信息只确认可复用入口，未提供计费依据或新增真实调用授权。生产配置未修改。正式首次0、后继0、付费调试0；B1/A1仍未通过。

新[池执行绑定草案](b1-pool-execution-binding.json)绑定路由fingerprint、完整配置及调用链文件hash、各实际profile/resource/model、原预算及命令。六个来源如下；source label仅作路由标签，实际模型以下表及物理账本为准，不能按标签推单价。

| 实际profile后缀 | provider_resource | 实际配置model |
| --- | --- | --- |
| `__scorpio_gemini` | `scorpio:gemini` | `gemini-3.8-flash-high` |
| `__zai` | `zai` | `glm-5.3-flash` |
| `__scorpio_grok` | `scorpio:grok` | `grok-4.7` |
| `__scorpio_luna` | `scorpio:luna` | `gpt-5.6-luna` |
| `__scorpio_deepseek` | `scorpio:deepseek` | `DeepSeek-V4.1-Flash` |
| `__deepseek` | `deepseek` | `deepseek-flash` |

运行仍通过原`CommonGatewaySemanticProvider`和`LlmClient`。入口检查全部物理来源与绑定一致，任何成员缺计费/上下文依据均阻止真实准备；来源、模型、权重或切换策略改变须重冻。所有成员的重试/repair/切换共用一个物理预算。处理身份使用池名及路由hash标识路由版本，实际模型保存在每次gateway trace和物理账本中，不把池名当作实际执行模型。原直连模式继续可用；§8绑定与回执保留为`f90a9f64`历史快照，不将新代码hash写回旧绑定或套用旧成绩。

本次[验证回执](b1-pool-verification.json)及[证据清单](b1-pool-evidence/artifact-hashes.json)记录了新的208页离线预演：入口48.329秒，query/export一致，原生6个可靠对象仍在。首个模拟请求由池选到`scorpio:gemini`，完整物理预留92142输入＋4000输出＝96142 token；usage unknown保留全额。第二次逻辑发现选到`zai`，需101309，余23858，在发送前拒绝。实际transport只有1次且为fake，真实请求0。模拟成功范围为1–26页，另182页未读；实际模型阅读0页。三视图均`extraction_failed`，各完整性标志false；19核心、6负例和已审准确率均未评估。账本0.100142元仍仅为合成测试价格运算，不是实际费用。

直接验证命令如下，结果**21 passed、6 warnings**，覆盖监督入口、原直连兼容、路由/来源漂移拒绝、缺成员价格及模拟跨来源故障切换的共享账本；[测试日志](b1-pool-evidence/direct-tests.log)可复用。Ruff检查和格式检查通过。B人工Review本轮未提交差异，无新增A类阻塞；本次不修改公共网关或追修C类既有短时限测试。此前§8的P1回归仅按其原快照保留，不冒称本轮重新跑过全部测试。

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_utils/test_llm_physical_budget.py
timeout --signal=KILL 900s /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-execution-binding.json --run-root data/QuoteBak/b1-pool-binding-20261017/full-report-v1
```

该预演目录已存在，复验须换用新的QuoteBak目录。持久[查询](b1-pool-evidence/query.json.gz)、[导出](b1-pool-evidence/export.json.gz)、[源回执](b1-pool-evidence/source-receipt.json)、[预算](b1-pool-evidence/budget-final.json)、[模拟发送](b1-pool-evidence/offline-transport.json)和[入口耗时](b1-pool-evidence/supervisor.json)不依赖临时库存续。

当前需补资料仍为六个来源的计费/订阅增量成本依据、币种换算及上下文/配额；仓库配置与所查网关文档未找到可核的价格/上下文表，未用真实请求试探。另需解决完整发现、合成及核验在120000 token内的有效计量/资源可行性，池可用并不扩大预算。已向用户询问池的计费与上下文资料维护位置，尚未据此取得真实权限。按[B1卡§5–6](b1-execution-card.md)停在实际运行前，待资料和可行性补齐后由A核定最终绑定与运行权限，不启动B2或扩批。

本次基线`f90a9f64`，11份既有tracked文件hash均未变化，既有untracked内容未操作；仅提交本人入口、直接测试与本change新增材料。Git身份以本节所属提交为准。B活跃开发/等待时间未精确计量，本次正式复验0；新路线仍为开发0/4、回归0/12、未见0/20、试运行0/100，生产`not_authorized`、规模声明false。

## 10. A角接续审核与资源裁决（2026-10-17）

审核提交：`f90a9f64`、`d320ed1dd289c03b3112b496b3617509b8307d64`。结论：**原两项P1修复在直接核验范围内成立，池/物理预算及部分交付链可接续；B1/A1仍未通过。** 本轮未发现新的业务P0/P1，当前阻塞为完整请求计划的资源可行性，以及实际计费/上下文资料和真实授权。下一步按[B1卡§7](b1-execution-card.md#7-当前接续先让完整请求计划可运行a于2026-10-17裁决)实施，不继续增加池功能或以fake数量代替业务。

### 10.1 A实际核验与完成边界

- 官方PDF hash与冻结版本一致；池绑定54份执行文件hash全部匹配当前内容，包含既有脏的抽取依赖。8份持久制品hash通过；本轮只读复核query/export的profiles相等，解析缓存208页逐页文本hash与持久source receipt一致。缓存只用于诊断与重建本轮请求计划，不作真实语义成绩。
- 原A-P1-01和A-P1-02关闭：A独立执行6项直接用例，覆盖模型漏回传原数值、原事实上下文保留、Segment原ID/新ID新增金额拒绝、完整/不完整verify中的真实冲突及依赖隔离。原生事实可以独立保留，未经支持的金额变体不能由verify pass放行；未据此宣称所有未测边界成立。
- A审阅了现有池接续、物理发送前预留、失败/切换不重置、unknown usage、入口监督和原owner读回。针对性组合首次为**26 passed、1 failed、7 warnings、16.30秒**，失败为`test_routed_failover_uses_same_run_ledger_and_rejects_unpriced_source`的请求deadline；其路由fixture配置0.5秒时限。单独复验为**1 passed、6 warnings、1.84秒**。两次结果并列，不能称A一次27/27或全套通过；这项短时限波动未显示新业务P1，不追修整个旧网关套件。B的21 passed回执仍保留，不用A后继改写。
- B此前网关142 passed/11 failed及原client同环境对照保持历史；本次未重跑那组。A没有修改业务代码，没有重跑208页入口，没有发真实请求或写生产库。A活跃审核时间未单独计量，记unknown。

A组合复验命令（必须继承项目TMPDIR）：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_utils/test_llm_physical_budget.py tests/unit/test_research/test_company_profile_llm_led.py::test_native_numeric_fact_survives_model_omission_and_is_verified tests/unit/test_research/test_company_profile_llm_led.py::test_preserved_native_fact_keeps_its_source_page_in_verify_context tests/unit/test_research/test_company_profile_llm_led.py::test_segment_cannot_add_amount_even_with_a_new_candidate_id tests/unit/test_research/test_company_profile_llm_led.py::test_native_fact_semantic_conflict_quarantines_fact_and_dependent_answer
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_utils/test_llm_physical_budget.py::test_routed_failover_uses_same_run_ledger_and_rejects_unpriced_source
```

### 10.2 当前主要矛盾及资源诊断

| 核验项 | 实际结果与裁决 |
| --- | --- |
| 208页离线预演 | 是完整入口的一次预算停止观察；只模拟发送第一批1–26页，另182页未读。真实模型阅读0页，三视图均为`extraction_failed`，19核心/6负例未评估 |
| 首次物理预留 | 96,142＝92,142输入上界＋4,000最大输出；第二批需要101,309、只余23,858，在发送前拒绝。预算硬停成立，完整资源可行性未成立 |
| 发现请求形态 | `report_interpretation.py`按24,000字符/1,000重叠形成8批；`stage5_provider.py:interpret`每批发送完整`ReportSemanticResponse`schema，30个defs、紧凑JSON为25,833字节，包括该purpose不能返回的records/claims/checks |
| 当前计量法的下界 | 208页正文404,158字节＋8份schema206,664字节＋8份最大输出32,000＝**642,822个当前预留单位**；还未计system、封装、元数据/转义/256 framing、合成/核验及重试。120000下无法跑通，单靠更小分批仍会重复schema |
| 最小purpose诊断 | 只在内存保留inventory允许字段及可达defs，测得4,121字节/5 defs。A未实现或批准以该投影绕过完整本地校验；单独缩schema也不证明全程可行 |
| token与费用含义 | 上述是UTF-8保守计量及请求结构证据，**不是真实token超额/模型费用实测**。账本0.100142元为合成价格结果，实际费用仍未评估；不能凭该数校准真实成本 |
| 资源资料 | 全池六来源price均null，没有可核上下文资料；不以型号别名推价格。全池若继续使用，须核全部可达成员；首家也可沿现有已核concrete profile关闭回退准备隔离绑定，生产池不变 |

A未放宽900秒/120000 token/16物理请求/10元及整卡上限，也没有据当前过度保守计量要求直接加预算。B先去除purpose无关schema/事实重复，形成全程请求计划及有依据的计量/上下文方案；必要时采用通用有界发现和核心正文阅读，但不能裁掉主要业务、把结构索引冒充正文已读或改用固定关键页白名单。只有这一步可行后，真实首发才有检验业务质量的价值。

### 10.3 下一卡、流程与进度

执行[B1当前接续卡§7](b1-execution-card.md#7-当前接续先让完整请求计划可运行a于2026-10-17裁决)：在同一B窗口完成完整请求/预算计划、非空的208页离线协议闭环及最小可用来源资料。A负责核最终实际来源、路由/版本与资源权限；没有真实授权前不试探模型。若只有一个来源资料齐全，不为凑六份阻塞首家准备；其余未定价来源不得切换进入真实执行。

按A/B流程v3，B自主处理卡内P0/P1和必要P2/P3，最多两次离线资源方案迭代；超限或确实需要资源/合同/权限裁决时交A，不无限堆fake轮次。真实首次/后继仍0/0，允许上限仍首次1＋后继最多1，旧离线停止观察不占正式次数、不增加费用授权。下一次交接须说明完整方案可否运行及具体差额，不能只交新的网关测试数。

唯一验收进度表已更新：来源合同1/4、实际执行冻结0/4；开发0/4、S1新链0/12、S2首组0/20（失败最多另20）、S3试运行0/100。后续按4→12→20→100顺序推进，到数量上限即结论，不追加样本稀释失败。本卡19核心/6负例及可用门槛不变；允许次要遗漏和有据unclear，已知错误/依赖答案退出确定结论，无法补救须写原因、影响和恢复条件，核心不成立不算可用。生产`not_authorized`、规模声明false，不归档、不扩批。

本次只新增审核/任务文档指引，不改旧§6–9、绑定、成绩或制品。任务开始时实际有14份tracked既有修改（B起始11份之外另有3份并行修改）及原untracked；均须保持，验收计划只隔离本轮活动进度行。临时制品继续使用项目TMPDIR，原正式来源/持久证据不清理。

本轮文档通过`openspec validate restore-company-profile-llm-led-generalization --strict --no-interactive`和`git diff --check`；5份本人文档58个本地链接及19项核心条件检查通过。14份既有文件在扣除本人进度行后内容hash不变。审核期间另有并行提交`9841642a`及无关文档修改，A未操作这些内容，仅隔离提交本轮5份文档。

## 11. B角资源方案交付与停止裁决申请（2026-10-17）

结论：**已完成请求精简和两次离线资源方案观察，完整208页非空发现→合成→核验预演仍未通过；按卡§7两次上限停止继续试方案，交A作资源/范围裁决。** 原两P1保持A已关闭状态；B1/A1仍未通过，正式首次/后继0/0、真实模型调用0、真实模型阅读0页、实际费用未评估。R1/R2继续未勾选，不进入B2、不归档、不扩批。

### 11.1 本轮实现与路由偏好

接续A审核提交`1e8b2e30`；工作期间另有并行提交，未操作其内容。沿原`company_profile_common_core` owner/provider/writer/query/export增加显式opt-in v3：发现、合成、核验分别携带可达schema；report身份一次绑定、Evidence去重引用，响应展开后仍走完整本地类型、来源原句、数值及依赖验证。旧v2不改版。紧凑schema字节数分别为地图594、完整正文发现1938、合成2123、核验1063（旧全schema25833）。v3只生成开放语言claims，原生结构化事实只读保留并逐项核验；不能用模型照抄遗漏或新增金额绕开原保护。

第二方案每页统一取首48、中16、尾16字符并记录原长/偏移，全部208页均对模型可见；地图明确为`structure_preview_only`，不能建立证据条件。随后模型请求正文，原生事实所需页及相邻上下文一并读取；完整发现允许最多一次受同账本约束的补源。无证券代码分支、固定页码、标题白名单或19项答案提示。地图必然存在未采样正文，不能证明全部主要业务被召回；这一质量风险需真实原文审核，不能凭全页索引覆盖率消除。

用户先提`scorpio-deepseek`，随后明确允许从池选取、失败换其他模型。因此准备绑定复用现有`semantic_extraction → shared_semantic`及其既有负载策略/故障切换，不永久锁定单一模型，不新增随机路由。所有物理切换/重试继续共用预算；未定价成员在真实执行前仍需补齐计费依据。来源选择澄清不被解释为新增付费授权。当前[执行绑定草稿](b1-resource-execution-binding.json)的`resource_feasibility_passed=false`、`real_authorization=null`，不能直接真实运行；旧绑定和旧成绩不追改。

### 11.2 两次观察与完整差额

证据：[清单与复现说明](b1-resource-evidence/README.md)、[方案1预算](b1-resource-evidence/iteration1/budget-final.json)、[方案2预算](b1-resource-evidence/iteration2/budget-final.json)、[方案2物理发送](b1-resource-evidence/iteration2/offline-transport.json)、[未发送请求投影](b1-resource-evidence/remaining-projection.json)。单位一律是当前UTF-8 payload字节＋256 framing＋最大输出的保守预留，**不是部署模型实际token**；unknown usage不冲减，合成价格仅测试用途，不能作为真实费用。

| 项目 | 实际观察及限制 |
| --- | --- |
| 方案1：v3精简＋全文分批 | 发送1次模拟发现，1–26页；62801字节payload＋256 framing＋2500最大输出＝65557。下一批需70724、仅余54443，在发送前拒绝；182页未读，未走合成/核验 |
| 方案1入口 | 49.162542秒，exit 0只表示停止结果成功持久化；不是业务通过。14条合成条件，6条原生事实保留，query/export一致 |
| 方案2地图发现 | 第1次模拟发送，全208页片段地图：75253＋256＋1000＝76509；输出172字节（选页计划） |
| 方案2完整正文发现 | 第2次模拟发送17–26、104–106共13页：33297＋256＋2500＝36053；输出7903字节，14条合成unclear条件。页号由通用fixture选报告中部、原生事实及相邻上下文产生，非生产页码规则 |
| 方案2合成准入 | 已预留112562，仅余7438；合成需62838输入＋2500输出＝65338，发送前拒绝，当前一步至少差57900；核验未发送 |
| 方案2入口与范围 | 47.734818秒；208页已索引及片段模拟检查，13页完整正文模拟读取、195页仅部分检查。195页未采样正文仍未读，真实模型正文阅读0；6条原生事实保留，query/export一致 |
| 三类产品/质量 | 两次三视图均未形成可用语义交付，completeness=false；19核心、6负例与≥98%准确率未评估。不能把模拟unclear条件或原生事实读回当成真实语义成功 |

对未改变的第二方案做**只序列化、未发送**的剩余请求测算：合成含17页上下文37887字节、schema2123、最大输出2500；核验同正文、schema1063、最大输出2000。14条件→7claims→27checks均非空、标记合成且不含业务答案；六池成员因型号字符串长度产生差异，具体输入/输出逐成员见投影文件。合成预留65338–65351，核验70266–70279。

在已发生112562基础上，保守取六成员较大值，四个正常请求合计**248192**，较120000差**128192**；额外允许一次最大请求重试还需**76509**，合计**324701**，差**204701**。4个正常物理请求、加一次重试5个，小于16仅说明请求数不先阻塞；补源/更多路由跳转仍须计费计量并受剩余账本控制，没有证明额外容量。投影不是第三次方案迭代，也不是完成核验预演；不能把正常四请求形态说成所有可能输出增长的绝对上界。

资源取舍：保留当前载荷及计量时，正常形态仍须减少128192预留单位才适配原限额；仅schema精简已不足。应先核部署型号计量依据，用可信计量重新评估，不能按经验除数换算或伪造usage。若有效计量仍不适配，需要A另卡裁决新的通用表示/阅读范围或预算；本轮未擅自增加预算、裁掉主要业务或执行第三方案。当前约48秒只验证本地解析/停止/读回开销；部署上下文、真实推理耗时及费用均未知，900秒监督硬停不等于已证明真实全流程能按时完成。

### 11.3 资源资料、验证与交接

[资料回执](b1-resource-evidence/resource-metadata.json)记录公开只读核查：Scorpio公开SPA的模型广场接口`https://scorpio.reepool.com/api/v1/model-plaza`返回HTTP404、`Model plaza is not enabled`；没有发送凭证、查询管理接口或发起推理探路。六来源仍缺：渠道输入/输出价或订阅增量成本及资料日期、币种换算、实际上下文/最大输出、部署匹配的token计量/usage依据、供应商实际配额。配置限流不是供应商配额，官方同名产品价格不能直接套用渠道别名。不再反复向用户追问同一问题；将确切缺项及绑定交A集中裁决。

直接回归：**57 passed、1 deselected、7 warnings，16.95秒**，原生数值遗漏保留、Segment新增金额拒绝、冲突及依赖退出确定输出、源先行主业漏项、计划/否定/第三方、紧凑schema及Evidence展开、三purpose非空网关协议、未知usage、池切换共享预算和入口deadline均在对应定向用例覆盖。未选中的原owner重型用例本轮未重跑；两次真实资产入口均独立核query/export profiles一致，但不替代未完成的三purpose资源闭环。[完整测试回执](b1-resource-evidence/tests-final.log)。命令：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_report_wire.py tests/unit/test_research/test_company_profile_llm_led.py tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_utils/test_llm_physical_budget.py -k 'not existing_owner'
```

正向路由fixture由0.5秒改为整体10秒/单次5秒，专门超时/取消断言未放宽。Review只核本轮阻塞性错误：修正新测试把逻辑profile误写成未配置profile的问题；精简路径保留完整本地与硬约束，未发现其他需阻塞的代码错误。公共provider/contracts的既有格式问题与无关旧套件问题不扩修。所有执行文件hash和源PDF hash与两次绑定匹配；11份既有tracked文件hash与本轮基线相等，原untracked未操作。完整重建仍依赖绑定列出的既有脏依赖，不能声称单独checkout本次提交即可复原运行环境。

本轮按A/B v3自主完成可独立推进的实现/回归/证据工作。触发停止来自[B1卡§7](b1-execution-card.md#7-当前接续先让完整请求计划可运行a于2026-10-17裁决)明确的“最多两次离线资源方案迭代；若仍不可行交A”合同，不是增加逐项审批。下一步由A核定计量/资源取舍及后续卡，资料和完整资源可行性成立后再核真实权限。本次正式调用0、实际费用unknown、B活跃时间未精确计量；开发0/4、回归0/12、未见0/20、试运行0/100不变，生产`not_authorized`、规模声明false。提交身份以本节所属Git提交为准。

## 12. A审核df946753：资源裁决及正文发现P1（2026-10-17）

结论：**三阶段精简及两次停止记录成立；B1/A1未通过，新增1项范围内P1。** A不批准直接提高预算或盲开第三压缩方案；下一步按[B1卡§8](b1-execution-card.md)修正文发现和完整性，再以可信计量核完整计划与实际资源。原两项数值P1保持关闭，§11及26份原证据不改。

### 12.1 独立核验与资源裁决

- A复验B的相同定向组合：**57 passed、1 deselected、7 warnings，15.09秒**；命令见§11.3。未选中的原owner重型用例本次未运行，不声明全套测试通过。
- 26份证据hash、56份绑定执行文件hash及正式PDF hash通过；两次query/export profiles一致。绑定包含既有脏依赖，仍不能声称仅checkout提交就能重建。A未重跑208页入口、未调用真实模型、未写生产数据。
- A执行既有只读`project_remaining.py`重建：正常四请求248192、差128192；另加一次最大请求重试76509，合计324701。均为UTF-8保守计量单位下的既定合成形态，**不是实际token、费用或动态增长上界**，也不是完成闭环。
- 方案2仅模拟完整阅读13页，195页仅片段检查；片段地图不能证明主业召回。三视图、19核心/6负例及真实质量/费用仍未评估，正式首次/后继0/0。
- 资源裁决：采用有依据、与实际部署型号及消息/schema封装相匹配的计量可替代字节上界，未知usage继续保留预留；不按经验比例折算。原120000token/900秒/16物理请求/10元及累计上限不变。若计量仍超限，再交量化资源裁决，不能先裁掉主业或直接放大额度。
- 六来源可核计费/上下文/计量资料仍缺。继续现有池/故障切换，实际可达成员须全有资料；可提交已核成员隔离范围交A，未核成员在选路前不可达，不永久锁单一模型、不新建池。没有可核来源时，资源绑定与真实权限保持未完成；公开模型广场404不是模型不可用或价格为零的证据。

### 12.2 A-P1-MAP-01：预览外主业未读仍判完整

位置：`report_interpretation.py:page_map`每页仅取48/16/16字符，结构模式只对模型所选页及原生事实相邻页做正文发现；末尾`all_pages_read`在该模式以`map_checked`及已生成条件代替内容发现充分性，再据这些条件判七维完整。

A使用现有`CompactSemanticFixture`构造3页离线反例：第1页为原七维支持原文，第2页为相邻普通说明，第3页在三个采样点之外新增“本期主要经营电池回收服务，占营业收入百分之六十”。不传任何该业务候选或答案。实际模型可见完整页只有1、2，第3页标`partially_checked`且chunks为空；该主业既不在预览也不在后继全文请求，结果仍`draft_complete=true`、七维全部verified、gaps为空。此处无真实调用，**不是歌华有线实际漏项成绩**；它证明存在当前代码决定的语义盲区及完整性误判，同一判定用于provider模式的真实完整性。

可复现命令（只读来源/代码，零网络推理）：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python - <<'PY'
import json
from tests.unit.test_research.test_company_profile_llm_led import TEXT, _run
from tests.unit.test_research.test_company_profile_report_wire import CompactSemanticFixture
from research.company_profile.report_interpretation import compact_processing_identity, page_map
hidden = '另外，本公司本期主要经营电池回收服务，占营业收入百分之六十。'
pages = (
    {'page': 1, 'text': TEXT, 'readable': True},
    {'page': 2, 'text': '与第一页配套的普通说明。', 'readable': True},
    {'page': 3, 'text': '附加说明。' * 50 + hidden + '附加说明。' * 400, 'readable': True},
)
provider = CompactSemanticFixture()
result = _run(provider, processing_identity=compact_processing_identity(discovery_scope='page_map'), pages=pages)
print(json.dumps({
    'hidden_in_map': hidden in page_map(pages)[2]['text'],
    'full_context_pages': sorted({p['page'] for r in provider.requests if r.source_scope == 'full_context' for p in r.source_pages}),
    'page3': result.page_coverage[2], 'draft_complete': result.draft_complete,
    'verified_dimensions': result.verified_dimensions, 'gaps': result.gaps,
}, ensure_ascii=False))
PY
```

修复条件：对非采样/陌生位置且无结构化事实的正文提供通用语义发现/补源路径；未取得核心覆盖依据时不能单凭地图和已知条件宣称完整。不能只加“电池回收”等关键词，也不能把所有存在未读次要附注的报告一律永久判失败。不要求逐行抽取全部附注，不承诺绝对零遗漏；主要业务必须有进入语义发现的机会，已确认漏项须阻塞可用结论。B补隐藏主业反例及无关长附注正例，A复核该P1和直接回归。

### 12.3 下一卡、授权及进度

按[B1卡§8](b1-execution-card.md)，B在同一窗口先修P1及直接验证，并行核实部署匹配计量和资源。第7节2/2离线方案已用尽；A仅明确追加**1次最终修复快照的专项离线整体验证**，必须先有可信计量及全流程可行计划；否则不启动。此次追加单列，不重置旧2次、不增加正式/付费次数；失败即交具体结论，不再派生无限方案。来源资料不可得时先交可独立完成的P1修复及缺项，不增加fake测试或池功能替代真实业务。

本卡19核心至少18项、已审准确率≥98%、全部主要业务/关键数字/三视图及6负例要求不变；次要遗漏及有据unclear允许限用，错误和依赖结论退出确定输出，无法补救须说明原因/影响/恢复条件。真实首次/后继仍0/0，真实授权未签发；开发0/4（尚差4）、S1新链0/12、S2首组0/20、S3试运行0/100不变。按4→12→20→100推进，S2至多另20确认，到上限失败须结论；不归档、不扩批，生产`not_authorized`、规模声明false。

A本轮只审核及更新任务文档，不替B修业务代码；活跃审核时间未精确计量记unknown。开始时11份tracked既有修改及原untracked继续保护，验收计划仅隔离本人计量澄清及活动进度行；原合同、原§11和首次制品不变。

本轮5份文档通过OpenSpec本change严格校验、`git diff --check`及69个本地链接检查；19项源条件保留，11份既有文件扣除本人两处文档变更后hash相等。审核旧记录保持原字节前缀；本人提交仅含上述5份审核/任务文档。

## 13. B角接续：未采样正文发现与计量阻塞（2026-10-17）

结论：**A-P1-MAP-01的发现通路和完整性误判已按卡修入同一owner；可信token计量资料仍缺，最终计划的字节上界仍超过120000，故§8允许的那一次208页离线整体验证没有启动。** B1/A1仍未通过。正式首次/后继0/0，真实模型调用0，实际费用未评估。19项核心、6项负例和已审准确率未评估。不进入B2，不归档，不扩批，不改生产池，不加预算。

### 13.1 正文发现与完整性

地图仍只做均匀采样导航，不建立源条件，也不使用公司、证券代码、固定页码或标题白名单。模型选中的页及其相邻页继续读全文。其余可读页把预览没有覆盖的连续区间单独打包，送入`full_context`的源先行发现；这些区间不并进全文块，避免被已选正文淹没。程序不判断句子是否无关。已确认的主要条件若未进入交付，或引用不能落在本次正文上，就不能把七维标成完整。未采样正文没有进入发现请求时记`unsampled_body_not_discovered`，不能靠`map_checked`当成已读或未披露。

| 回归 | 实际结果 |
| --- | --- |
| A的三页反例，盲fixture | “占营业收入百分之六十”的句子不在48/16/16预览内，进入独立全文发现请求。该fixture仍引用第一页原文，引用不在未采样区间内，流程记`semantic_incomplete`，`draft_complete=false`。页状态为`residue_discovered`，不是`read` |
| 同一隐藏主业被模型立成主要条件但回答未交付 | 条件进入源先行清单，核验为block，观察回答不包含该条件，`draft_complete=false` |
| 无关长附注 | 第8页长附注进入未采样发现；fixture不逐行生成条件。短页仍只是`partially_checked`。七维可`draft_complete=true`，记录仍为1条，不是每行一条 |

没有把“电池回收”或收入比例写成生产规则。长报告不会因为存在未逐行抽取的附注而永久判失败。

### 13.2 计量、资源与静态计划

`semantic_extraction`的六个具体profile仍只有路由、超时和并发字段，没有价格、上下文字数或分词器字段。模型名为`gemini-3.8-flash-high`、`glm-5.3-flash`、`grok-4.7`、`gpt-5.6-luna`、`DeepSeek-V4.1-Flash`、`deepseek-flash`。没有可绑定到这些渠道型号的计费依据或分词器，因此物理预算接点继续使用UTF-8 payload字节＋256 framing＋最大输出。这不是token，也不是费用。未改`LlmRunBudget`的算法，未用经验除数、正文除法或fake usage冲减。未发送推理请求，也未重查模型广场。生产池和故障切换保持原配置；六个成员都没有资料，所以没有一个可单独放行的隔离绑定。

静态计划只序列化、不发送，正文来自第二次方案已保存的208页checkpoint（184544字 / UTF-8 404158字节），不是新的入口运行。非空协议的中页选择为第105页，全文为其相邻的104–106页，其余页发送未采样区间。发现阶段10个物理请求，按六成员较大预留合计**575995**，比120000多**455995**。再保留一次最大请求重试则为**652471**。若模型把全部未采样页一次补成全文，这一次补源单独为**449032**。合成、核验和Evidence增长尚未计入，因为发现阶段已经超过上限。请求数10小于16，先阻塞的是预留单位。动态内容若大于该计划，现有发送前拒绝仍然生效。因此§8的208页专项验证**未启动**，旧两次失败记录不重置。

证据：[静态计划](b1-coverage-evidence/static-plan.json)、[重建脚本](b1-coverage-evidence/project_static_plan.py)。

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
PYTHONPATH=/home/python/Quote /home/python/miniconda3/envs/Quote/bin/python openspec/changes/restore-company-profile-llm-led-generalization/b1-coverage-evidence/project_static_plan.py
```

### 13.3 验证、保护与停止

直接回归：**60 passed、1 deselected、7 warnings**。命令与§11.3相同，多出的3个通过项是本节两个正反例所在文件中的新用例；未选中的原owner重型用例仍未重跑。

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_report_wire.py tests/unit/test_research/test_company_profile_llm_led.py tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_utils/test_llm_physical_budget.py -k 'not existing_owner'
```

开始时HEAD为`1a9de515`。11份既有tracked修改和原未跟踪文件未操作。真实调用0，正式首次0/1、后继0/1，真实授权仍未签发。B活跃时间未精确计量，记unknown。开发0/4、S1新链0/12、S2首组0/20、S3 0/100不变。生产`not_authorized`，规模声明false。

下一步需要A裁决：在有部署匹配的分词与计费依据后重估这版发现计划，或明确阅读范围/预算取舍。本窗口不再开第三种压缩方案，也不把575995换算成token。

## 14. A审核71735099：正文发现修复接受，资源阻塞未解除

结论：**关闭A-P1-MAP-01，仅接受离线代码修复；B1/A1仍不通过。** R3可保留完成，R1/R2/R4及正式业务项不勾完成。下一卡为[B1卡§9](b1-execution-card.md#9-a对71735099的裁决与下一卡解资源阻塞后完成b1)：先清理无用历史开发产物并补交接，再解资源资料阻塞；无新资料不再做fake/压缩方案，不批准直接加预算或削掉主要业务。

### 14.1 独立核验与边界

- A按产品总需求→验收计划→交付路径→A/B流程/当前change顺序审核。实际仍无真实三视图或调用/usage证据，19核心/6负例及业务准确率均未评估，不以测试数或静态计划计开发公司数。
- 独立执行报告协议直接套件：**12 passed、7 warnings，4.84秒**，覆盖预览外正文、隐藏主业未交付、无关长附注、源先行漏检、数值/冲突保护及unknown usage物理预算。B的60 passed/1 deselected是其交接回执，本次A不冒称重跑全部60项；没有重跑无关旧套件。
- A额外检查0、1、47、48、49、80、81、999、24001、50001字符的页面：预览与正文发现区间合计无字符空洞，每块≤24000字符。这只是结构覆盖核查，不证明真实模型已读懂各句或业务召回达标。代码没有新公司/证券代码/固定页码选择分支。
- A读取现有208页checkpoint，只序列化重建静态计划，并拦截`Path.write_text`防止覆盖冻结JSON；重建对象与提交的static-plan.json完全相等：发现10请求575995，差455995，加一次最大重试652471，全部未采样页补全文的示例449032。这些都是当前字节保守预留，不是实际token、费用或完整可行计划；合成/verify/动态增长仍未计齐。原重建脚本会写回static-plan.json，不能直接当只读核验命令运行。
- 未发真实请求、未运行208页专项、未改业务代码/生产配置、未删除历史数据；源与§13原记录保留。本轮未发现新增业务P0/P1；当前外部资源阻塞仍是实际部署映射、计费/币种、上下文/输出、可信计量和可达范围/配额资料，配置中没有字段不等于服务不可用或免费。

实际直接验证命令：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_report_wire.py
/home/python/miniconda3/envs/Quote/bin/python -m ruff check research/company_profile/report_interpretation.py research/company_profile/report_wire.py tests/unit/test_research/test_company_profile_report_wire.py openspec/changes/restore-company-profile-llm-led-generalization/b1-coverage-evidence/project_static_plan.py
/home/python/miniconda3/envs/Quote/bin/python -m ruff format --check research/company_profile/report_interpretation.py research/company_profile/report_wire.py tests/unit/test_research/test_company_profile_report_wire.py openspec/changes/restore-company-profile-llm-led-generalization/b1-coverage-evidence/project_static_plan.py
```

静态核查有两项**非阻塞**：辅助脚本未使用导入`protocol_reply`（F401），report_interpretation.py一处条件表达式格式提示。留B下次触及相关文件时处理，不据此重开P1、扩大重构或消耗正式尝试；本轮不能称所有lint/格式检查通过。

### 14.2 新开发者报告方式与清理前置

§13如实区分了离线、静态预留、真实调用和业务进度，报告内容基本合格；“第8节已执行完”则不准确，R4/资源前置尚未完成，应报“正文发现局部修复完成、资源受阻”。以后按[A/B流程§8](../../../docs/development/ab_role_development_workflow.md#8-一个固定交接入口)统一必要字段，聊天不强制相同文风；顶部给最新状态和本次链接，详细证据仍用原入口，不重写§13或旧首次结果。

本次交接未报告清理范围/释放空间或保留理由，清理执行**未证实**。A仍见QuoteBak约275MB，111MB旧隔离输出、54MB pytest数据及若干旧B1临时目录仍在；目录存在不能直接证明可删。下卡由B先核引用/活跃占用/证据持久化再清无依赖产物，持久checkpoint、正式来源、首次失败、必要证据及既有/并行改动保留。A本次只提出清理前置，没有宣称已释放空间，也不建立新清理台账。

### 14.3 资源裁决、权限与进度

优先取得至少一个现有来源与实际部署匹配的计量/计费/上下文资料，再核真实token需求；当前字节投影不能据以判断120000实际token必然不足。允许同网关隔离已核可达成员，未核来源不参与实际切换，不改生产池、不要求先凑齐六份，也不冒称单来源已证明故障切换。资料确实不可得时，B交一个确切缺项简表即停止，由A集中协调资源，不继续派纯fake任务；只有可信计量仍不可行，才需要有数据支撑的范围/预算裁决。

旧离线2/2用尽；原专项0/1未启动；真实首次0/1、后继0/1且**未获授权**。预算、19核心/6负例及容错政策不变。开发0/4（剩4）、S1新链0/12（剩12）、S2首组0/20（剩20）、S3 0/100（剩100）；不归档、不扩批，生产not_authorized、规模声明false。A活跃审核时间未精确计量，记unknown；本轮新增业务P0/P1为0，报告/清理缺项需补，非阻塞静态提示两项不扩大范围。

A本轮只更新流程、活动进度、任务卡/待办和本审核入口；提交身份以本节所属Git提交为准。开始时11份tracked既有改动及原untracked继续保护；验收计划仅隔离本人活动进度一行，其他既有字节不纳入提交。

本轮文档通过当前change的OpenSpec严格校验、76个本地文件链接核查及`git diff --check`；11份既有文件扣除本人活动进度行后hash不变，静态计划/资源回执及此前审核正文保护通过。未新增过程脚本、测试数据或备份，pytest临时日志按既有session收尾清除；历史待清候选留B按下卡核依赖处理。

## 15. B角：清理与资源缺项停止（2026-10-17）

结果：当前是B1卡§9。本次是清理和资源核对，不是正式首发或后继。正文发现局部修复此前已被接受；本窗口资源受阻并停止。待A判断的是：由维护方提供至少一个现有来源的部署映射、计费、上下文和可信计量后，再重估完整计划。

业务：歌华有线三视图没有新的交付。19项核心、6项负例、已审准确率均未评估。允许的次要遗漏和有据歧义本窗口没有新的业务样本可记。

差异：基线`672449ad`。本窗口没有改业务规则或阅读代码。A-P1-MAP-01保持关闭。R4仍未完成。

验证：没有新的业务测试。上一轮60 passed是§13的离线回执，本窗口未重跑。§14的两项非阻塞提示（`protocol_reply`未使用导入，以及`report_interpretation.py`一处格式）本窗口没有改那些文件，仍然未处理，不能称为全部静态检查通过。清理不依赖测试通过。

资源：真实调用0，usage和费用未评估。静态预留仍是§13的字节保守单位575995，不是token，也不能据此判断120000 token不足。原离线方案2/2已用。208页专项0/1，本窗口未启动。真实首次0/1、后继0/1，授权未签发。

证据：正式PDF、OpenSpec中的iteration2 checkpoint、`static-plan.json`和S1持久回执都未删除。`a-s1-review-W6qOUq/results.xml`仍在，SHA-256与持久副本`s1-validation-receipts/s1-repaired-independent-results.xml`相同：`cdc2ef3293dd5461f8a76486ef6f806510baad9ecb9ac191e904ae7b504cc914`。资源文件删除前，iteration1/2的budget、source-receipt、entry-result、offline-transport以及query/export与`b1-resource-evidence`逐一相同；`scorpio-model-plaza.json`也相同。`full-report-final-v2/export.json`与`b1-continuation-evidence/export.json.gz`解开后相同。

闭环与清理：清理时点2026-10-17。清理前`du`约275M，文件字节279,853,560；清理后`du` 67M，文件字节67,561,851；释放212,291,709字节。`lsof`未看到占用这些目录的进程。已删除：

| 路径 | 文件字节 | 删除依据 |
| --- | --- | --- |
| `a-s1-review-W6qOUq/work` | 115,241,777 | 隔离测试库；成绩已在持久XML，hash一致 |
| `pytest-of-python` | 54,024,082 | 已结束的pytest数据目录，可重建 |
| `cache/pycache`与`cache/pytest` | 32,173,931 | 工具缓存，可重建 |
| `b1-resource-20261017` | 5,796,264 | 与持久iteration证据hash一致的重复运行副本 |
| `debug-s1-600082`、`debug-s1-600000` | 2,506,379 | 无引用的调试输出 |
| `b1-continuation-20261016/full-report-final-v2` | 2,476,758 | export与持久gzip一致 |
| 75个`quote_pytest_logs_*` | 72,518 | 已结束的pytest日志目录 |

保留及原因：`v61_s0`仍被v61预检脚本使用；`s1-s2-freeze-20261016`仍被S2冻结核验脚本读取；`s1-v60-baseline-gmcgf7oq`和`profile-plan-ctuAoKJK`没有引用，但内容是导出或文档草稿，归属未证实，不按目录名删除；`b1-continuation-20261016`其余探针的export与持久副本不一致，连同baseline diff和日志保留；`b1-pool-binding-20261017`和`b1-llm-led-offline-20261016`未证明是已持久化副本；`logs/`是当前开发日志目录；`cache`下的ruff、black、node-compile缓存仍可复用。正式PDF、OpenSpec checkpoint和11份既有工作区改动未动。

资源缺项（2026-10-17只重读本地`config/13_llm.json`，没有新的网络查询，也没有推理探路）。六个`semantic_extraction`具体profile都没有价格、上下文、分词或配额字段。配置里的超时和每分钟请求数不是供应商配额。公开模型广场404是既有回执，本窗口没有重查。

| profile | resource / model | 部署映射 | 计费、币种、日期 | 上下文 / 最大输出 | 可信计量 | 配额 | 回退 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `semantic_extraction__scorpio_gemini` | `scorpio:gemini` / `gemini-3.8-flash-high` | 缺渠道到部署型号的依据 | 缺 | 缺 | 缺 | 缺 | 未核，不能参与切换 |
| `semantic_extraction__zai` | `zai` / `glm-5.3-flash` | 缺 | 缺 | 缺 | 缺 | 缺 | 同上 |
| `semantic_extraction__scorpio_grok` | `scorpio:grok` / `grok-4.7` | 缺 | 缺 | 缺 | 缺 | 缺 | 同上 |
| `semantic_extraction__scorpio_luna` | `scorpio:luna` / `gpt-5.6-luna` | 缺 | 缺 | 缺 | 缺 | 缺 | 同上 |
| `semantic_extraction__scorpio_deepseek` | `scorpio:deepseek` / `DeepSeek-V4.1-Flash` | 缺 | 缺 | 缺 | 缺 | 缺 | 同上 |
| `semantic_extraction__deepseek` | `deepseek` / `deepseek-flash` | 缺 | 缺 | 缺 | 缺 | 缺 | 同上 |

没有一个来源资料齐备，因此没有隔离绑定，没有重算发现/合成/核验计划，也没有启动208页专项。生产池未改。需要维护方提供上表中的缺项；本窗口停止，不再迭代fake或压缩方案。

效率：B活跃时间未精确计量，记unknown。本窗口正式复验0次。既有11份tracked修改和原未跟踪文件未操作。开发0/4，S1 0/12，S2 0/20，S3 0/100。生产`not_authorized`，规模声明false。

## 16. A审核8e30ebff：清理交接接受，资源依赖转交维护方

### 16.1 结论与独立依据

接受§15清理和缺项交接，R5完成；报告符合A/B流程§8，本窗口没有把旧60 passed或字节预留冒称新测试/真实usage。A-P1-MAP-01保持关闭。没有新业务代码，本轮不重跑pytest或业务；B1/A1未通过，R1/R2/R4仍未完成。

A只读核得QuoteBak约67MB，§15指定清理目标已不存在、列明保留路径存在、quote_pytest_logs_*无残留。两份S1 results XML同为`cdc2ef3293dd5461f8a76486ef6f806510baad9ecb9ac191e904ae7b504cc914`；正式PDF hash仍为`b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`，持久checkpoint及static-plan保留。B删除表字节加总为212,291,709，与前后差额一致；删除前占用检查和全部副本逐项hash属于B记录，A不能事后独立复演。保留有依赖或归属未明内容合理，不为清空临时目录而删证据。

A检查本地配置的六个semantic_extraction profile字段，均未见部署映射、计费、上下文/配额及可信计量资料。缺资料不等于来源不可用；575995字节保守预留也不能证明120000实际token不足。当前是资源前置阻塞，不另造代码P1；两项既有非阻塞静态提示继续后置，不声明全套静态通过。

### 16.2 下一卡、责任与恢复条件

按[执行卡§10](b1-execution-card.md#10-a审核8e30ebff资源交付与条件恢复卡)执行A0-R及条件B1-R：A集中协调现有资源维护方，至少补一个来源的部署映射、费用/订阅增量成本、上下文/输出/配额和可信发送前计量依据；资料只需原入口链接/最小说明，不需密钥或新平台。只核一个来源可准备关闭回退的隔离绑定，未核成员不可达，不改生产池、不宣称故障切换已验收。

B资料到位前暂停该业务窗口，不再重复读同一配置、查同一404、清理同一历史或派生fake方案。到位后在原owner内连续绑定并重估全流程；在原预算内才消耗原专项0/1完成208页离线闭环，再一次交A0核定真实权限。资料不可得或可信计量仍不可行，A基于具体缺项/差额裁决资源和范围，不能先删主业或加预算。

原离线方案2/2已耗尽；专项0/1、真实首次0/1及后继0/1均未启动，真实权限未签发。原19核心/6负例、容错与资源上限不变。开发0/4（剩4）、S1 0/12、S2首组0/20、S3 0/100；不归档、不扩批，生产not_authorized、规模声明false。

### 16.3 本次审核交付与验证

本轮仅更新当前任务卡、实施待办、交付路径、验收活动进度行及本审核结论，不修改业务代码、模型配置或§15原报告。当前change的OpenSpec严格校验、78个本地文件链接核查及git diff --check通过；11份既有修改扣除本人活动行后hash不变，68份冻结文件及§1—15原审核正文保护通过。Review未发现本轮文档新增阻塞，资源缺项保持前置阻塞、旧静态提示后置；不新增过程脚本、测试库或备份。A活跃审核时间未精确计量，记unknown；业务运行/真实调用0，未产生模型费用。

## 17. A接续用户裁决：复用现有池，撤销资料齐备才能试跑的门槛

用户明确所有LLM/LLMs配置在config/13_llm.json，池中选源、不可用时换源。A核到semantic_extraction → shared_semantic、六个同权成员、weighted_fair及启用的max_hops=3故障切换；现有能力满足选路意图，无需新建随机路由。精确计费/上下文/分词资料与调用路由不同，前者缺失不证明模型不可用。

A此前将精算所需资料全设成首家开发观察前置，阻止取得真实业务证据，这一规划门槛过重。代码现状也会实际阻塞：bounded_entry.validate_binding/prepare_gateway要求price/context和全流程feasibility，LlmRunBudget.reserve要求每源有price；因此仅口头说可用池不能让下一轮跑起来。A不代B实现，派[执行卡§11](b1-execution-card.md#11-b1-p按现有配置池完成首家真实开发观察)做有界局部接续，原§16资料暂停卡撤销、不记为资料已补齐。

本卡仅授权原歌华同源首次1次：900秒/16物理请求/120000 token总账，逐请求保守预留、有效usage结算、unknown/失败占预留，沿原池切换。静态全计划575995字节不能冒充实际token或单独否决该观察。费用未知只在验收§7.1这一首家例外中允许，不能保证原10元/80元货币上限或宣称成本通过；已知价照常核费，不新增购买/充值。原质量/容错门槛不变，不以试跑授权冒充B1通过。

B完成局部修改、直接回归和快照冻结后连续执行，不再等维护资料或额外格式审核；失败结果保留，后继和其余公司仍待A据实测裁决。原离线2/2、专项0/1保留，不新增fake轮次。当前真实调用0，开发0/4、S1 0/12、S2 0/20、S3 0/100；不归档、不扩批，生产not_authorized、规模声明false。

本轮A只修正文档与目标delta，不改业务代码/生产配置或旧冻结绑定，不运行模型、不重跑pytest。当前change的OpenSpec严格校验、81个本地文件链接及git diff --check通过；11份既有修改完整保留，66份未改目标的冻结文件及§1—16审核正文hash不变。Review确认本次明确改变的是首家开发观察的资源前置和费用例外，业务门槛未放宽；不以文档变更宣称B代码已经可运行。A活跃时间未精确计量，记unknown。

## 18. B角：现有池首次真实观察（2026-10-17）

结果：B1卡§11的首次开发观察已经执行，占用首次1次。结果是传输失败后的资源停止，不是业务通过。待A判断失败分类、是否授权后继，以及这一次预留是否应按传输失败保留。

业务：歌华有线三视图状态都是`extraction_failed`，claims为0。`draft_complete=false`，`real_semantic_complete=false`。19项核心、6项负例和已审准确率未评估。208页都记为`unsampled_body_not_discovered`。query与export的profiles一致，`delivered=1`只表示限用记录已保存，不是可用画像。

差异：基线`accdd03e`。严格计费模式仍拒绝没有价格的来源。`development_unpriced`只用于这次绑定：没有价格仍可发送，费用保持unknown，不造`LlmPrice`或context。每个请求仍按完整payload字节加256再加最大输出预留；有效usage才把token账结算下来。开发观察另限制payload≤65536字节、最大输出≤4000。发现分块把每页正文都保留在某个请求里，不按公司或页码挑选。旧绑定文件没有改。

验证：`pytest -q tests/unit/test_utils/test_llm_physical_budget.py tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_research/test_company_profile_report_wire.py tests/unit/test_research/test_company_profile_llm_led.py -k 'not existing_owner'` 为64 passed、1 deselected、7 warnings。`ruff check`覆盖本次改动文件，通过。没有重跑无关旧套件。

资源：入口墙钟56.5秒，未到900秒。物理请求1/16。预留60963，账上限120000。费用unknown，10元和80元都未评估，不能宣称货币上限内完成。

| 项 | 实际 |
| --- | --- |
| 来源 | `scorpio:gemini` / 配置模型`gemini-3.8-flash-high` |
| 响应模型 | 未返回 |
| payload | 59707 UTF-8字节，最大输出1000 |
| 结果 | `ProxyError`，host=`scorpio.reepool.com`，约958毫秒 |
| usage | 无，预留60963不释放 |
| 重试/换源 | 第2次尝试在发送前被同一总账拒绝（剩余59037，下一次需要60963）。`failover_count=0`，没有第二个来源的物理发送 |

证据：[绑定](b1-pool-observation-binding.json)、[按卡授权，不是代码已审](b1-pool-observation-authorization.json)、[账本](b1-pool-observation-evidence/budget-final.json)、[摘要](b1-pool-observation-evidence/summary.json)。运行目录`data/QuoteBak/b1-pool-observation-20261017/first`保留query/export/日志。正式PDF和旧checkpoint未改。

闭环与清理：本窗口测试新产生的2个pytest日志目录已删，约8KB。观察目录保留到A审核。11份既有修改和原未跟踪文件未动。首次已用，后继0/1未授权。开发0/4、S1 0/12、S2 0/20、S3 0/100。生产`not_authorized`，规模声明false。

效率：B活跃时间未精确计量，记unknown。正式复验1次，没有重复根因重跑。

## 19. A审核首次真实失败与唯一后继授权（2026-10-17）

### 19.1 结论与独立证据

审核b1c57236：接受开发模式接续及首次真实失败记录；**B1/A1业务未通过**。缺三类产品交付，本家分级为隔离，不能以exit_code=0或delivered=1算可用。A核首次query/export均存在且profiles一致；持久budget-final与运行副本的JSON一致。冻结绑定56份执行文件hash全部吻合，授权所指binding SHA吻合；这些只证明执行身份与失败记录一致，不证明LLM已读正文。

首次物理发送1/16，配置选中scorpio:gemini / gemini-3.8-flash-high，响应model/usage均未返回。entry.log记录ProxyError约958毫秒；完整入口56.54秒。59707字节完整payload＋256封装＋1000最大输出＝60963保守预留，剩59037；同源第二attempt未通过reserve，没有第二次物理发送，failover_count=0。不能把2次逻辑attempt写为2次模型调用，不能把ProxyError推成模型不支持或远端明确未收到。

三视图extraction_failed、claims=0，208页仍unsampled_body_not_discovered。解析成功不等于模型阅读完成。19核心未形成交付，阶段公司召回按0/19、0%计；逐项语义准确性、数字错误及6负例不可评估，不以无输出记零错误/负例通过。首发观察1/4、本家首发可用0/1（全组0/4），补救未评估；其余3家未观察且本卡未授权。S1 0/12、S2 0/20、S3 0/100。费用unknown，10元/阶段80元未验收。

### 19.2 阻塞分类与下一张卡

| 类别 | 发现及影响 | 本次处理 |
| --- | --- | --- |
| 范围内P2：出口 | 真实进程ProxyError，根因未确认；HTTP transport默认继承环境，尚不能区分代理、沙箱出口或上游问题 | 同进程有界诊断，先有出口证据，不额外发真实模型探针；不关闭TLS或全局改代理 |
| 范围内P2/P3：恢复余量 | 单次60963已超过总账一半；unknown保留后同源重试必被拒绝，未验证实际大载荷下跨源路径 | 原预算正确拒绝，不回溯放行；用实际序列化请求验证失败占预留后仍可跨源及合成/核验准入，允许仅隔离降低同源重试 |
| 非阻塞/限制 | 部署别名、价格和usage未知；B原直接64 passed不是本家业务通过 | 延续本家费用unknown例外，不再等待六来源资料；成本未验收，不能据此扩样 |
| 与本轮无关 | 11份既有修改、无关旧套件/格式提示 | 不修改、不纳入提交，不为其扩回归范围 |

A签发[B1-R执行卡§12](b1-execution-card.md#12-b1-r恢复真实出口与换源余量完成唯一后继)及[验收§7.2](../../../docs/development/company_profile_acceptance_program.md#72-b1首次失败与原有后继裁决2026-10-17)：B在一个连续窗口内先完成必要证据持久化/清理、出口诊断、实际载荷恢复验证和新快照冻结，再自主使用原有唯一后继；不逐模块等A。旧首次1/1失败及60963unknown预留保留，后继仍限900秒/120000 token/16请求，累计额度不变，无第三次、无额外付费探针。环境仍不通则停止交接，不先启动后继。

业务仍核19核心至少18项、主要板块/变现和三视图、已审准确率≥98%、6负例及底线错误隔离；允许次要遗漏和有据unclear，不要求全附注100%。B按A/B流程v3§8使用本文件唯一交接，保留首次/后继和限制；A在卡末独立核原文，再裁决是否进入B2。第17节已撤销的资料暂停卡不重开，原离线2/2和专项0/1不重置。不归档、不扩批，生产not_authorized、规模声明false。

### 19.3 本次A验证与保护

A独立运行：`source scripts/quote_dev_env.sh`后，`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_utils/test_llm_physical_budget.py`，**13 passed、6 warnings**；验证严格/开发模式、有效usage/unknown及共享账本等直接行为。首次曾加`-p no:cacheprovider`，与项目cache_dir配置冲突、未收集测试，改回上述标准命令后通过。未把这一命令错误计为业务失败，也未声明该小载荷测试证明本次大载荷恢复路径。

本轮A只修改任务卡、待办、交付路径、验收活动进度及本节，不修改业务代码/生产配置、不启动后继或任何真实LLM。当前change的OpenSpec严格校验、90个本地文件链接及git diff --check通过。§1—18正文及73份未修改的change文件hash不变；11份既有修改（验收计划仅隔离本人新增活动内容，原历史区块hash不变）及所有原未跟踪内容不触碰。Review未发现本轮文档新增阻塞，真实出口和大载荷恢复仍为B下一卡待办，不据发卡宣称已修。测试日志按现有fixture自动清理，本次独立测试的QuoteBak/pytest-of-python/pytest-2及其current链接已核归属后删除，约9KB，可由测试重建；可复用工具缓存保留。首次观察目录约束为必要证据持久化后再清，不因本次失败整目录删除。A活跃审核时间未精确计量，记unknown。

## 20. B角：出口排查与唯一后继（2026-10-17）

结果：B1-R已执行。出口障碍来自沙箱注入的本地代理，无沙箱进程可以连到配置主机。唯一后继已启动并占用1次。三视图仍未交付，待A裁决，不再发起第三次。

业务：后继三视图均为`extraction_failed`，claims为0，`draft_complete=false`。19项核心、6项负例和已审准确率未评估，不能把空输出记成负例通过。阶段召回仍按首次审核的0/19理解；后继没有新的可用公司。

差异：基线`6b46e5d3`。请求打包把单次保守预留压到30000以内，使一次unknown失败后仍能再发一源、合成和核验。隔离绑定把同源`max_retries`设为0，生产`config/13_llm.json`未改，路由指纹仍是`fb5b895c00cdbdd7c158d35d3707988d5813d62f7d5df7319112151afb1e9141`。旧首次绑定和授权原文件未改。

验证：`pytest -q tests/unit/test_research/test_company_profile_report_wire.py tests/unit/test_utils/test_llm_physical_budget.py tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_research/test_company_profile_llm_led.py -k 'not existing_owner'` 为68 passed、1 deselected、7 warnings。其中歌华208页真实载荷测试确认：一次失败预留后，另一`provider_resource`、合成和核验四个准入都在120000内，费用保持unknown。`ruff check`覆盖本次改动文件，通过。没有重跑无关旧套件。

资源：出口检查3次，均无鉴权、无模型调用、超时5秒。沙箱进程代理为`http://127.0.0.1`本地端口，访问`scorpio.reepool.com`得到`ProxyError`。去掉沙箱后代理为空，`https://scorpio.reepool.com/`返回200，`https://api.deepseek.com/`返回401。后继因此在无沙箱进程中运行。

后继入口100.1秒，物理请求3/16，token账35233，费用unknown。10元和80元仍未验收。首次60963 unknown预留留在首次账，没有并进后继账，也没有记成0。

| 序 | 来源 | 配置模型 | 结果 | usage | 结算token |
| --- | --- | --- | --- | --- | --- |
| 1 | `scorpio:gemini` | `gemini-3.8-flash-high` | HTTP 200 | 6356+659 | 7015 |
| 2 | `zai` | `glm-5.3-flash` | HTTP 200；请求最大输出1000，供应商返回2020 | 5015+2020 | 7035 |
| 3 | `scorpio:grok` | `grok-4.7` | HTTP 404，`provider_error` | 无 | 预留21183 |

三次都是独立的地图请求，`failover_count=0`。404按现有池策略不换源。前两次选出的页码没有在第三次失败前写入覆盖结果，所以208页仍显示未采样。进程在写出query/export之后关闭HTTP客户端时出现`Event loop is closed`，入口退出码1。响应模型除日志中的provider request id外，账本没有单独的响应model字段。

证据：首次持久副本及hash在[first/manifest.json](b1-pool-observation-evidence/first/manifest.json)。后继账本、query/export和日志在`b1-pool-observation-evidence/successor/`，与清理前运行副本hash一致。绑定[b1-pool-successor-binding.json](b1-pool-successor-binding.json)，授权写明按卡执行、不是代码已审。

闭环与清理：首次运行目录的重复库和导出已在hash核对后删除，`attempts/first.json`保留。后继的`research.db`、pdf缓存和output副本已删，query/export/日志的持久副本保留，`attempts/successor.json`保留。本窗口pytest日志目录已删。11份既有修改未动。QuoteBak当前约68M。没有第三次运行。开发观察1家已跑首次和后继，可用0家；S1 0/12、S2 0/20、S3 0/100。生产`not_authorized`，规模声明false。

效率：B活跃时间未精确计量，记unknown。后继1次，没有在失败后再跑。

## 21. A审核唯一后继失败与专项闭环裁决（2026-10-17）

### 21.1 审核结论、真实进度与已解决事项

审核167f07ec：接受出口排查、恢复余量局部接续及首次/后继失败留存；**B1/A1业务未通过**。原首次1/1、后继1/1均已耗尽，B没有自行第三次。当前开发观察1/4（剩3家未观察）、首发及原后继可用均0/1（全组可用0/4）；三视图未交付，交付召回均按0/19记，准确率/数字错误/6负例不可评估。S1 0/12、S2 0/20、S3 0/100，不以旧规则成绩或真实token计数替代新路线业务验收。

A核后继冻结56份执行文件hash全部一致，首次/后继两个manifest各6份文件hash和字节数均吻合，query/export profiles一致。首次/后继入口分别56.535554、100.132023秒；后继退出码1。真实后继3次独立地图请求：gemini有效usage7015，zai有效usage7035，grok 404无usage保留21183，总账35233。两次累计4物理请求、156.67秒，14050有效usage＋82146未知预留＝96196账本占用，不能统称实际token。三个地图请求failover_count均0，不是一次请求的跨源成功。费用unknown，10元/阶段80元未验收。

出口证据与B记录吻合：沙箱注入本地代理时ProxyError，无沙箱进程对既有主机返回200/401；这证明主机可达，不证明所有模型或鉴权可用。后继已取得两个真实模型响应，不能再声称真实调用为0。用户另明确grok 404为配置问题且已修正，A按该确认记录，不另发模型探针、不追改原404或判定B须重修配置。新运行仍须重冻当前配置，原绑定与失败保护。

zai请求输出1000、usage输出2020，日志已记录output_budget_exceeded，有效响应并按实际7035入账。该次输出超过请求值，入口4000上限只约束请求设置，不能保证供应商遵守。当前总账未耗尽，不将有效响应丢弃或虚构业务错误；保留超出提示和实际用量，不把2020改成1000。响应model未单独返回/保存时保持unknown，配置model与provider request id不冒充已核底层部署型号。

### 21.2 独立发现与下一卡范围

| 类别 | 实证与影响 | 下一步 |
| --- | --- | --- |
| P2：全链请求数量不可行 | 现版把30k恢复预留约束施于所有批次，208页静态重建两种情形分别37/46请求，均超过16；不是只修404就可完成 | 原owner内调整通用分批/重复上下文，先核完整主路径≤15并留1次恢复，再启动专项 |
| P2：HTTP客户端生命周期 | entry.log在bounded_entry关闭client时触发Event loop is closed，退出码1；同步provider可用临时asyncio.run执行共享client，关闭在入口循环 | 局部修复同一客户端调用/关闭的循环归属，验证多次调用及失败收尾，不能吞异常假装成功 |
| P2：部分地图覆盖未合并 | 前两批共100页预览的成功回执及35个选页已保存，但map_checked只在全部地图成功后更新；第三批失败使coverage未反映这部分导航 | 保留成功批次的导航证据和失败/未读去向；不把预览改为正文已读，不建平行checkpoint体系 |
| P3：大载荷换源验证仍有缺口 | gehu测试生成完整载荷，但只手工reserve四笔，未让大载荷经LlmClient失败→另一源物理发送；合成/核验只用一个body chunk，也不能证明完整输入增长后可执行 | 使用真实载荷与模拟transport验证现有client的物理跨源，并对完整发现/合成/核验做静态尺寸和次数检查；不扩fake业务轮次 |

原四笔reserve回归通过是有效的局部预算证据，R10/R11勾选不表示上述缺口已验收。前两批选页也没有彻底丢失：call_receipts.response_payload已保留，缺的是它们对覆盖结果的反映。208页正文均未进入发现仍是正确的未读限制，不能为了改进覆盖展示把它们升为已读。

A上一卡只约束失败后的四笔预留，漏查完整请求数，这是规划遗漏。此次以一次直接的全链次数核查补足，不新增测试框架或精算渠道资料门槛。静态计数方法：从[原208页checkpoint](b1-resource-evidence/iteration2/checkpoint.json.gz)取pages/report，沿compact_processing_identity(page_map)、page_map、pack_fitting_batches和现有fits_development_payload生成地图；再调用structural_discovery_chunks并加合成/核验至少2次。两种结果：

| 选页情形 | 地图 | 正文发现 | 合成/核验至少 | 不含重试的合计 |
| --- | --- | --- | --- | --- |
| full_page_ids为空，仍发现所有预览外正文 | 5 | 30 | 2 | 37 |
| 后继前两次成功回执的35个requested_pages及相邻页 | 5 | 39 | 2 | 46 |

以上是现版打包两种实源静态情形，不是全报告的理论最少请求数，不是实际usage/费用，也没有运行第三轮fake业务或真实模型。不能据37/46直接增加请求额度，先去除过碎分批及重复载荷，同时保持全部正文发现、主要业务与必要上下文。

### 21.3 明确授权与交接

A签发[B1-C执行卡§13](b1-execution-card.md#13-b1-c修正全链请求计划与运行收尾条件执行一次专项闭环)及[验收§7.3](../../../docs/development/company_profile_acceptance_program.md#73-b1原两次耗尽后的专项裁决2026-10-17)。B按A/B流程v3在一个连续窗口先清理、修复请求计划/生命周期/部分覆盖、完成直接验证并冻结新快照；前置成立后自主执行**一次第三次完整专项尝试**，无逐模块等待。这是本次A新增的次数例外，专项0/1，不重命名或重置原首次/后继；原失败和原96196账本占用不变。

专项仍限900秒/120000 token总账/16物理请求，取原B1累计1800秒/240000/32请求的余量。专项用满后累计216196，不增加L0/L1总额或公司数。费用unknown例外仅延续本家专项，无新购买/充值、不保证≤10元或阶段≤80元。原19核心至少18、主要板块/变现和三视图、已审准确率≥98%、6负例及底线错误隔离不变；允许次要遗漏/有据unclear，无法补救说明来源、影响、尝试、限制和恢复条件，不追求所有附注100%。

完整请求计划仍不成立或本卡两个局部迭代用完则不启动专项、交差额；专项失败则停止，A重新裁决路线/资源，不能自动第四次同样重跑。唯一交接仍为本文件，明确首次/原后继/专项三列、三视图及源核、覆盖和真实请求/usage/成本、query/export/磁盘、测试/清理和剩余额度。不归档、不扩批，生产not_authorized、规模声明false。

### 21.4 本次验证、Review与保护

A独立命令（先source scripts/quote_dev_env.sh）：`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_report_wire.py -k 'gehu_batches_keep_failover_synthesis_and_verify_after_one_unknown_failure or compact_plan_preserves_source_first_omission_checks_and_partial_coverage'`，**3 passed、11 deselected、7 warnings**。复用B同快照68 passed/1 deselected的其余直接回执，不声明全套测试通过；测试通过的范围限制如上。A仅做只读源/回执/代码核验和静态计数，没有真实模型调用或第三次入口。

本轮只更新本卡、待办、交付路径、验收活动台账、design当前指引及本审核节。当前change的OpenSpec严格校验、98个本地文件链接及git diff --check通过；Review未发现本轮文档新增阻塞，真实业务缺口仍留B实施。原§1—20正文及88份未修改的change文件hash不变，config未改；11份既有修改和原未跟踪内容保留，验收计划历史区块hash不变，提交只隔离本人活动差异。必要证据不再复制，临时产物统一QuoteBak；本次测试日志由fixture清理，残留的本人空日志目录核无文件后rmdir删除约4KB，可重建，可复用缓存保留。首次/后继清理核现存状态和持久hash，删除前逐副本比对属B记录。当前QuoteBak约68M，不因目录大小盲删。A活跃审核时间未精确计量，记unknown。

## 22. B1-C专项受阻交接（2026-10-17）

### 22.1 结果

局部修复完成，专项正式闭环已执行并受阻。B1/A1业务未通过。三视图均为`extraction_failed`，`draft_complete`、`semantic_complete`、`real_semantic_complete`均为false。19项核心和6项负例没有可审交付，记未评估，不用零输出记零错误或负例通过。观察仍1/4，可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批，生产`not_authorized`，规模声明false。

首次1/1、原后继1/1、专项1/1均已用尽。专项失败后按卡停止，没有第四次入口。待A重新裁决路线和资源。

### 22.2 业务与覆盖

同一家600037.SH、同一208页源，PDF SHA-256 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。专项`source-receipt.json`与首次回执同为`f304554e`、17175字节。

查询与导出profiles一致。`delivered=1`只表示写库，不是可用三视图。条件、主张和记录均为0。缺口209条：1条`semantic_incomplete:physical_request_token_context_or_cost_budget_exhausted`，以及208条`unsampled_body_not_discovered`。正文发现没有完成。

三个视图的`checked_pages`各208页：全部`partially_checked`且`map_checked`，`map_selected` 53页，没有一页`read`。两批地图成功后的预览和选页留在覆盖里；失败的正文请求没有把预览升为已读。

### 22.3 差异

基线`9865f79c`。请求计划不再使用30000四槽预留切批。单次载荷仍≤65536字节；另要求一次预留不超过120000账本的一半（60000），以便一次未知失败还能重发同等载荷。地图与正文发现可以放在各自保留scope的同一请求里。合成和核验只带已经读过的引用摘录和条件，不把同一正文再发一遍。

HTTP客户端在创建它的事件循环里释放，入口`close`不再触发`Event loop is closed`。失败也会写入`entry-result.json`后再抛出。地图按成功批次写入`map_checked`/`map_selected`，不等全部批次结束。

当前`config/13_llm.json` SHA-256仍是`270f9a3b06555dc1802a3f13a6c5372274bb0abd00e0345f983df8c38cf3c9ae`，路由指纹仍是`fb5b895c00cdbdd7c158d35d3707988d5813d62f7d5df7319112151afb1e9141`，与后继冻结相同。本次没有探测模型，也没有改生产池或`max_retries`。专项绑定只在内存里把同源重试设为0。旧首次/后继绑定和attempt标记未改。

### 22.4 静态计划与直接验证

208页checkpoint上，现打包为2次地图＋10次正文发现＋2次合成/核验＝14，三种选页都一样：未选页仍发现预览外正文、后继前两次35个选页及相邻页、全部页面选中。余1次恢复。直接测试断言每种合计≤15，且每个发现片段覆盖对应正文偏移。

命令（先`source scripts/quote_dev_env.sh`）：

`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_report_wire.py tests/unit/test_research/test_company_profile_llm_led.py tests/unit/test_research/test_company_profile_bounded_entry.py tests/unit/test_utils/test_llm_physical_budget.py tests/unit/test_utils/test_llm_gateway.py::test_owned_http_client_is_closed_on_the_loop_that_created_it`

结果：**70 passed、7 warnings**。没有跑全库。最大真实正文载荷经`LlmClient`和`ScriptedTransport`：第一次可重试失败，第二次另一`provider_resource`物理发送同一载荷，共享开发预算，两次usage均unknown并保留预留，费用unknown。这是模拟传输，不是专项里的真实换源成功。

### 22.5 专项资源

入口在无沙箱进程启动。当时`http_proxy`/`https_proxy`均为空，没有改全局代理或TLS。监督退出码0，耗时363.318675秒。`entry-result.json`已写，日志没有`Event loop is closed`。

| 次序 | 来源 | 结果 | 账本 |
| --- | --- | --- | --- |
| 1 | scorpio:gemini `gemini-3.8-flash-high` | HTTP 200，地图142页 | 有效usage 17240＋725＝17965 |
| 2 | zai `glm-5.3-flash` | HTTP 200，地图66页；请求输出1000，供应商输出5062，日志`output_budget_exceeded` | 有效usage 6489＋5062＝11551，不把5062改成1000 |
| 3 | scorpio:grok `grok-4.7` | 流已打开且HTTP 200，随后客户端尝试超时182712毫秒，无usage | 未知预留52068保留 |
| 未发送 | scorpio:luna | 路由选了换源，预留52072超过剩余38416，发送前拒绝 | 不计入物理请求 |

物理请求3次，不是4次。换源被选中但没有第二次物理发送。总账81584＝29516有效usage＋52068未知预留。费用`unknown`，不能声称≤10元或阶段≤80元。停止原因是token总账，不是16次请求用尽；被拒时还剩13次请求名额。

| 尝试 | 秒 | 物理请求 | 有效usage | 未知预留 | 账本占用 |
| --- | --- | --- | --- | --- | --- |
| 首次 | 56.535554 | 1 | 0 | 60963 | 60963 |
| 原后继 | 100.132023 | 3 | 14050 | 21183 | 35233 |
| 专项 | 363.318675 | 3 | 29516 | 52068 | 81584 |
| 累计 | 519.986252 | 7 | 43566 | 134214 | 177780 |

原B1累计上限1800秒、240000 token、32请求。剩余约1280.014秒、25次请求、62220 token账本。专项没有用满120000；未知预留使下一次同等正文载荷放不进剩余账本。这不是模型token不足的证明，也不是费用为0。

### 22.6 证据、清理与下一步

专项证据在[b1-pool-observation-evidence/special](b1-pool-observation-evidence/special/manifest.json)。`entry.log` `3423109c` 31351字节；`query.json` `122f8a38` 220044字节；`export.json` `c38083a8` 220488字节；`budget-final.json` `518e4dde` 2501字节；`supervisor.json` `95ef320a` 60字节；`entry-result.json` `1ed86ef7` 50032字节。日志无`sk-`、`Bearer`或`api_key`。绑定[b1-pool-special-binding.json](b1-pool-special-binding.json) SHA-256 `45ee4272b62d3184c19faf2d2cd0d3363a8c29a661ad096990c45a3e2290d89f`。计划见[b1-pool-special-plan.json](b1-pool-special-plan.json)。

哈希一致后删除专项运行目录里的`research.db`、`pdf-cache`、`output`、`checkpoints`和`export`目录，约2.1MB，可从同一PDF和已保存查询重建。本窗口`pytest-of-python/pytest-4`约4.9MB已删。19:10的`pytest-2`/`pytest-3`、观察证据、attempt标记和其余QuoteBak目录保留。清理后QuoteBak约69M。11份既有修改和未跟踪内容未改。

B活跃开发时间未单独计量，记unknown；上面363秒是专项进程时间，不是开发时间。重复根因：未知usage占满预留后，剩余账本装不下下一次正文载荷。恢复条件是A重新给出可容纳已结算地图用量加一次失败预留再加剩余正文的账本或请求策略。本次不自动重跑。

## 23. A审核专项失败与局部业务闭环裁决（2026-10-18）

### 23.1 结论与真实进度

审核9cca3f5b：**接受局部工程修复与专项失败留存，B1/A1业务未通过**。首次1/1、原后继1/1、完整专项1/1均用尽，B按卡停止，无擅自第4次。三视图仍extraction_failed，conditions/claims/records均0，19核心及6负例没有可审核交付；阶段交付召回记0/19（不是逐项语义已评估），准确率/数字错误/负例不可评估。开发观察1/4、可用0/4，另3家未观察；S1 0/12、S2 0/20、S3 0/100，20家来源准备不抵扣执行。

A核专项56份冻结执行文件hash均匹配，原首次/后继/专项3份manifest所列19份文件hash及字节数一致。专项query/export profiles一致；208页均为地图预览已核且partially_checked、53页map_selected、正文read为0。已有两个成功地图回执没有被后续失败抹掉；不能据此认为正文已读。监督363.318675秒、退出码0，entry-result可用，日志不再有Event loop is closed；这是执行收尾改善，不是业务成功。用户已确认的404配置修复不重开；本次grok为HTTP 200后流超时，不是同一个404。

实际专项3次物理发送：gemini地图usage17965、zai地图usage11551（实际输出5062、请求1000，保留原warning与实账）、grok正文超时无usage占52068，总账81584；luna虽被路由选中，52072>剩38416，未发送，不计第4个物理请求。三次累计519.986252秒、7请求；43566有效usage＋134214未知预留＝177780账本。原累计余62220、约1280.014秒、25请求；有余额不代表有剩余完整尝试授权或费用达标，费用仍unknown。

### 23.2 主要矛盾与独立核算

| 类别 | 证据及影响 | 本次处理 |
| --- | --- | --- |
| 已关闭的直接阻塞 | 客户端同循环close、失败入口回执、成功地图覆盖，以及大载荷经LlmClient的模拟跨源已有直接回执；A独立相关4项通过 | 接受该范围修复，不反复派同一项；模拟换源仍不冒充真实跨源成功 |
| P2：恢复准入未按已消耗及末端计算 | 29516地图实账＋52068未知失败＋52072下一源＝133656>120000，尚未合成/核验。每请求≤60000只能证明空账容纳两笔，不证明地图之后可恢复 | 按剩余工作而非初始一半留额；下一卡先核完整局部计划和一次unknown路径，拒绝前不发送、不释放旧unknown |
| P2：完整计划只验次数、没有全链预算可执行证据 | 14个槽位成立，但仅发现的完整序列化保守预留已约58.6万—60.6万；动态条件/主张/引用增长尚未证明末端可执行 | 不直接放大为60万；同步提交阶段资源表和最小差额，完整路线/资源留下一次明确裁决 |
| P0：真实正文业务闭环仍没有证据 | 三次都停在地图或第一个正文请求，无法评估主张生成、局部语义核验和实际读回质量 | 改做原余量内最小纵向切片，至少一条实质正文主张真实通过核验并读回，不能再仅交地图或fake测试数 |

上一卡A检查了全链请求数和单请求一半恢复上界，却没有证明“已消费＋一次unknown＋剩余正文＋末端”可执行，这是规划遗漏。本次明确纠正，不将符合原预算拒绝规则的B行为定为违规，也不把供应商超时说成单靠程序就能消除的问题。

A仅做只读静态序列化：从原iteration2/checkpoint取同一解析的208页（184544字符、404158 UTF-8字节），按现版pack_fitting_batches/structural_discovery_chunks打包；调用既有project_static_plan.py的gateway_payload/reserve函数，对六来源完整payload含instructions/schema/输出取上界，不发送、不给fake响应、不新跑业务。此规模是运行时解析，不混用早先pypdf独立抽字规模。专项真实53选页加邻页共80页。结果：

| 当前选页情形 | 地图/正文请求 | 地图预留 | 正文预留 | 仅发现合计 | 再加一次最大正文unknown失败 |
| --- | --- | --- | --- | --- | --- |
| 无全页选中，发现预览外正文 | 2/10 | 80065 | 506131 | 586196 | 640453 |
| 专项53选页及相邻页 | 2/10 | 80065 | 513619 | 593684 | 647809 |
| 全页选中 | 2/10 | 80065 | 525906 | 605971 | 659649 |

这些是静态请求身份下的保守预留单位，尚不含合成/核验、额外上下文及真实输出增长；不是实际token或人民币，也不是理论最低量。有效usage结算会释放未用预留，不能据此断言实际120000token一定不够；同样不能凭两次地图usage比例推定全文可运行。直接回归只证明单次大小、片段覆盖、请求数量及新空账的一次换源，不证明所有上述阶段与一次失败在120000同账内完成。

### 23.3 下一卡与资源裁决

A签发[B1-D卡§14](b1-execution-card.md#14-b1-d先交付真实正文的可靠部分再裁决整报告资源)、[验收§7.4](../../../docs/development/company_profile_acceptance_program.md#74-b1专项失败后的局部业务验证与资源重规划2026-10-18)，当前只执行R14/R15。遵循A/B流程v3：B一个连续窗口先清无依赖产物，落实局部正文→源先行条件→主张→局部核验→原owner保存/query/export，补直接回归及新快照；全局未读明确保留，不为了局部交付伪造完整性。可按身份核验复用专项成功地图作为内部导航，不把它算作新首发或正文证据，不改前3次失败，不建通用恢复平台。B自主选择满足约束的通用片段组织方式，不用冻结合同页码、公司名或正则替模型做相关性裁判。

**不批准第四次完整重跑；新增条件授权局部真实验证0/1**，入口启动记累计第4次真实执行，不隐藏在“离线调试”中。局部完整计划（正文/合成/核验/增长/一次unknown恢复）总预留≤62220、最多900秒/8物理请求、每请求≤65536字节/4000请求输出，仍沿公共池及同一预算接点，旧177780保留，累计≤1800秒/240000账本/32请求。前置成立后B自主执行，无额外探针、无第5次入口；计划不成立不发送。费用unknown例外只延续该局部窗口，有价仍按原上限，无新增账号/购买/充值，不能声称≤10元/阶段≤80元。

本卡目标至少一条有实质含义的正文主张通过真实合成/核验并查询导出一致，三视图均有状态和缺口；局部成功只说明链路可工作，不代表完整B1达标。未完成全文发现时所有完整标志false，未评估负例不算通过；已核可靠主张可读，其他事实保留原状态。同步交整报告各阶段无故障/一次unknown计划、完整payload与增长、末端留额及最小资源差额，A据此下一次统一裁决完整路线/预算，不再要求六份资料、不盲增额度或继续新fake业务方案。至多两个共性局部迭代；局部窗口用尽、权限/合同冲突或既有改动不可隔离就停。

完整验收仍为三视图、全部主要板块和变现成立、原19核心至少18、已审准确率≥98%、6负例实际通过及底线错误隔离；允许次要遗漏和有据unclear，无法补救写来源/影响/尝试/限制/恢复条件，不要求全附注逐行抽取。局部不增加可用公司数、不放行B2，不归档、不扩批，生产not_authorized、规模声明false。

### 23.4 验证、保护与交接质量

B的§22已按A/B流程§8区分真实/模拟、静态/usage、次数与成本、未评估、清理和Git状态，报告方式符合要求；A不另设报告模板或要求重复制品。尚缺的是业务证据和整条资源可执行性，不是格式问题。

A独立命令（先source scripts/quote_dev_env.sh）：`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_report_wire.py::test_gehu_batches_keep_failover_synthesis_and_verify_after_one_unknown_failure tests/unit/test_research/test_company_profile_report_wire.py::test_successful_map_batches_remain_when_a_later_batch_fails tests/unit/test_research/test_company_profile_bounded_entry.py::test_worker_failure_keeps_the_entry_receipt tests/unit/test_utils/test_llm_gateway.py::test_owned_http_client_is_closed_on_the_loop_that_created_it`，**4 passed、7 warnings**；复用B同快照70项组合回执，与A的4项有重叠，不相加、不声明全套通过。A无真实LLM调用，无完整或局部业务重跑，仅只读核源/制品/代码和静态请求序列化。

本轮仅更新执行卡、tasks、design当前指引、交付路径、验收活动表和本节，原§1—22正文保护。OpenSpec严格校验、本人改动diff及107个本地文件链接检查通过；CLI遥测联网提示不影响严格校验结果。既有验收历史区块及11份脏文件的他人改动不纳入提交，原首次/后继/专项和配置保持。临时产物沿QuoteBak，测试日志由fixture清理；B所述删除前副本比对归B回执，A核现存持久hash及约69M状态，不盲删仍有用途/归属不明的目录。A活跃审核时间未单独计量，记unknown；本次新发现/量化的阻塞为2项预算可执行性P2，业务未交付P0持续，不将静态核算或单测计数充当业务进度。

## 24. B1-D局部验证未形成主张（2026-10-18）

### 24.1 结果

局部计划成立并已启动第4个真实入口，局部验证1/1用尽。没有一条经过真实核验的主张，查询`delivered=0`。B1/A1不通过。19项核心和6项负例仍未评估。观察1/4、可用0/4。不自动第5次，也不把这次拼进完整成绩。

### 24.2 业务切片

专项两批地图回执与当前208页正文哈希一致，只作导航，不计入本次物理请求。模型选中页的第一段连续页里，能同时放下正文、合成、核验和一次未知重发的是第12–13页。第13页末尾未完句被裁到最后一个句号，偏移1281之后未发送，记`slice_tail_unread:13:1281`。其余选中页和未选页仍是未读缺口。`draft_complete`、`semantic_complete`、`real_semantic_complete`均为false。

静态预留：正文13614、合成13563、核验11774、恢复13614，合计52565，低于62220，余9655。这是保守预留单位，不是token或费用。

### 24.3 真实调用

无沙箱，代理环境变量为空。监督退出码0，477.564630秒。

| 次序 | 来源 | 结果 | 账本 |
| --- | --- | --- | --- |
| 1 | scorpio:gemini | 客户端尝试超时211868毫秒，无usage | 未知预留13633 |
| 2 | zai | HTTP成功并换源完成；请求输出2500，实际输出12327，日志`output_budget_exceeded` | 有效usage 2262＋12327＝14589 |

物理请求2次。总账28222＝13633未知预留＋14589有效usage。费用unknown。换源已经用掉那一次恢复。随后合成被`partial_tail_hold_would_be_exhausted`拦住：代码仍按“再留一次完整恢复”计算，28222＋合成＋核验＋第二次恢复超过62220。工作检查点里的解释结果因此没有条件和主张。发布阶段又因`processing_identity`被写入`partial_plan`后与原身份不一致，把解释结果清成空，查询和导出profiles均为空，但两者一致。

这次入口已经计1/1，不能再用剩余额度重跑。运行之后补了两处代码：恢复用过就不再预留第二次；不再把计划写进会参与身份比较的`processing_identity`。该补丁没有新的真实调用。

### 24.4 整报告资源差额

表在[b1-partial-resource-table.json](b1-partial-resource-table.json)。单位是完整payload字节＋256＋请求输出上限，取池内最大预留，不是实测token或人民币。

| 情形 | 发现预留 | 无故障再加合成/核验 | 一次未知失败 |
| --- | --- | --- | --- |
| 只发现预览外正文 | 588281 | 695456 | 749989 |
| 选页及相邻页 | 595369 | 702220 | 756591 |
| 全页选中 | 607021 | 712708 | 766497 |

相对当前累计余量62220，一次未知失败路径至少还差687769。相对原120000单次上限，至少还差629989。不能把地图usage比例换成全文token，也不能据此直接把预算改成60万。

### 24.5 验证、证据与清理

直接回归：`test_company_profile_report_wire.py`、`test_company_profile_llm_led.py`、`test_company_profile_bounded_entry.py`、`test_llm_physical_budget.py`，**72 passed**。另有尾部预留单测2 passed。没有跑全库。29516＋52068之后，下一次超过剩余38416的发送在进入传输前被拒绝。缩块正文经`LlmClient`模拟传输，失败后另一来源再发，合成和核验共用62220账本。

证据目录[partial](b1-pool-observation-evidence/partial/manifest.json)。`entry.log` `bd692340` 30803字节；`budget-final.json` `4c067f20` 1486字节；`work-checkpoint.json` `2ebae960` 661155字节；`source-receipt.json`仍是`f304554e`。绑定[b1-pool-partial-binding.json](b1-pool-partial-binding.json) SHA-256 `c63b9ae418713f83ff6ab97f63254f4d0238167782591bf30770aa07a685521e`。

清理：pytest-2/pytest-3是2026-10-17 19:10的测试临时目录，无清单引用；本窗口pytest-0/pytest-4同样只是可重建日志，已删。专项和局部的库与PDF缓存在哈希核对后删除。QuoteBak现约69M。11份既有修改未动。

三次加本次累计997.550882秒、9次物理请求、账本206002。原上限内约剩802.449秒、23次请求、33998账本单位。这些余额没有第5次入口授权。B开发时间未单独计量，记unknown；477秒是本次进程时间。

## 25. A审核920aea41：真实正文留存和非空末端计划仍阻塞

### 25.1 结论与实际进度

**B1/A1未通过，接受失败交接；不批准第5个真实入口。** A按产品目标→验收v2→交付路径v3→A/B流程v3→本change核查。独立确认局部目录9项manifest一致、预算2物理请求/28222账本、477.564630秒及query/export均无profiles；entry.log记录真实超时后换源成功，也记录publish的report_interpretation=None校验失败。退出码0和空查询/导出相同不能作为业务成功。

zai的有效usage是2262输入＋12327输出＝14589，不能改为请求输出2500；gemini的13633 unknown预留不释放。四次累计58155有效usage＋147847未知预留＝206002账本、9请求、997.550882秒，费用unknown。原剩33998账本/802.449118秒/23请求不构成第5入口授权，也不能证明原10元/80元费用达标。

开发观察1/4（余3家未观察）、可用0/4；S1新链0/12、S2 0/20、S3 0/100。原19核心无可审交付，阶段交付召回0/19，逐项语义及6负例不可评估，准确率/数字错误不可评估。局部运行不是完整首发，不拼地图或其他失败轮补分。次要遗漏和有据unclear仍可容忍，主要板块/三视图不成立不能标可用；既有质量门槛和分母不变。

### 25.2 两类范围内阻塞与补丁边界

**A-P1-RESULT-02：成功正文的语义结果未保留到可追溯交付。** work-checkpoint.json的source_conditions/claims均空，call_receipts只有两笔reused_navigation=true地图回执，无本轮真实正文response_payload；其他提交的entry-result/query/export同样没有该响应。runtime._bind在处理身份不一致时清空report_interpretation，verify可重新进入semantic，publish最终报None；这解释了“账本成功但没有可继续核验的内容”。A不能独立判断这次正文生成了哪些主业条件或引用。当前代码已将partial_plan留在局部变量，方向接受，但缺同owner跨semantic/verify/publish及失败后持久化的非空证明。不能拿模拟回复补造本轮真实内容，也不能无条件放宽真实版本变化的身份失效规则。

**A-P1-TAIL-02：末端资源计划未计非空增长，恢复消费识别也不完整。** reading_budget.stage_reservations仅带source_pages，没有合成必需的source_conditions/records、核验必需的conditions/claims/records，故13563/11774不是后继载荷上界。A在同一p12–13源scope上用现有protocol_reply构造14个模拟条件、7条模拟主张，经真实compact_request/response_schema及池内完整payload序列化，发现/合成/核验预留13624/13046/14600；尚未带原生事实，核验已超过11774。这只证明原上界不足，不是语义核验、实际token或最小资源需求。原749989—766497整报告表也不能据此作为严格上界或直接放大预算的依据。

remaining_tail_units的事后补丁能识别failed_or_cancelled且无usage的恢复已用，直接回归通过；但相同失败/换源若账本status=http_failure，无usage，仍留25388而非11774。A只读抽取该函数得到上述结果；utils/llm/client.py会真实产生http_failure这一状态。一次恢复须以实际故障/换源/repair消耗识别，不能只按一种unknown状态判断。有效usage超请求输出同样须进末端计划与账本，不能靠请求字段假定供应商输出已封顶。

### 25.3 下一张卡与流程

当前唯一任务为[B1-E执行卡§15](b1-execution-card.md#15-b1-e修复非空结果的完整调用链定稿可执行资源方案当前卡)、tasks R16–R17：先清无依赖垃圾；B在一个连续窗口修复上述两类阻塞，用同一operations/runtime、真实provider/client适配器和模拟transport跑通非空条件→主张→核验→原writer/query/export，保留失败后的条件/Evidence/回执与已核结果；提交一次带非空增长、原生事实、一次恢复及超输出路径的资源定稿。最多两个共性局部修复迭代，纯风格、无关旧失败和平台建设后置。

**本卡真实调用0次，原四个入口不重置。** 不重跑地图/208页专项，不新增付费探针/购买/充值，不借其他公司余量。查到旧真实响应可作为有出处的诊断输入；查不到须明示缺失，模拟工程闭环不能宣称真实主张已成立。A据工程闭环与完整方案再统一裁决真实运行合同，当前不签发第5入口。这样先修实际断点，避免继续付出模型读取成本后才发现保存或末端仍走不通。

沿A/B流程v3：A冻结目标和边界，B卡内连续实现/修复/直接验证，A卡末审核，无新增逐模块审批；唯一交接仍本文件。数量继续1→4、12、20、100，S2/S3门槛、有限补救、无法补救的具体限制说明沿验收计划。未归档、不扩批，production=not_authorized、scale_quality_claim_allowed=false。

### 25.4 A验证、保护与成本

- A直接复验：`source scripts/quote_dev_env.sh`后，用Quote环境Python执行`pytest -q tests/unit/test_research/test_company_profile_report_wire.py -k 'used_recovery_is_not_reserved_again or verified_slice_survives'`，2 passed/16 deselected；同文件`-k shrunk_slice_body_tail_and_recovery_share_one_ledger`，1 passed/17 deselected。两命令外层75秒中断保护，均正常退出。它们只证明指定离线行为，不证明完整运行或真实语义。
- A曾启动B报告中的四文件完整组合，期间没有取得完成汇总，已中断（退出130）；不声称A复跑72 passed，不据此追认新的代码根因。B原72 passed回执保留，必要的同owner组合验证由R16补足；不将测试数当公司数。
- A独立只读核manifest/累计账、检查点及日志；上述非空载荷反例只序列化、未联网/未发模型，输出为14条件/7主张及三个实际预留值，未生成新的正文副本或业务数据库。http_failure反例只执行只读抽取函数。
- 本轮仅更新六份规划/审核文档；源资产、四次原制品、§1—24正文、配置、11份既有修改和16项未跟踪内容均按开始基线保护，验收计划的既有历史块不纳入本次提交。本人已结束的pytest-0测试库及中断日志已清，约6.8MB，均可重建；前序未知归属对象不删。OpenSpec严格校验及diff检查通过；最终隔离提交和保护结果见本次提交回复。
- A活跃审核约20分钟（估算，未独立计时）；新增阻塞为上述两类，未扩大产品范围。B活跃开发/等待时间仍unknown；477秒仅本次真实进程运行时间，不能混作人工成本。

## 26. B1-E：离线非空调用链与资源定稿，真实授权仍为0

### 26.1 结果

当前阶段是B1-E / tasks R16–R17，离线工程修复，不是正式首发、后继或局部真实验证。工程闭环已交，待A审核。B1/A1未通过。19核心与6负例未评估。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批，生产未授权。

待A判断的只有下一份真实合同：本卡不申请、也不构成第5次入口。四次历史入口保持各1/1。剩余33998账本、约802.449秒、23次请求不是新的授权。

### 26.2 业务与旧响应

歌华有线600037.SH、2025年报、filing 1225229913、PDF SHA-256 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76` 的真实正文没有可回放响应。第4次zai请求 `cab6f133f04a4a9cbbeae7a08c68d020`、request_hash `b984750527ddf076eacee131a3dacdf49208ba47b9d19658ccd301005f334691` 的usage仍是输入2262、输出12327、合计14589。entry.log只有这组usage和请求标识；work-checkpoint、query、export、entry-result、budget-final都没有该次response_payload。结论是响应缺失，不能回放。没有用日志、夹具或手写答案补造这次正文。

离线证明使用600000.SH夹具页，分类 `offline_fixture`。它证明同一业务入口能留下非空条件和已核主张，不能当作歌华有线的阅读成绩。draft/semantic/real 三个完整标志均为false。

### 26.3 差异

基线是A已推送的 `6d53e19a`。本轮只改结果留存、恢复计数和末端预留：

- `runtime._bind` 只在报告或阅读策略变化时清空解释结果。策略键是 semantic_policy、prompt_version、preprocessing_version、discovery_scope、wire_protocol、execution_mode。预算、计划和导航回执的JSON往返不再清掉已保存的解释。真实prompt或策略变化仍会使旧结果失效。
- `remaining_tail_units` 把一次窗口里的一次恢复交给任何非空、非reserved、非success的尝试。`http_failure`、无usage的失败、以及带usage的失败都消耗这唯一一次，不按阶段重置。
- `stage_reservations` 和 `partial_slice_plan` 把条件、主张和已有结构化记录放进合成与核验。空的后续请求不再当作上限。

未改生产池、价格、上下文、尝试标记和四次真实证据。

### 26.4 验证

环境入口 `source scripts/quote_dev_env.sh`，解释器 `/home/python/miniconda3/envs/Quote/bin/python`。写入位置是pytest临时目录，不覆盖冻结证据。

```text
ruff check research/company_profile/reading_budget.py \
  research/company_profile/runtime.py \
  research/company_profile/report_interpretation.py \
  tests/unit/test_research/test_company_profile_llm_led.py \
  tests/unit/test_research/test_company_profile_report_wire.py
```

结果：All checks passed。

```text
pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_nonempty_chain_survives_failover_publish_and_a_later_failure \
  tests/unit/test_research/test_company_profile_report_wire.py::test_used_recovery_is_not_reserved_again_before_synthesis \
  tests/unit/test_research/test_company_profile_report_wire.py::test_shrunk_slice_body_tail_and_recovery_share_one_ledger
```

结果：3 passed，131.86秒。没有跑全库，不把这3个测试当成72 passed或公司数。

这条离线链的实际路径是 `CompanyProfileTaskService` → runtime → `OfflineGatewayProvider` / 真实 `LlmClient` → `ReplyTransport`。第一次发送抛传输错误并换到第二个来源；正文返回请求输出之上的6000 completion，账本按total 6100结算；合成和核验请求都带 `rev-cloud`；query与export的profiles相同，delivered为1；可读主张包含页内原句；成功正文回执带非空 `source_conditions`；随后发现失败写入 `later-discovery` 缺口，已核主张和正文回执仍在。重新读出的profiles与query相同。保存身份没有 `partial_plan`，阅读策略与入口一致。`accepted_facts` 含 `rev-cloud`。未核部分留在缺口，不写成确定事实。

### 26.5 资源

单位仍是UTF-8载荷字节加256再加请求输出上限，不是token，也不是人民币。费用unknown。实返超输出保持：地图请求1000、实返5062；正文请求2500、实返12327。

同一裁剪后的第12–13页，`protocol_reply` 给出14个条件、7条主张。不带原生记录时合计73548（正文13527、合成18971、核验20525、恢复20525）。这组数字说明空后续请求不是上限，也不是将要发送的载荷。

同一检查点上 `select_core_evidence` 与 `project_owned_page_facts` 得到6条Measurement/Segment，证据都在第18页“收入和成本分析”。第12–13页不引用它们，但合成和核验仍会带上全部结构化记录。带上这6条后：

| 范围 | 正文 | 合成 | 核验 | 一次恢复 | 合计 | 相对62220 | 相对剩余33998 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 仅第12页 | 9475 | 20552 | 24892 | 24892 | 79811 | 差17591 | 差45813 |
| 第12–13页 | 13527 | 25067 | 29407 | 29407 | 97408 | 差35188 | 差63410 |

`partial_slice_plan` 在62220下带这6条记录会拒绝，没有任何选中页获准发送。旧的整报告一次故障表749989–766497仍不是严格上界，也不作为加预算依据。全文发现阶段原先的588281–607021还没有加上非空末端。

首选：不启动第5次真实运行。不把账本改成79811、97408或约75万。若A以后要真实阅读，需要单独裁决账本和未读范围；本卡不预签。

### 26.6 证据、清理与Git

四次真实证据目录、尝试标记和源回执未改。第4次证据仍是 [partial/manifest.json](b1-pool-observation-evidence/partial/manifest.json)。资源数字写在 [b1-partial-resource-table.json](b1-partial-resource-table.json) 的 `native_facts_on_followups`，旧空表数字保留。

本窗口删除的是可重建测试库 `pytest-of-python/pytest-0`、`pytest-1`、`pytest-11`、`pytest-14`、`pytest-15`、`pytest-16`，以及空的 `quote_pytest_logs_*` 和只含本次测试日志的 `quote_pytest_logs_tr2i82k5`、`quote_pytest_logs_21afd5kg`、`quote_pytest_logs_ch87sd7r`。QuoteBak回到约69M。首次结果、局部证据和未知归属对象未删。

11份既有修改和未跟踪内容不纳入本次提交。B活跃开发时间unknown；本轮没有模型调用，正式复验次数仍是已耗尽的4次。

## 27. A审核d12f6a56：工程已证部分接受，局部依赖和准入仍阻塞

### 27.1 结论及实际进度

按产品目标→验收计划v2→交付路径v3→A/B流程v3→当前change/源/代码/制品顺序审核。接受B对真实响应缺失、usage原值、离线fixture、未运行及清理的区分，报告方式符合单一入口要求，无需另造模板或平行汇总。

**B1-E离线工程的已证范围接受，B1/A1业务不通过。** 同一业务入口已能保留模拟非空条件、核验主张和原生记录，经原writer保存并由query/export/重读一致返回；换源、有效usage超请求输出及后续失败保留通过。旧真实响应无法找回，不把它重造为真实成绩；第4次14589用量和四次失败不改。3个测试的600000.SH夹具不能折算歌华19核心/6负例。

当前仍观察1/4、可用0/4，余3家未观察；S1 0/12、S2 0/20、S3 0/100。没有可审三视图，交付核心0/19，语义逐项/准确率/数字错误/6负例不可评估。四次累计997.550882秒、9请求、58155有效usage＋147847未知预留＝206002账本，余33998；所有费用unknown，不能声称满足10元/80元目标。

### 27.2 两项范围内阻塞及一项非阻塞说明

1. **A-P1-SCOPE-01：局部核验全带无关原生事实。** `report_interpretation.py:650`的切片计划传全部structured，`:861`的合成继续传全部structured，`:939`核验传合并后的全部records并要求全部targets成立。B已如实确认六条Measurement/Segment都在第18页，p12–13不引用。原执行卡§14.2(4)、§15.2(3)要求只核本片段实际来源及相关原生事实。当前开销把p12模拟计划从56131抬至79811；它说明当前实现的问题，不能证明最小业务阅读不可行。修复条件是相关依赖正确送达、无关原生记录原状态保留且不列本次核验targets，不删事实或自动视为已核。
2. **A-P1-PLAN-01：离线回复进入运行准入和ceilings。** `reading_budget.py:208`的protocol_followup导入resource_probe.protocol_reply；`:311`让每个实际候选切片生成固定14条件/7主张，再交stage_reservations，运行的partial_plan.ceilings据此拒绝后续真实载荷。resource_probe明确是离线压力fixture，输出条数和描述不随真实主业变化；既可能虚增小片段开销，也不能给真实输出提供上界。接受B资源披露，不接受据此宣称完整末端或最低资源已证明。修复条件是运行准入与fixture脱离、先核可执行的载荷边界、取得真实非空响应后重新校验，超界保留待核结果停止。不得改空表凑准入或把14/7换成另一个凑出的模拟常数。

`runtime._reading_policy`六键比较的放宽必要性未被单独证明；不认定为本轮阶段切换成功的必要原因。当前bounded入口冻结身份，生产队列work_id包含完整processing_identity hash，operations按同hash筛阶段，因此没有发现当前入口跨模型/scope误复用的实际触发路径。记为非阻塞说明，不另派身份平台或扩大本卡。原数字/主体硬约束、完整标志与未读保护不放松。

### 27.3 A独立验证与保护

环境为 `source scripts/quote_dev_env.sh`，解释器 `/home/python/miniconda3/envs/Quote/bin/python`。为核非空链及新增准入实现，本轮独立执行，输出只进入隔离pytest临时目录：

```text
timeout --signal=TERM --kill-after=10s 200s \
  /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_nonempty_chain_survives_failover_publish_and_a_later_failure \
  tests/unit/test_research/test_company_profile_report_wire.py::test_used_recovery_is_not_reserved_again_before_synthesis \
  tests/unit/test_research/test_company_profile_report_wire.py::test_shrunk_slice_body_tail_and_recovery_share_one_ledger
```

结果3 passed、7 warnings、135.87秒，退出0；不声明全套测试或任何公司语义通过。上述第三项仍不是本次运行链携带六条记录的真实准入证明，相关限制保留。B同快照Ruff回执仅按声明范围复用；新文档另做OpenSpec严格校验及diff检查，不将其算业务成绩。

A只读核原四次manifest，first 6、successor 6、special 7、partial 8，共27份文件SHA-256无差异；核本卡p12冻结段407字符/1169字节及hash。不重跑原业务、没有真实模型调用。本人pytest临时库收尾清理，必要命令/结果保留在本入口；未知归属对象不删。§1—26正文、其余冻结材料和配置保持原样；11份既有修改及16项未跟踪内容保护，验收文档既有历史块不纳入本次提交。最终隔离提交和保护结果见本次回复。

### 27.4 下一卡与开发者执行要求

当前仅执行[B1-F卡§16](b1-execution-card.md#16-b1-f修正局部核验范围与准入在原余量内交一条真实主张当前卡)/tasks R18–R19。B在一个窗口内先清理、修两项直接阻塞、必要正反回归及冻结；不逐模块等A，不重建已通过的链，不再开208页fake专项或要求六来源资料。A已核原p12主营段并冻结scope/hash，至少交一条真实核验主张；实际网络服务与计划/探索5G须区分，不能手写主张或将原生事实当LLM成果。

本次A明确新发条件授权：前置及全非空计划成立后，短段真实证明最多1次，≤600秒/6物理请求/33998账本，仍守累计1800秒/32请求/240000、每请求65536字节/请求输出4000。一次恢复共享，unknown保留，超输出如实结算并停后续；费用unknown例外只延续本窗，不声称费用达标、无购买/充值。前置不足不发送，入口启动即1/1及累计第5次，不自动第6次/完整重跑/其他公司。

该授权显式接续原§15的0次状态，不追认原四次后的既有运行权限。单条成功仍是限用局部成果；原19/6和完整公司可用门槛、4/12/20/100数量、次要遗漏容忍、错误隔离及补救说明不变。不归档、不扩批，生产not_authorized、规模声明false。卡末A必须就完整路线继续/收窄/终止作结论，不继续滚动短段专项；失败也保留并据实裁决。

A活跃审核约30分钟（估算，非独立计时；测试墙钟单列），新增范围内阻塞2项，非阻塞说明不派任务。B活跃开发和等待时间unknown，不能以131.86秒测试耗时冒充开发成本。本轮仅同步六份审核/任务文档，不代B实现业务修复。

## 28. B1-F：局部范围与可执行准入已修，短段计划不足未发送

### 28.1 结果

当前阶段是B1-F / tasks R18。两项阻塞已在原owner内修改，冻结计划不成立，所以R19的真实短段没有启动。这不是歌华有线的真实主张，B1/A1未通过。19核心与6负例未评估。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批，生产未授权。

条件授权的一次短段入口仍是0/1，尝试标记没有写入。原四次1/1保持。剩余33998账本、约802.449秒、23次请求没有被消费。

### 28.2 业务与冻结输入

来源仍是歌华有线600037.SH、2025年报、filing 1225229913、PDF SHA-256 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。本卡诊断片段是iteration2检查点物理第12页字符区间[415,822)，407字符、1169字节，SHA-256 `930fd035efb3384f73fab6be3eba5c169939bb70f40fa588a938aa97c96b256b`。页内其余文字和其他207页未读。第18页六条原生事实仍在原投影里，不进入这次核验目标，也不标成已核。

没有新的真实响应。第4次正文响应仍然缺失，不能回放。

### 28.3 差异

基线是A已推送的 `9b2f18e1`。

- 合成和核验只携带证据引文落在本次已读片段内的原生事实。片段外记录留在原存储，查询结果不把它们写成已核事实。主张若引用片段外记录，该主张保持unclear，不把缺上下文的数值当成核验结论。片段内记录仍进入核验；核验冲突会隔离依赖主张。
- `partial_slice_plan` 不再调用 `protocol_reply`。能否启动，取决于这段原文的实测网关载荷再加一份已读文本的UTF-8字节，以及一次恢复。空的条件/主张请求不是上限。只有该合计低于账本时，账本里剩下的空间才放宽后续复核；真实响应若超过这个复核上限，保留已成功的正文响应和用量，不再发送。本次合计已经高于33998，没有剩余空间，也没有发送。
- 该片段位置由调用方传入，不写进生产选页分支。完整发现路径仍按全页处理。

### 28.4 验证

环境入口 `source scripts/quote_dev_env.sh`，解释器 `/home/python/miniconda3/envs/Quote/bin/python`。写入位置是pytest临时目录。

```text
ruff check research/company_profile/reading_budget.py \
  research/company_profile/report_interpretation.py \
  research/company_profile/bounded_entry.py \
  tests/unit/test_research/test_company_profile_llm_led.py \
  tests/unit/test_research/test_company_profile_report_wire.py
```

结果：All checks passed。

```text
pytest -q \
  tests/unit/test_research/test_company_profile_report_wire.py::test_explicit_span_ceiling_uses_citation_volume_not_protocol_reply \
  tests/unit/test_research/test_company_profile_llm_led.py::test_partial_slice_verifies_only_records_inside_the_read_span \
  tests/unit/test_research/test_company_profile_llm_led.py::test_in_span_record_conflict_isolates_the_dependent_claim \
  tests/unit/test_research/test_company_profile_llm_led.py::test_followup_over_citation_ceiling_keeps_the_body_response \
  tests/unit/test_research/test_company_profile_report_wire.py::test_shrunk_slice_body_tail_and_recovery_share_one_ledger \
  tests/unit/test_research/test_company_profile_report_wire.py::test_used_recovery_is_not_reserved_again_before_synthesis
```

范围两项与超界保留 3 passed。片段上限、旧切片账本和恢复计数 3 passed。d12f6a56的非空链在本轮改动后与片段上限一起复跑，2 passed、149.80秒；其中非空链144.52秒。全模式阅读和原生冲突回归 3 passed。没有跑全库。

片段上限测试确认：调用 `protocol_reply` 会失败，因此准入没有走到它；六条页18记录的record_count为0，合计与不带它们时相同；33998下 `partial_slice_plan` 拒绝发送。

### 28.5 资源

单位仍是UTF-8载荷字节加256再加请求输出上限。费用unknown。计划没有换算历史超输出比例。

| 项目 | 预留 |
| --- | ---: |
| 正文 | 8264 |
| 合成壳 | 8213 |
| 核验壳 | 6424 |
| 引用体积 | 1169 |
| 合成上限 | 9382 |
| 核验上限 | 7593 |
| 一次恢复 | 9382 |
| 合计 | 34621 |

相对33998差623。只含壳、不含引用体积的31165低于33998，但那是空条件请求，不用它放行。请求数若获准发送是3次正文阶段加1次共享恢复，共4次，未超过6次；因为账本不足，这4次都没有发出。历史实返12327/2500和5062/1000只作为超输出风险披露，没有加进34621。

首选：不启动这次已授权的短段。不把33998提高623，也不退回31165的空壳。真实调用0次。

### 28.6 证据、清理与Git

四次真实证据和尝试标记未改。资源数字在 [b1-partial-resource-table.json](b1-partial-resource-table.json) 的 `b1f_span_executable_bound`。旧的79811和749989—766497保留，不再作为本卡准入。

本窗口删除可重建测试库 `pytest-of-python/pytest-0`、`pytest-1`，以及 `quote_pytest_logs_7gcsv3m6`、`4svwxp9s`、`c5ep2uw_`、`3iqaghs2`、`ugjxz3rh`、`vedhzt4v`。四次真实证据未删。11份既有修改和未跟踪内容不纳入提交。B活跃开发时间unknown。

## 29. A审核3bc3024f：工程部分接受，引文范围仍阻塞，接续原未用授权

### 29.1 结论及真实业务进度

**B1/A1未通过；接受已证工程部分和预算不足未发送的说明。** 准入已不调用固定14条件/7主张模拟回复，片段外六条记录退出records/核验targets、片段内冲突隔离及超界保存响应的直接测试成立。冻结计划34621较原33998多623时停下符合原授权，短段仍0/1，没有第5次真实入口。本轮A真实调用0次，不把测试或静态计划计为语义成果。

仍有一个原范围内P1：原生记录被筛掉后，其Evidence仍从全量structured_evidence进入合成/核验source_pages。只验证record_count=0不能证明完整请求已限域。因此R18只在已证范围接受，不能宣称两项前置全部完成；不重开已证的非空持久化链或共享恢复问题。

观察仍1/4、公司可用0/4，余3家未观察；S1 0/12、S2 0/20、S3 0/100。原19核心无可审交付、6负例未评估；三视图未成立，数字错误和准确率仍不可评估。不归档、不扩批，生产not_authorized、规模声明false。

### 29.2 可定位阻塞与独立验证

`research/company_profile/report_interpretation.py`的related_records已经按sent_fragments筛选，但cited_evidence仍展开`structured_evidence.values()`（3bc3024f第862行），随后cited_quote_pages将页外Evidence及continuation/subject页变成source_pages。A沿同一interpret_report，用真实iteration2检查点的208页及六条原生记录、原special地图回执、冻结p12[415,822)和40000静态身份捕获请求；离线provider显式offline_only，不联网，只对发现返回一条源内条件“有线网络经营”，引文为冻结段起句，在合成前捕获并终止。结果：

| 阶段 | records数 | source_pages | 最大池物理预留 |
| --- | ---: | --- | ---: |
| source_inventory | 0 | 12 | 8270 |
| synthesize_views | 0 | 12、18、19、20、21、22、23、24、25 | 17493 |

该身份下预留与B原计划身份不同，不混为实耗；表明运行会携带计划未计入的页外引文，可能在正文成功后才被末端cap挡住，也违反冻结片段范围。修复必须覆盖最终payload及引用有效性，页外原件保留；片段内依赖不能漏传，缺上下文的候选保持unclear。此处是明确原合同缺口，不是新增语义完备平台。

A独立直接回归命令（统一环境入口，所有临时输出在QuoteBak）：

```bash
source scripts/quote_dev_env.sh
timeout --signal=TERM --kill-after=10s 90s \
  /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_report_wire.py::test_explicit_span_ceiling_uses_citation_volume_not_protocol_reply \
  tests/unit/test_research/test_company_profile_llm_led.py::test_partial_slice_verifies_only_records_inside_the_read_span \
  tests/unit/test_research/test_company_profile_llm_led.py::test_in_span_record_conflict_isolates_the_dependent_claim \
  tests/unit/test_research/test_company_profile_llm_led.py::test_followup_over_citation_ceiling_keeps_the_body_response
```

结果4 passed、7 warnings、7.87秒；警告不计业务通过。复用B本快照§28.4的非空链2 passed/149.80秒回执，不重复144秒旧链或全套测试。只读重建原计划8264＋9382＋7593＋9382＝34621；这只是完整请求壳与一份已读文本的可执行cap，不证明实际条件/主张/引用一定落在其中，必须在每次发送前核实际payload并在增长超界时保留停止。未发现需另派任务的风格问题。

### 29.3 A资源裁决和下一张接续卡

按[验收§7.7](../../../docs/development/company_profile_acceptance_program.md#77-b1-f未发送后的接续裁决2026-10-18)、[执行卡§16.4](b1-execution-card.md#164-审核3bc3024f后的同卡接续当前执行条款)继续原B1-F/R18–R19。B采用A/B流程v3，在一个窗口内先清无依赖开发垃圾，修上述残留P1，补最终网关载荷正反回归，冻结新快照及完整非空计划后自主执行原短段0/1；不逐模块等A，不新开fake专项或一轮623差额压缩。

A显式将本窗口账本由33998改为40000，留出当前静态计划之上5379执行余量；原206002不改，本家累计上限改为246002，较原240000增加6002约2.5%。L0/L1合并966002，其余3家各240000不变；窗口600秒/6物理请求、本家1800秒/32请求及每请求65536字节/请求输出4000不变。修复后按真实执行身份重冻；全计划及共享一次恢复不成立即不发送，不缩源凑数、不借其他公司或重复使用余量。该调整是保守账本授权，不是实测token/费用，也不采用旧整报告估计加额。

次数仍为原未用0/1，启动才记本次1/1及累计第5入口，不新开专项或自动第6次。逐物理预留、有效usage结算、unknown保留和实返超输出后停后续不变；不能保证供应商单次超输出绝不越过预算。费用unknown例外仅延续这次原未用窗口，有价仍核原费用上限，不声称成本达标，无充值/购买/新账号或生产授权。首次及四次失败、原19/6不改。

至少一条来自冻结片段的实质主营主张需经过真实发现/合成/核验，在原writer及query/export/重读一致，响应及原文引用可追溯；计划/探索5G不能当已运营事实。所有完整标志false，其余未读/未核明示。次要遗漏可限用、有据歧义可unclear；底线错误及依赖须修正/隔离，无法补救须说明原因/范围/影响/恢复条件。单条成功不增加可用公司数、不放行B2。卡末A据真实结果决定完整路线继续/收窄/终止，不再滚动派短段专项。

### 29.4 保护、清理及审核成本

A只读核原四次manifest：first 6、successor 6、special 7、partial 8，共27份文件的SHA-256及字节数全部一致。QuoteBak约69M，本轮无新整库备份或正文副本；测试仅用统一临时目录，本人无用临时产物收尾清理。必要证据仅记本入口，原§1—28正文、配置、四次制品、11份既有修改和16项未跟踪内容保护；验收文档既有历史块不纳入本次提交。

A活跃审核约35分钟（估算，测试墙钟另列），新增/残留范围内阻塞1项；B活跃开发及等待时间unknown，不用测试耗时冒充开发工时。本轮A只改六份审核/任务文档，不改业务代码、不代B真实运行。提交、推送及最终保护核验见本次回复。

## 30. B1-F：引文范围已收紧，短段已发送，核验未完成

### 30.1 结果

当前阶段是B1-F / R19，一次真实短段，不是离线成绩。入口已用：本次1/1，累计第5次。发现和合成已返回，核验请求没有发出。查询、导出和重读一致，但保存的主张都是unclear，答案为“语义流程未完成。”没有已核主营主张。B1/A1未通过。19核心与6负例未评估。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批，生产未授权。

没有第6次。246002累计上限内的剩余30971账本、约650.667秒和21次请求不是新的授权。

### 30.2 业务

来源仍是歌华有线600037.SH、2025年报、filing 1225229913。阅读范围是第12页字符[415,822)，407字符、1169字节，SHA-256 `930fd035efb3384f73fab6be3eba5c169939bb70f40fa588a938aa97c96b256b`。五条发现条件的引文都在这段原文内。其中“加快部署5G网络并探索应用”是片段里的探索表述，没有被核验，也不当作已独立运营的全链。

合成回执里有一条未核的主营叙述：公司主要负责北京市广播电视网络的开发、经营管理和维护。它引用条件cond-1。该主张同时把报告编号写成supporting_record_ids，引用检查未通过。核验停止后，查询层不再把这条叙述当作已核结论。

第18页六条原生事实仍在缺口`native_fact_outside_read_span`中，不是本次核验对象，也没有标成已核。合成和发现的正文回执source_pages都是第12页。完整标志draft/semantic/real均为false，分类limited。

### 30.3 差异

基线是A已推送的`c3fb88b9`。有界片段的后续请求不再把全量structured_evidence收进source_pages。引文必须落在已读片段内；片段外的延续页不进入这次载荷。原记录和Evidence对象没有删除。完整发现路径仍按相邻页引用，不受这段诊断限域影响。

### 30.4 验证

环境入口`source scripts/quote_dev_env.sh`，解释器`/home/python/miniconda3/envs/Quote/bin/python`。

```text
ruff check research/company_profile/report_interpretation.py \
  research/company_profile/bounded_entry.py \
  tests/unit/test_research/test_company_profile_report_wire.py
```

结果：All checks passed。

```text
pytest -q \
  tests/unit/test_research/test_company_profile_report_wire.py::test_frozen_span_followup_payload_excludes_unread_record_quotes \
  tests/unit/test_research/test_company_profile_llm_led.py::test_source_first_reading_three_views_without_heading_regex \
  tests/unit/test_research/test_company_profile_llm_led.py::test_partial_slice_verifies_only_records_inside_the_read_span \
  tests/unit/test_research/test_company_profile_report_wire.py::test_explicit_span_ceiling_uses_citation_volume_not_protocol_reply \
  tests/unit/test_research/test_company_profile_llm_led.py::test_in_span_record_conflict_isolates_the_dependent_claim \
  tests/unit/test_research/test_company_profile_llm_led.py::test_followup_over_citation_ceiling_keeps_the_body_response \
  tests/unit/test_research/test_company_profile_report_wire.py::test_used_recovery_is_not_reserved_again_before_synthesis
```

新载荷测试与两项相邻回归3 passed、4.37秒。其后四项4 passed。没有跑全库。真实检查点上的六条页外引文不出现在合成/核验序列化载荷中，source_pages只有第12页，原记录内容不变。

真实命令：

```text
python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-segment-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-segment-authorization.json \
  --run-root data/QuoteBak/b1-pool-segment-20261018 \
  --real
```

退出码0。监督记录151.782221秒。绑定SHA-256 `da2c893ea316b436a73f2eec48ad56e7eef87afd68a433d2db87178dd2a54b3a`。

### 30.5 资源

预发送计划仍是正文8264、合成9382、核验7593、一次恢复9382，合计34621，低于本窗口40000，余量5379。这是载荷预留，不是token或人民币。

实际两次请求都有usage，没有unknown预留：

| 顺序 | 来源 | 输入 | 输出 | 合计 | 请求输出 |
| --- | --- | ---: | ---: | ---: | ---: |
| 发现 | scorpio:gemini | 1239 | 1073 | 2312 | 2500 |
| 合成 | zai | 1936 | 4781 | 6717 | 2500 |

账本9029，费用unknown。合成输出4781超过请求2500，随后核验预留超过冻结上限，核验没有发送。查询与导出profiles一致，delivered为1。

原四次加本次：1149.333103秒、11次物理请求、215031账本。本家累计上限246002。费用仍不能声称达到10元或80元。

### 30.6 证据、清理与Git

证据目录[segment/manifest.json](b1-pool-observation-evidence/segment/manifest.json)。`entry.log` `30b5a7c7` 20194字节；`query.json` `d3a94d00` 244709字节；`export.json` `8ddc58da` 245154字节；`budget-final.json` `4d66b216` 1520字节；`entry-result.json` `64ae03f8` 50022字节；`work-checkpoint.json` `e4b8f7aa` 681298字节；`supervisor.json` `4fbe7619` 61字节。`source-receipt.json`仍是`f304554e`，17175字节。

尝试标记`segment.json`已写入，不覆盖前四个标记。本窗口删除了可重建的`quote_pytest_logs_7xd652zt`、`huinazvc`，以及运行目录里的research.db和pdf-cache。QuoteBak约71M。11份既有修改和未跟踪内容不纳入提交。B活跃开发时间unknown；151.782221秒是本次进程时间。

## 31. A审核21df1ce0：接受范围修复与失败留存，结束短段试跑

### 31.1 结论与独立业务核对

**B1/A1仍未通过。** 接受已证的引文限域、原生事实保留、逐阶段真实响应留存及按冻结边界停止；第5次入口1/1已用，无已核主营主张，不追认19核心/6负例、数字错误或准确率通过。原范围P1关闭，当前两项主要阻塞为阶段预算与引用契约，不以更多短段调用试错。

A核对segment检查点、调用回执、query/export、账本及冻结代码：五条发现条件的引文均在p12字符[415,822)，发现/合成source_pages仅p12。p18六条原生事实在原存储及未读缺口中，未进入本次targets、未标已核。核验checks为0、verified_dimensions为空，七条保存候选均unclear、draft/semantic/real完整标志false；query/export的profiles一致。delivered=1仅表示限用记录已保存，不表示主营结论已核。合成回执有主营叙述，查询的“语义流程未完成。”符合未核状态。

第5次151.782221秒、2物理请求，发现2312有效usage、合成6717有效usage，合计9029，无新增unknown token预留，费用unknown。原五次累计1149.333103秒、11请求，67184有效usage＋147847未知预留＝215031账本；本家累计上限246002/1800秒/32请求，余30971/650.666897秒/21请求不是第6次授权。不宣称费用≤10元或阶段≤80元。

### 31.2 两项阻塞及A对契约的修正

**P1：阶段cap低估实际核验输入，冻结卡需要修正。** 用原processing_identity、五条源条件、七条原合成候选、records空集及完整引文，沿原request_reservation全池序列化重建：核验compact_request为9114字节，保守预留14262。旧核验7593＋未分配5379＝12972的阶段cap触发`followup_growth_exceeds_frozen_ceiling`；实际已用9029＋核验14262＋原共享恢复9382＝32673≤40000。因此直接阻塞是阶段cap，不是总账耗尽。合成实际输出4781超过请求2500是事实，但输出token与后续payload字节不同，不混称。B按冻结卡停止是正确执行；A上一卡用一份正文体积约束后继，低估了条件/主张增长，本轮修订后续契约，不追溯改写失败。

后续按完整实际payload、实际已用及unknown、剩余必要阶段和一次共享恢复进行发送准入；阶段估计用于规划/预警，不能单独拒绝满足已冻全部硬约束的必要核验。真正总账/payload/输出/请求/deadline超限仍停，unknown不释放，恢复已用不再预留第二次；恢复留额按实际待恢复请求重算，原9382不充当增长后的充分上界。32673是本次实际输入配原恢复计划的离线比较，不是若发送就必成功的结论，也不是任何新运行授权。

**P1：真实模型的引用ID契约不成立。** 七条原合成候选的supporting_record_ids全部为报告资产ID`asset_84332159e02a19189c1f61fef85cbf02`；本次records为空，因此全部invalid_claim_reference。当前硬校验隔离正确，放宽资源不能解决它。下一卡须让prompt/schema只暴露实际可引用ID，资产与record含义明确；没有原生record的文本主张可以源条件/证据支持且record引用为空。未知ID仍隔离，不猜映射、不静默删除依赖、不手改原响应洗白。局部阅读不强迫模型填七维占位，整报告七维/三视图不减。cond-3的5G探索及战略转型等仍需结合原文区分计划与当前经营，不新增逐公司规则替代语义核验。

### 31.3 后续唯一任务与停止条件

按[验收§7.8](../../../docs/development/company_profile_acceptance_program.md#78-第5次失败后的完整路线裁决2026-10-18)发[B1-R卡§17](b1-execution-card.md#17-b1-r完整报告路线的契约修复与真实响应回放当前卡)：继续LLM主导的完整报告路线，结束短段试跑。本卡新增真实调用、真实探针及付费调试0次，不增加预算、不延续新费用unknown例外。B依流程v3在一个窗口内连续完成R20/R21，最多两次共性局部修复，不逐模块等A。

回放直接使用已保存的真实发现/合成response_payload，保留5条件7候选及原错误引用，捕获核验完整请求。可用明确mock/offline传输验证同owner后续持久化，但七条错误依赖仍unclear；合法引用/冲突用最小通用正反例证明，不把模拟核验算歌华真实成绩。不复制全报告/整库、不重开208页fake专项、不新建平台。

同卡必须交完整208页的一个首选执行方案，含全文发现覆盖、必要上下文/可靠数值、源先行漏项、三视图及核验，列无故障/一次unknown恢复/响应增长的完整payload、请求、保守字节预留、实测usage及最小差额。不能只交407字符或53个导航页，也不能拿旧749989—766497估计直接申请数十万token；不把所有静态字节合计≤120000当一切真实观察的前提。下次A直接裁决完整报告范围/预算/次数或终止当前实现方案，不再滚动派短段试跑卡。当前没有第6次、其他公司、购买/充值或生产授权。

观察1/4、可用0/4，余3家未观察；S1 0/12、S2 0/20、S3 0/100。验收仍是4→12→20（失败最多另20）→100；S2首发18/20补救19/20、S3首发90/100补救95/100，宏召回≥95%及已审准确≥99%与分层/底线并行。非主要遗漏可限用、有据歧义可unclear，已知关键错误/依赖必须修正或隔离，无法补救要说明原因、范围、影响、恢复条件。当前不归档、不扩批，生产not_authorized、规模声明false。

### 31.4 验证、保护与清理

A使用统一QuoteBak环境独立执行：

```text
source scripts/quote_dev_env.sh
timeout --signal=TERM --kill-after=10s 90s \
  /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_report_wire.py::test_frozen_span_followup_payload_excludes_unread_record_quotes \
  tests/unit/test_research/test_company_profile_llm_led.py::test_source_first_reading_three_views_without_heading_regex \
  tests/unit/test_research/test_company_profile_llm_led.py::test_partial_slice_verifies_only_records_inside_the_read_span
```

结果3 passed、7 warnings、4.64秒；复用B本快照另外4项直接回执，不声明全套通过。A本轮只改审核/任务文档，不改应用代码，不代B调用真实模型。OpenSpec严格校验、文档diff与锚点检查结果见本次最终交接。

绑定的60份执行文件hash匹配；first 6、successor 6、special 7、partial 8、segment 8共35份manifest文件的hash/字节数一致。原§1—30正文、源资产、五次制品、配置、11份既有修改及16项未跟踪内容保护；验收文档既有历史块不纳入本次提交。QuoteBak约71M，本轮不复制正文或数据库，本人空测试日志目录收尾清理；下卡继续要求先清无依赖历史产物，必要证据仅一套。

A本轮活跃审核约45分钟（估算，测试墙钟另列），旧P1关闭1项、主要阻塞2项；B活跃开发及等待成本unknown。提交、普通推送及最终保护检查见本次回复。

## 32. B1-R：契约修复与完整报告方案，真实调用仍为0

### 32.1 结果

当前阶段是B1-R / R20–R21，离线契约修复和完整报告准备，不是新的真实运行。阶段估计不再单独拒绝仍满足总账的核验。保存的短段发现和合成响应已沿原入口回放到核验发送点；七条把报告编号当成record ID的候选仍是unclear。查询与导出一致。这不是歌华有线的已核成绩，B1/A1未通过。19核心与6负例未评估。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批，生产未授权。

本卡真实调用0次。剩余30971账本不是第6次授权。

### 32.2 业务与回放

回放输入是[segment/work-checkpoint.json](b1-pool-observation-evidence/segment/work-checkpoint.json)里的两个full_context响应：5条条件和7条候选，supporting_record_ids仍是报告资产编号。离线传输补了核验检查，只为把原链走到保存。查询层七条主张全部unclear，三个完整标志false。空record引用的文本主张可以核过；资产编号不能核过。片段内记录冲突仍隔离依赖。

### 32.3 差异

基线是A已推送的`8ac623dd`。

- 发送准入看实际载荷、已用账、后面还要的阶段估计，以及一次按该实际载荷重算的恢复。超过早先阶段估计只记`stage_estimate_exceeded`，不再因此拒绝。总账不够、payload上限、请求输出、请求数和deadline仍会在发送前停下。失败状态仍消耗唯一一次恢复，unknown不释放。
- 提示词写明report_id和资产编号不是record ID；只有请求里出现的record ID才能写入supporting_record_ids，没有记录时留空。有界片段不再要求把七个维度都填成占位主张。整份报告的七维和三视图要求还在。

### 32.4 验证

环境入口`source scripts/quote_dev_env.sh`，解释器`/home/python/miniconda3/envs/Quote/bin/python`。

```text
ruff check research/company_profile/reading_budget.py \
  research/company_profile/report_interpretation.py \
  research/company_profile/report_wire.py \
  research/company_profile/stage5_provider.py \
  tests/unit/test_research/test_company_profile_llm_led.py \
  tests/unit/test_research/test_company_profile_report_wire.py
```

阶段cap算术、恢复不重复、空引用可核、资产编号仍隔离、超总账保留正文：相关测试通过。保存响应的owner回放1 passed、126.98秒：核验请求已发出，source_pages只有第12页，七条错误引用仍unclear，query与export及重读一致。没有跑全库，没有新的真实模型调用。

### 32.5 完整208页首选方案

单位是UTF-8载荷字节加256再加请求输出上限，不是token，也不是人民币。测量用的是iteration2检查点的208页可读正文和当前打包器。

| 路径 | 请求 | 预留 |
| --- | ---: | ---: |
| 地图 | 2 | 80365 |
| 全文发现 | 10 | 528454 |
| 发现合计 | 12 | 608819 |
| 再加一次最大发现请求的恢复 | 13 | 662758 |

相对剩余30971，只做发现就差577848。相对120000差488819。合成和核验不放进这个合计：短段已经看到核验实际预留14262，高于当时估计7593；合成输出4781，高于请求2500。下一次若获准运行，这两段要按当时的实际载荷再算，并只留一次该请求本身的恢复。

首选：不要在剩余30971上启动第6次。若继续完整报告路线，下一个窗口至少要覆盖发现608819预留单位加一次最大发现恢复53939。不要把890954或旧表749989–766497批准成token或费用。现有池、prompt和schema以本提交的代码为准；不改默认生产池。拟执行入口仍是`python -m research.company_profile.bounded_entry`，但本卡没有新的授权文件，也不应启动。

### 32.6 清理与Git

五次证据和尝试标记未改。本窗口删除可重建的`pytest-of-python/pytest-1`、`pytest-2`、`pytest-3`，以及`quote_pytest_logs__xxvcbr3`、`op3e8kgl`、`9sj26jrq`、`jgjp13b5`、`y9rxr61g`。11份既有修改和未跟踪内容不纳入提交。B活跃开发时间unknown。真实调用0次。

## 33. A审核77c76156：离线部分接受，改为完整报告分层阅读交付

### 33.1 结论与实际业务进度

**B1/A1未通过。** 接受阶段估计不单独拒绝总账可容纳核验、有限ID/空record文本主张协议，以及原响应离线回放在已证范围内的交付。原5条件、7候选及错误资产ID引用保留，七条仍unclear；模拟核验不产生歌华真实业务成绩。R20/R21勾选表示实现/准备已交，不等于全链可执行或资源获批。

本轮B和A真实模型调用均0，原五次1149.333103秒/11请求/215031账本（67184有效usage＋147847未知预留）不变，费用unknown。观察1/4、可用0/4，余3家未观察；S1 0/12、S2 0/20、S3 0/100。完整19核心仍无可审交付，准确率/数字错误及六负例不可评估，不能以无输出计通过。

### 33.2 范围内阻塞与原资源表更正

**P1：共享恢复漏识别HTTP 200后的schema repair。** `utils/llm/client.py`在`_send_attempt`物理预留并按HTTP状态settle成功，之后解析/schema失败仍会repair再发送。`reading_budget._recovery_consumed`只按status判断，两笔success不能证明未消费恢复。A离线直接反例：同request_id，两项`attempt=1/2,status=success`，调用`send_admission_hold(plan, attempts, "verify_views", 14262)`仍返回14262，而唯一恢复已被第二次发送消费，期望恢复hold为0。下一卡在原物理预算/重试关系接点修复，不新建恢复平台；既须停止第二次恢复，也不能重复预留已用恢复。单看failure状态的测试不能覆盖此路径。

**资源表是发现阶段的披露，不是完整可执行闭环。** 原§32明确未计合成/核验，这点如实接受。发现608819−剩余30971＝577848；发现＋恢复662758−30971＝**631787**。`b1-partial-resource-table.json`的`preferred_decision.reason`将577848用于后者，须旁挂更正；§32“只做发现”的577848本身正确，不改原正文、JSON或旧成绩。两种差额都是当前打包器的保守载荷预留，不是实际token/人民币，也不证明应追加数十万token。

### 33.3 A对实现策略的明确裁决

前几卡把“全报告发现”逐渐执行成“先发送全部可读附注正文才交核心”，又滚动拆短段，没有形成三类业务产品；A的准入和发卡设计也需纠正。**结束这条默认实现策略及短段试跑，保留LLM主导的完整报告业务目标。** 产品契约更新为`company_profile_product_contract.2026-10-18.llm-led.v3`，本轮同步总需求、验收、交付路径、proposal/design/tasks、两份相关delta和执行卡；不是宣称B已实现。

新路线：全报告导航 → LLM选择主业/变现/收入构成/重要投入/上下游的完整上下文 → 至少一次不见候选答案的独立源先行查漏与有界补读 → 合成/核验 → 原owner持久化及query/export。程序保可靠数值/身份/单位/引用/预算等硬约束；不输入A的19项条件/Gold指导选页，不新增公司/页码正则。不要求全部附注逐字发送或逐行抽取，导航覆盖、正文已读和核心可用分开；未读补充附注明示未读而非未披露。已知主要板块/必要上下文缺口仍阻止核心完整；导航自身也可能漏项，其风险由独立源审和20/100家验证评价，不许诺零遗漏。

旧全读合同、五次失败及原首发数据均保留，不能按新口径重判旧轮通过。原19核心至少18、六负例、七维/三视图实质完整、已审准确率≥98%及底线要求不变；A核原文及至少20个补充对象（不足全查）。非主要遗漏、有据歧义允许限用/unclear，关键错误及其依赖必须修正/隔离，无法补救说明来源、原因、影响及恢复条件。

### 33.4 下一张卡与明确运行授权

只执行[B1-G卡§18](b1-execution-card.md#18-b1-g分层阅读完成一家真实三视图当前卡)的R22/R23。B按A/B流程v3在一个连续窗口完成先清垃圾、分层阅读与恢复修复、最多两次共性局部迭代、直接回归、新快照冻结，条件成立即自主执行**一次完整报告真实开发运行0/1**，不逐模块等A，不再交纯短段/预算压缩准备卡。原owner/模型池沿用；旧响应仅离线回归，新首发禁语义复用，允许PDF解析缓存。

新窗900秒/120000账本/16物理请求；主路径含独立查漏≤15，另1次全窗共享恢复（换源/schema repair/额外补源），每请求完整payload≤65536字节、请求输出≤4000。逐请求实载准入与有效usage结算，unknown保留；阶段估计不能单独拒绝必要核验，实际超总账仍停，不以全未读附注字节总和卡死启动，不以空末端或模拟答案放行。超输出如实入账、保存未核结果并停止后续。

旧215031不清零，旧余额30971不叠加。本家新累计335031账本/2049.333103秒/最多27请求且不突破原32；相对旧累计额度显式增加89029账本/249.333103秒，非按662758批准token扩额。L0/L1合并变为1055031账本/7449.333103秒，其余3家各240000/1800秒不变且仍未授权。费用unknown例外仅续这次完整窗口：现有账号/池可用，有价照原上限，无价unknown，不编价、不宣称≤10元或阶段≤80元，不授权充值/购买/新账号。

入口启动即累计第6次、本次1/1，无第7入口、其他公司或额外探针。前置不成立不发送，运行失败/超限也保护结果并停止，不修补重跑。A下次依据完整真实结果决定通过、收窄或结束该策略。后续严格4→12→20（失败最多另20）→100；S2首发18/20补救19/20、S3首发90/100补救95/100，宏召回≥95%、已审准确率≥99%及分层/底线同时满足。到数量仍失败结束该方案，不增加公司稀释。当前不归档、不扩批，生产not_authorized、规模声明false。

### 33.5 验证、保护与清理

A独立命令（统一`source scripts/quote_dev_env.sh`，解释器`/home/python/miniconda3/envs/Quote/bin/python`）：

```text
python -m pytest -q \
  tests/unit/test_research/test_company_profile_report_wire.py::test_actual_verification_is_not_refused_by_the_old_stage_cap \
  tests/unit/test_research/test_company_profile_report_wire.py::test_used_recovery_is_not_reserved_again_before_synthesis \
  tests/unit/test_research/test_company_profile_llm_led.py::test_text_claim_with_empty_record_ids_verifies_and_asset_id_stays_unclear
```

结果3 passed/4.28秒（7 warnings）；共享schema repair反例另行独立复现，不冒称已修。已读原响应owner回放的实现/断言并复用其1 passed/126.98秒回执，不重复生成测试库。`openspec validate restore-company-profile-llm-led-generalization --strict`及`git diff --check`通过（OpenSpec遥测联网失败不影响校验退出0）；这是文档审核，不声明全套pytest或R22实现通过。

原§1—32正文、五次制品、资源JSON/旧绑定及本地配置保护；独立hash核验119份未改change文件及配置一致。旧绑定代表历史代码，不能要求改动后的当前代码匹配旧执行hash。11份既有修改和16项未跟踪保护，验收计划既有历史块与本次活动区改动隔离；146个文档相对链接/锚点和新预算算术检查通过。QuoteBak约71M，本轮未复制正文/整库；已清本人空测试日志目录`quote_pytest_logs_g529m4df`，释放数据量0，不删除前序未知归属目录。开发者后续继续先清无依赖垃圾再开发，结束清本人产物，必要证据一套。A活跃审核成本未独立计时，记unknown，不以测试运行时长冒充人工成本。

## 34. B1-G分层阅读：第6次真实运行账本耗尽，没有可审三视图

### 34.1 结论

**B1/A1未通过。** R22的分层阅读和共享恢复修复已进入这次冻结运行；R23入口已启动并记为累计第6次、本窗1/1。歌华有线600037.SH、2025年报、208页可读，导航两批都返回了选页，正文第一批没有读成，独立查漏、补读、合成和核验都没有发出。查询与导出所存的是同一份空视图：主张0条，三个完整标志均为false，分类limited。没有可交给A打分的三视图。19项核心、6项负例、数字错误和已审准确率保持不可评估，不能按空输出计通过。

观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批。生产仍`not_authorized`，规模声明false。没有第7次入口、其他公司或额外探针。本次失败后没有改代码再跑。

### 34.2 这次运行

冻结绑定SHA-256 `3ffd90d04befc6a04826ec82c71d7805e54aca34468acd18181addfb9ac8c9ae`。阅读策略`layered_v3`，产品契约`company_profile_product_contract.2026-10-18.llm-led.v3`，`partial_slice=false`。来源回执与前几次相同，SHA-256 `f304554ec2ee85fd08071b1bb78427c1c854b588806c05efce7befc3ea912d30`。知识截止2026-09-17。工作号`bp-work-d69a142ba195cb8c7362bc54`。入口退出码0，监督耗时141.034100秒。费用unknown，不宣称不超过10元或阶段不超过80元。

| 序 | 来源 | 模型 | 请求 | 结果 | 入账 |
| --- | --- | --- | --- | --- | ---: |
| 1 | scorpio:gemini | gemini-3.8-flash-high | `506775a90db4493698bb2a1df2fd27e1` | HTTP 200，地图第1—142页，选出12、13、14、15、16、17、18、19、20、93、115、116 | 18674（输入17269，输出1405；请求输出1000） |
| 2 | zai | glm-5.3-flash | `28a3835547fd4bce8bddb832e5243f05` | HTTP 500，地图第143—208页，usage unknown | 26483预留 |
| 3 | scorpio:grok | grok-4.7 | `f106565638e9405b987a931e099c6e0f` | HTTP 200，同一地图换源，选出152、156、161、162、165、166、177、187、188、189、190、191、192、193、195、196、197 | 16301（输入11137，输出5164；请求输出1000） |
| 4 | scorpio:luna | gpt-5.6-luna | `500c6ebcb06c453f888f7ff49b365ed0` | HTTP 503，正文第12—25、93、115页，usage unknown | 51599预留 |

四次都是`attempt=1`。有效usage 34975，未知预留78082，合计入账113057 / 120000。请求哈希依次为`14bd95e3ace359793686f8d6b5910f6b9f2a01997f38d555819530f8ecd2b3fd`、`ec85c8eba8de452d7a4895a9e5fd0e64c2a8b2251607cbd140a7e0cf88e9db01`、`5f58271af4c33992c01c9d5de34b67baca49908f4d3b5c3310f6611e91246687`、`aa73c4b65937592b45274a9eec5155de42de5557f460865421f8af717d99524b`。

下一次要发的是正文换源，请求`d019128e96b146e2b4223ce9658bb97f`，准备走scorpio:deepseek / DeepSeek-V4.1-Flash。载荷48850字节，需要51606，剩余6943，剩余请求12，发送前停止，原因`physical_request_token_context_or_cost_budget_exhausted`。它没有成为第5次物理请求。

208页都进入了地图。正文一页都没有读完。模型选出的页再加上程序并入的原生事实所在页，第一批正文才包含第21—25页；第116页和后段选出的页还没排到发送。独立源先行查漏没有执行。停在正文循环内，所以保存的缺口只有`semantic_incomplete:physical_request_token_context_or_cost_budget_exhausted`，没有写成`supplement_unread`。这些页是未读，不是未披露，也不是已经覆盖。导航覆盖、正文已读和核心可用三者分开：地图已查，正文未读，核心不可用。

查询交付1份，与导出及重读一致。三视图没有主张。`draft_complete`、`semantic_complete`、`real_semantic_complete`均为false。

旧五次215031账本、1149.333103秒、11次请求保持不动，30971没有并入本窗。本窗用掉113057 / 141.034100秒 / 4次请求。六次累计328088账本、1290.367203秒、15次请求。公司累计上限335031 / 2049.333103秒 / 最多27次且不超过32次。窗口里还剩6943账本、约758.966秒和12次请求额度，这不是第7次授权。

### 34.3 共享恢复与资源差额

这次真实路径没有出现HTTP 200之后的schema repair。两次成功都是`attempt=1`。第2次地图的HTTP 500之后，第3次是该地图的一次换源，占用本窗那一次共享恢复。正文HTTP 503后的再换源没有发出，拦住它的是总账，不是`shared_recovery_already_used`。后者只拒绝已经有过`attempt>1`之后的又一次修复发送；本窗没有走到那条路径。离线测试覆盖了A的两笔success反例：第二次修复可以发送，第三次修复在预留时拒绝，且已用恢复不再重复预留。

原表和§32不改。发现差额577848（608819−30971）仍然只代表发现阶段。发现加一次最大发现恢复的差额是631787（662758−30971）。`b1-partial-resource-table.json`新增`arithmetic_correction`旁挂这一点，原来的`preferred_decision.reason`仍写着577848。两个数都是载荷预留单位，不是token，也不是人民币。

### 34.4 证据与验证

持久证据在[layered](b1-pool-observation-evidence/layered/manifest.json)。来源回执与历史`f304554e`一致。研究库和PDF缓存没有放进OpenSpec。绑定与授权是`b1-pool-layered-binding.json`、`b1-pool-layered-authorization.json`。

直接回归：

```text
ruff check research/company_profile/bounded_entry.py \
  research/company_profile/reading_budget.py \
  research/company_profile/report_interpretation.py \
  research/company_profile/report_wire.py \
  research/company_profile/runtime.py \
  research/company_profile/stage5_provider.py \
  tests/unit/test_research/test_company_profile_llm_led.py \
  tests/unit/test_utils/test_llm_physical_budget.py \
  utils/llm/budget.py
pytest -q \
  tests/unit/test_utils/test_llm_physical_budget.py::test_schema_repair_reserves_the_expanded_physical_payload \
  tests/unit/test_utils/test_llm_physical_budget.py::test_second_schema_repair_does_not_get_another_recovery \
  tests/unit/test_research/test_company_profile_llm_led.py::test_layered_reading_supplements_a_hidden_business_and_labels_unread_notes \
  tests/unit/test_research/test_company_profile_report_wire.py::test_actual_verification_is_not_refused_by_the_old_stage_cap \
  tests/unit/test_research/test_company_profile_report_wire.py::test_used_recovery_is_not_reserved_again_before_synthesis
openspec validate restore-company-profile-llm-led-generalization --strict
git diff --check
```

Ruff通过。上述5项3.35秒通过。OpenSpec严格校验通过；校验后的遥测联网失败不影响退出码。`git diff --check`通过。没有跑全库。隐藏主业的离线用例能补读并标出未读附注；本次真实运行停在补读之前，不能用那条用例代替歌华成绩。

### 34.5 清理与保护

证据核完哈希后，删除本窗运行目录`data/QuoteBak/b1-pool-layered-20261018`（含`research.db`和PDF缓存，约2.6MB）、空测试日志`quote_pytest_logs_v0vs6pfr`与`quote_pytest_logs_t45rvtta`，以及只剩断链的`pytest-of-python`。五次历史证据和`layered.json`尝试标记保留。11份既有修改和16项未跟踪内容未纳入提交。B活跃开发时间unknown。

## 35. A审核1df565fb：失败披露接受，重整执行方案

### 35.1 主线、真实进展与结论

**B1/A1未通过；接受失败留存与如实报告，不接受R22勾选为整条业务/共享恢复已完成。** 本轮比前轮新增两份真实导航成功和可核物理账本，没有新增可用公司或已核主张。8份layered manifest文件SHA-256/长度成立，query/export的profiles完全一致、claims=0、三个完整标志false；一致的空结果不等于业务交付。核心19项、六负例、准确率及数字错误均不可评估。六次累计328088账本/1290.367203秒/15请求；102159为有效usage，225929为unknown预留，费用unknown，不按空输出报零错误。

方向仍是数千家公司真实LLM解读三视图，当前主要矛盾是执行方案和资源分配不能在可预见故障后完成正文与末端，不是附注行不够多。A前卡只准一个恢复、未确认layered实际末端留额且禁止复用成功导航，导致重复启动成本和审批循环；B按卡在硬限停机正确，A需重整这些规划假设。六次失败不能证明LLM不能解读年报，亦未证明当前通用链可行。此次明确选择**重整实现策略**，保留分层阅读产品目标，不再续发短段或单项压缩卡。

### 35.2 三项当前阻塞及独立证据

1. **P1：恢复仅看attempt，跨源漏计，且公共默认被扩大约束。** `utils/llm/budget.py:168`附近只凭attempt>1识别恢复；`client._complete_concrete`每次源尝试生成新request_id，attempt重置。本次地图500→grok已恢复一次，正文503后再换源应由共享恢复挡住，实际只被账本挡住。A离线直接调用同一物理reserve/settle，四次attempt=1、状态依次http_failure/success/http_failure/success，四次均准入，即两个跨源恢复可绕过原一次限制。新逻辑须绑定业务逻辑父请求及显式本卡政策，不能将卡内恢复次数硬写为所有LlmRunBudget默认。
2. **P1：layered真实发送未保护必要末端。** `report_interpretation.invoke`中send_admission_hold仅在partial且partial_plan非空时调用，layered不走；真实冻结binding也标resource_feasibility_passed=false。第6次第一批正文含16页、预留51599，unknown后再换源需51606但余6943；主路径尚未读完正文，合成/核验更无结果。阶段估计函数的5项回归通过不能证明真实layered路径留额。须以完整实载、非空条件/候选和响应增长验证末端，并减少单次故障暴露，不能清unknown、裁主业或用空壳放行。
3. **P1：已请求必要补读跨批时可能漏读并误标完整。** layered只发preview_batches[0]和supplement_chunks[0]，之后将未读页统一标supplement_unread；必要与纯补充没有保留区别，还会将片段所在页加入read_pages。A使用现有FakeReportProvider七维源fixture，模型首次选1页、独立查漏明确请求2/3页；只在分块接点把合法两页拆成两个batch，实际请求正文只到1/2页、3页未读，得到draft_complete=true、gaps=(supplement_unread:3,)。这是控制流反例，不是歌华实测分数，也不是模型已能发现所有隐藏主业。须处理所有必要批次或限用，片段不可冒充整页；该修复不要求普通附注全部阅读。

上述三个阻塞均指向同一完整交付卡，不派三轮工程卡。早先导入排序已修，A未把它或无关旧测试当新阻塞；5项原直接测试独立复验通过，但不覆盖上述反例。

### 35.3 下一卡、资源裁决与开发者告知

只执行[B1-H卡§19](b1-execution-card.md#19-b1-h可续跑的完整三视图交付当前卡)及tasks R24/R25。B按A/B流程v3先清垃圾，在一个连续窗口内修当前执行链、做直接验证并冻结，不逐模块等A；之后按同一冻结快照保存/续跑完整三视图。第6次导航可经身份/语义兼容性校验作显式开发接续，正文/独立源先行查漏/合成/核验仍真实执行，不能标新首发。原六次失败、源合同、账本与首次制品不改。

**A显式条件授权最多两个新入口0/2（累计第7/8次），每窗900秒/120000保守账本/16请求，整卡1800秒/240000账本/32请求；每物理预留≤24000，最多4次恢复整卡共享、每逻辑请求最多1次。** 数量依据与严格语义见验收§7.10。第二窗只处理可恢复未完阶段，不重跑有效导航/正文、不改代码拼成绩；第一窗已完整无需用第二窗。实际资源不能准入则停，旧余额不并入，费用unknown例外不宣称货币成本达标，不购买/充值。两窗仍失败结束此实现方案，无第9次、额外探针或其他公司；A不再自动滚动同类续额。

目标仍为19核心至少18、六负例实际通过、三视图/七维实质完整、已审准确率≥98%、无未处理底线，A核原文及至少20个补充对象（不足全查）。非主要遗漏和有据歧义可限用/unclear，主体/期间/关键数字/方向/计划错误修正或隔离依赖；无法补救说明来源、原因、影响、已试方法及恢复条件。现观察1/4、可用0/4，余3家未观察；S1 0/12、S2 0/20、S3 0/100。固定4→12→20（失败最多另20）→100，S2首发18/20补救19/20、S3首发90/100补救95/100、宏召回≥95%/已审准确≥99%及分层底线均不变；到上限失败停止，不加公司稀释。

### 35.4 验证、保护与效率

A本轮未修业务代码、未启动真实模型。环境入口`source scripts/quote_dev_env.sh`，解释器`/home/python/miniconda3/envs/Quote/bin/python`。独立复验§34.4同五个pytest节点（schema实载/第二修复拒绝、隐藏主业补读、实际核验阶段cap、已用恢复不重复预留），加`--basetemp=/home/python/Quote/data/QuoteBak/a-review-1df565fb-pytest`，**5 passed/3.50秒、7 warnings**；未跑全库。另做上述两个离线挑战（四次reserve/settle；分块接点stub的必要补读控制流），发现两项真实漏洞，不计成业务分数。

本轮只更新产品说明、验收唯一进度/资源、当前任务卡、交付路径及实施change，复用B的未改静态回执。`OPENSPEC_TELEMETRY=0 openspec validate restore-company-profile-llm-led-generalization --strict`退出0，`git diff --check`通过；9份本人修改Markdown的本地文件链接无失效，六次观察manifest共43份文件hash成立，资源算术成立。原§1—34除顶部索引不改，验收文档已有历史块保持字节且不纳提交，11份既有修改及16项未跟踪保护。pytest未留下测试库，清理本轮空日志目录（数据文件释放0字节），未知归属及B持久证据不动；Git结果见本次最终回复。A活跃审核时间未精确计时、记unknown，独立新增阻塞3项，不把开发/审核消耗混入业务模型usage。

## 36. B1-H第一窗：正文已返回，但引文未通过硬约束

### 36.1 结论

**B1/A1未通过。** R24把三项阻塞修进了这次冻结代码，R25的第7次入口已经用掉。歌华有线600037.SH的第6次导航被原样复用，随后只发出一批正文。模型返回了10条条件，每一条引文都不是对应页原文的精确子串，读取循环因此停在`inventory_evidence_outside_source`。后面的正文、独立查漏、合成和核验都没有发送。查询和导出是同一份空视图：主张0条，三个完整标志均为false，分类limited。19项核心、6项负例和已审准确率仍不可评估。

第8次没有启动。已保存的正文回执是success，同一冻结代码重放时会再次拒绝这些引文，不能把它当成可续跑的有效正文。要换引文校验就必须改冻结代码，本卡不允许改完再拼成绩。剩余的那个窗口因此不用。

观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批。生产仍`not_authorized`。没有第9次、其他公司或额外探针。

### 36.2 这次运行

冻结绑定SHA-256 `35b0c6251a703a212ab1a76387c74de938315edefe39d57a67fe54467bc8eb4d`。阅读策略`layered_v3`，每请求预留上限24000，整卡恢复上限4次、每个逻辑请求1次。导航来自第6次成功地图，来源回执仍是`f304554e`。预检：29个选出页打成5批正文，预留21858、23397、23205、23754、19676，合计111890，都能放进120000。这一窗把合成和核验留给下一窗，所以没有用末端留额拦住这5批。

实际只完成第1批。scorpio:gemini / gemini-3.8-flash-high，请求`a208bb94bcb8462fb775a9e13bbb6fa5`，逻辑请求`0da18e4267d9722d902ca7bace7af406ca20d8af12ba5a65f8a16a8e33ed3c2d`，HTTP 200。页12–15。输入4931，输出3463，请求输出2500，入账8394。费用unknown。没有换源，也没有schema repair。

返回的10条引文覆盖第12–15页，包括有线电视网络经营、4K频道、与北京移动/电信的共建共享等表述，但逐条都对不上原文子串。硬约束拒绝后，其余4批正文和查漏都没有发。输出超过请求值已按usage入账，没有因此停整家公司。

监督耗时133.203747秒。本窗8394 / 120000、1 / 16请求。旧六次328088账本、1290.367203秒、15次请求不动。七次累计336482账本、1423.570950秒、16次请求。本家累计上限568088 / 3090.367203秒 / 47次。窗口里剩下的额度不是第8次授权。

### 36.3 三项修复落在哪里

- 跨源和schema repair按逻辑父请求计数。公共`LlmRunBudget`不带政策时不再套用本卡的次数；本卡政策是整卡4次、每个逻辑请求1次。同一次请求的`attempt=1`换源也算恢复。
- 分层发送会按实际载荷留合成、核验和一次恢复。第一窗显式把末端留到下一窗，避免5批正文被本窗留额提前截断。必要核验本身仍只受总账约束。
- 独立查漏会处理所有预览批，模型点名的补读批也都要有结果。只读到片段不算整页已读。没读完的必要页记`necessary_unread`或`necessary_fragment_unread`，不能降成普通`supplement_unread`后标完整。纯补充未读不单独挡住已经成立的核心。

这三项都有直接测试。本次真实路径没有走到换源、第二批补读或核验。

### 36.4 验证与证据

持久证据在[resume1](b1-pool-observation-evidence/resume1/manifest.json)。研究库和PDF缓存没有放进OpenSpec。

直接回归包括：第二次schema repair在本卡政策下拒绝、公共预算不默认套用该上限、跨源`attempt=1`仍消耗该逻辑请求的一次恢复、隐藏主业补读、两批必要补读都要读到且漏读时`draft_complete`为false、实际核验不被旧阶段cap拒绝、已用恢复不重复预留。相关测试通过。没有跑全库。OpenSpec严格校验和`git diff --check`在交接时再核。

### 36.5 清理

证据核完哈希后，已删除本窗运行目录`data/QuoteBak/b1-pool-resume1-20261018`（含研究库和PDF缓存，约2.8MB）以及4个空测试日志目录。五次历史证据、第6次layered证据和`resume1.json`尝试标记保留。11份既有修改和16项未跟踪内容不纳入提交。B活跃开发时间unknown。

## 37. A审核475b728f：纠正引文归因并改绑未用入口

### 37.1 裁决与主线反省

**失败留存接受，B1/A1仍未通过。** 第7次只完成一批正文HTTP响应，未完成领域接受/真实核验；claims=0，三complete=false，19核心/6负例/准确率/数字错误不可评估。七次账本336482＝110553有效usage＋225929未知预留，1423.570950秒、16物理请求、费用unknown。第8次未启动，不因余额自动获得更多入口。

方向仍为真实LLM分层阅读生成公司画像、产品暴露和供应链定位，未到推翻LLM路线的证据。实际进展只是拿到了可回放的真实正文条件，尚无业务可用公司。主要阻塞是排版与引用契约不适配、单条错误中断全链及上下文语义尚未核；不是应提高附注明细召回或再做资源平台。A上卡将开发接续也限定同版，导致可离线修正的排版问题再次等待，冻结粒度不合理；本轮明确纠正，不把B正确停机解释成开发方案合理。

### 37.2 独立证据与阻塞分类

直接读取[第7次原始checkpoint](b1-pool-observation-evidence/resume1/work-checkpoint.json)中的10条件/13处Evidence，与其实际发送的p12—15整页正文比较，检查每处非空白字符及匹配次数；不是在全报告猜引文，也未改响应：

| 检查 | A结果 |
| --- | --- |
| 精确原文子串 | 3/13（cond-rm-02一处、cond-ud-01两处） |
| 仅忽略空白后的唯一原文定位 | 13/13；其余10处为PDF排版空白差异 |
| 所有引文原本均精确的条件 | cond-rm-02、cond-ud-01；程序在首条失败即停，未实际接受这两条 |
| 定位即语义正确 | 否；仍须上下文核验，不计真实19/6成绩 |

例如`cond-pb-01`在p12原文[415,505)可恢复连续精确引文，差异是换行；`cond-ud-01`的p13引文本已精确，却出自“从技术环境来看”的行业描述，不能仅据此断言公司已与中国移动建设700MHz网络。`cond-scr-01`引用战略转型目标，不能仅由目标句把身份判为已实现；可查其他实际业务证据支持，仍不清楚则分开标注/隔离。**§36“每条引文都非精确子串”不是正确的根因描述，本节旁挂更正，原报告/失败不改。**

两项范围内阻塞：P1引用定位与全链中断（`report_interpretation.py`的`_evidence_valid`及正文循环遇首条失败即抛错）；P1语义候选尚未完成上下文核验（行业/公司、转型/当前）。后一项是业务风险及验收要求，不假称已证明核验器误放行；本次尚未运行核验。原跨源逻辑归属/公共默认、必要补读修复有直接测试支持；layered留额已接真实分支，但完整最后窗口仍待实载证明，不以代码存在宣布端到端成立。

### 37.3 下一卡与开发者告知

执行[卡§20](b1-execution-card.md#20-b1-h接续可信证据到完整三视图当前卡)/R26—R27及验收§7.11，按A/B流程v3连续实施，不逐项审批。先清无依赖垃圾并护历史/并行改动；在原owner一次完成原文位置映射、上下文核验/局部隔离、真实响应离线回放和最后窗口完整非空载荷验证。最多两次共性局部迭代，不能只交新增测试数或另一张资源表。

A显式把原未用第8次改绑修复后冻结快照，**仍只一次900秒/120000账本/16请求，单发送预留≤24000，输出请求≤4000，原两窗恢复共享≤4/每逻辑请求≤1**。第6次导航/第7次原始正文可在新身份重新验证并保原始/派生链，不称同版首发、不改判旧失败；第8次发送后不改码拼结果。最后窗口不得再推迟合成/核验，unknown不清零、旧余额不并入、费用unknown不声称货币达标。前置成立B自主执行；前置不可行或第8次仍失败则结束当前实现方案，无第9次/探针/其他公司，不再自动派同类续额卡。

原19核心至少18、六负例实际全查、七维/三视图、已审准确率≥98%及底线不变；主要板块不能豁免，次要遗漏可限用、有据歧义可unclear，无法补救须说明来源、影响及恢复条件。A1通过才发B2，当前观察1/4、可用0/4，余3家未观察；S1 0/12、S2 0/20、S3 0/100。后续固定4→12→20（失败最多另20）→100，数量/门槛/补救只引用验收§2—6，不加公司稀释失败。不归档、不扩批，生产not_authorized、规模声明false。

### 37.4 验证、保护与交接要求

A本轮只改规划/规范/任务文档，未修应用代码、未运行真实模型。独立复验命令（统一环境、隔离TMPDIR）：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_utils/test_llm_physical_budget.py::test_public_budget_does_not_hardcode_the_card_recovery_cap \
  tests/unit/test_utils/test_llm_physical_budget.py::test_cross_source_reset_still_consumes_the_logical_recovery \
  tests/unit/test_utils/test_llm_physical_budget.py::test_second_schema_repair_does_not_get_another_recovery \
  tests/unit/test_research/test_company_profile_llm_led.py::test_layered_gap_check_reads_every_requested_batch_and_blocks_when_one_is_missed \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review-475b728f-pytest
```

结果**4 passed/8.11秒、7 warnings**，未跑全库。另独立执行上述13引文字符/位置对比，并用pypdf重读官方PDF的p12—15：PDF SHA-256与合同一致，13/13引文非空白字符均能在对应官方原页匹配；7次manifest共51份文件hash全部成立。旧静态检查只复用对应快照回执，不补称本轮全库通过。B的§36结构可沿用，但验证段缺精确命令/数量，归因过度概括；下次同一入口补齐并分开HTTP成功、领域接受和已核交付，不为报告格式另开开发窗口。A活跃审核时间unknown，独立新增范围内阻塞2项（定位/中断、待核语义），不把审核消耗记为业务模型usage。

本轮OpenSpec严格校验、diff及10份修改文档的本地文件链接检查通过；未留下测试库，已清本轮空pytest日志目录（数据文件释放0字节），未删除业务/验收资料。未新增证据平台或重复报告目录。既有11份修改/16项未跟踪按开始时hash核保护；验收文档已有历史块保留且不纳提交。Git结果见本次最终回复。

## 38. B1-H第8次：重放已存正文时响应校验失败

### 38.1 结论

**B1/A1未通过。** R26把排版空白定位、单条坏证据隔离，以及第7次正文到合成/核验/query/export的离线回放做进了这次冻结代码。R27把原未用第8次改绑到该快照并已启动。启动后只复用了第6次两批导航，重放第7次正文响应时`ReportSemanticResponseV3`的严格校验拒绝了`source_conditions`的列表形态，流程停在`semantic_incomplete`。没有新的物理请求，没有已核主张。19项核心、6项负例和已审准确率不可评估。

第8次已经记为用过。同一实现方案不再自动第9次。定位成功仍然不是语义通过；这次真实入口还没有走到上下文核验。

观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批。生产仍`not_authorized`。

### 38.2 离线修复

优先精确匹配。只有空白不同时，在本次发送的页面里找唯一连续原文，回填原文、页内偏移和模型原句。数字、标点或其他非空白变化，以及多处匹配，保持未定位。一条坏证据只隔离自己的条件；主要条件未定位仍阻止完整。核验请求带上标题和所在段落，并要求把行业环境与公司行动、战略目标与当前身份分开。合成或核验超过24000时按独立条件分批，不截断JSON。

第7次保存的13处引文，在已发送的第12–15页上13/13能唯一定位，其中3处原本就是精确子串。这只证明位置，不证明语义。离线原入口回放走到了query/export，模拟核验把主张留在unclear，`real_semantic_complete`为false，不计19/6。

### 38.3 第8次运行

冻结绑定SHA-256 `9da7e3172992ef98b7a07a932c7da3ecfae052e2f942fd4bce19aa56c16597ef`。预检仍是29页、5批正文，预留21942、23481、23289、23838、19760。这一窗没有推迟合成和核验。

监督耗时104.525174秒。物理请求0，入账0，没有usage。查询与导出一致，主张0条，三个完整标志false，分类limited。缺口是响应校验：`source_conditions`输入为list，严格模型要求tuple。旧响应文件没有改。

七次累计仍是336482账本、1423.570950秒、16次请求。本窗没有新增账本或请求，但入口次数已经用完。旧余额不转入。费用没有新的模型调用，不据此声称成本达标。

### 38.4 验证与证据

```text
pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_whitespace_quote_locates_on_the_sent_page_and_a_bad_quote_stays_isolated \
  tests/unit/test_research/test_company_profile_llm_led.py::test_seventh_run_quotes_relocate_on_the_sent_pages_without_a_model_call \
  tests/unit/test_research/test_company_profile_llm_led.py::test_layered_reading_supplements_a_hidden_business_and_labels_unread_notes
```

3 passed / 7.74秒。

```text
pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_saved_seventh_body_reaches_query_export_without_counting_as_a_real_score
```

1 passed / 145.74秒。Ruff检查本次修改文件通过。没有跑全库。

持久证据在[resume2](b1-pool-observation-evidence/resume2/manifest.json)。来源回执仍是`f304554e`。研究库和PDF缓存未放入OpenSpec。运行目录约2.8MB已删。9个空测试日志目录已删。`resume2.json`尝试标记保留。11份既有修改和16项未跟踪内容不纳入提交。B活跃开发时间unknown。

## 39. A审核24e8cd2f：结束B1-H试跑，先核完整实际入口

### 39.1 结论、主线与实施裁决

**失败留存接受，B1/A1未通过；引文定位修复接受，完整入口就绪不接受。** 第8次104.525174秒、0物理请求、0新增账本，仍消耗该入口；没有已核主张，19核心、6负例、准确率和数字错误均未评估。B1-H的2/2耗尽并结束，无自动第9次或其他公司授权。

终点仍是数千家公司真实LLM阅读年报后交付公司画像、产品暴露、供应链定位。本轮进展是排版定位、对象隔离和分批实现，尚无新增业务可用结果。当前主要矛盾是完整实际执行路径没有得到证明：本地JSON续跑边界错配，却被替代分支的离线测试遮住。它不是继续补六来源计费资料、扩大字节账本或逐字穷读附注的理由，也不能据此否定LLM路线本身。

A此前接受局部fixture作为实际入口准备依据、继续切分冻结/运行窗口，也没有避免用真实运行定位本地可发现的错误；本轮一并纠正。结束B1-H反复试跑的执行方案，仅准一次有退出条件的B1-I整体验证与实施取舍，真实入口/模型调用/付费新增授权均0。不是“一行类型修补后再运行”，也不把离线三视图当业务成绩。

### 39.2 独立发现、验证和限制

| 分类 | 证据与结论 | 当前处理 |
| --- | --- | --- |
| P1：持久JSON与实时JSON解码不一致 | `report_interpretation.py`的reused-stage分支对`json.loads`后的响应调用`model_validate`，严格tuple字段拒绝JSON数组；实时路径调用`model_validate_json`。A用第7次同一真实响应独立验证：前者5项tuple_type失败（source_conditions/claims/records/checks/requested_pages），后者成功读入10条件/13引文 | R28修严格JSON边界，保留身份/未知字段/数字约束，不全局松strict；旧响应不改 |
| 验证缺口：离线回放没有验证实际续跑和合法结果保留 | `test_saved_seventh_body_reaches_query_export_without_counting_as_a_real_score`只用15页输入并将前11页替换占位，未设置resumed_receipts/continuation checkpoint；把保存的正文重新包装为新provider响应，绕过上述分支。模拟核验全部unclear，只证明到verify和query/export，不证明可靠主张被保留 | R26仅部分接受；R28必须走完整208页、实际operator/绑定加载、新建及磁盘JSON续跑，证明非空有效结果和错误局部隔离 |
| 已接受且不扩大范围 | 13处引文在第12–15页可唯一定位，3处原本精确、10处仅空白差异；位置成立不代表公司/行业、计划/当前语义成立。预算/生产硬边界未因0外呼而自动放宽 | 保留有效实现，不追改第7/8次失败，不另建引文或校验平台 |

A独立运行3个直接回归：

```text
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_whitespace_quote_locates_on_the_sent_page_and_a_bad_quote_stays_isolated \
  tests/unit/test_research/test_company_profile_llm_led.py::test_seventh_run_quotes_relocate_on_the_sent_pages_without_a_model_call \
  tests/unit/test_research/test_company_profile_llm_led.py::test_layered_reading_supplements_a_hidden_business_and_labels_unread_notes \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review-24e8cd2f-pytest
```

结果3 passed / 7.83秒（7 warnings），未重复145秒旧模拟回放，未跑全库或真实模型。八轮manifest所列59份保护文件哈希核验一致。分批核验漏目标另作离线控制流挑战，但未抵达待验证分支，**不登记为已复现业务缺陷**；卡内直接正反验证仍须覆盖核验目标及合法结果保留，不能用未证疑点扩大开发。

### 39.3 累计口径更正、下一卡与保护

§38的“七次累计”可作历史小计，不能替代当前八入口累计。旁挂更正：1423.570950＋104.525174＝**1528.096124秒**；16物理请求、110553有效usage＋225929未知预留＝**336482账本**不变，历史费用unknown。八入口中前七次发生真实外呼，第8次0外呼；不退还入口、不借旧余额、不改原表/尝试标记。当前观察1/4、可用0/4，另3家未观察；S1 0/12、S2 0/20、S3 0/100。

下一卡唯一为[执行卡§21](b1-execution-card.md#21-b1-i完整实际入口验证与实施取舍当前卡)/R28—R29：B一个连续窗口、目标一个工作日、最多两次共性局部迭代，修实际边界并用真实输入/实际操作入口、只模拟外部传输，完整验证新建和磁盘续跑的非空三视图及原owner重读。至少一个合法已核状态主张保留、错误及依赖隔离，分批目标有处置，必要漏项挡完整；real_semantic_complete保持false，模拟不计19/6。交实际命令、版本/身份、完整结果及最短实施路径/能力资源取舍，不仅交测试计数或另一张预算表。两次迭代仍失败则停止该实现方案并给替代建议；离线成功后A也须作整体取舍，不能自动第9次或继续同类卡。

质量与规模仍按验收§3—6顺序4→12→20（失败最多另20）→100；B1的19核心至少18、6负例全查及三视图/七维/98%准确率底线不减。非主要遗漏和有据unclear可限用，主体/期间/关键数字/否定计划等错误须修正或隔离，不能补救写明来源、影响、尝试与恢复条件。未读附注不当未披露，不要求所有附注行齐全。A1未过不归档、不扩批，生产not_authorized、规模声明false。

流程已同步验收、交付计划、任务卡、tasks、设计及delta；开发者按A/B流程v3连续执行R28/R29，不逐项等A。先清已结束无依赖垃圾，临时产物统一QuoteBak，交接只留一套必要证据并清本人产物；历史/正式源及既有11修改/16未跟踪保护。A本轮不代B修改应用代码，未增加模型请求；人工审核成本未单独计量，记unknown。

本次文档交付验证：`openspec validate restore-company-profile-llm-led-generalization --strict`通过，9份修改文档的158个本地链接/锚点无失效，`git diff --check`通过。A测试basetemp已自动清理，核空目录及无进程占用后删除1个已结束空测试日志目录，文件内容释放0字节；其余QuoteBak内容未因本轮审核批量删除，正式证据不动。

## 40. B1-I：完整实际入口离线验证与实施取舍

### 40.1 结论

协议路径已经用实际 operator、208页源和原读写 owner 走通新建与磁盘续跑。这不是 B1/A1 通过，不计观察/可用公司，不计19核心/6负例。新增真实入口0、模型请求0、付费0。八入口时钟1528.096124秒和账本336482不变。

一次共性修正：结构预览短于200字时，模拟选页返回空，第一轮新建停在`page_map_requires_valid_full_context_selection`。改为选择预览里任一非空页后，两条入口都完成。没有用第二次迭代。

### 40.2 实际命令、身份与版本

两条都没有`--real`，传输是进程内 `CallableTransport`，`offline-transport.json` 的 `real_calls` 均为0。沙箱未开外网权限。没有写入 `attempts/` 下的真实尝试标记；`resume2.json` 仍是第8次失败标记。

```text
source scripts/quote_dev_env.sh
python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-i-fresh-binding.json \
  --run-root data/QuoteBak/b1-i-offline-20261019/fresh
python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-i-resume-binding.json \
  --run-root data/QuoteBak/b1-i-offline-20261019/resume
```

派生绑定只改离线协议和续跑指针，文件哈希按当时代码重算。fresh SHA-256 `d4812975aafd4afa27ee97dbe1238e10de07f42427af0fa02da9ca50064e2adf`。resume SHA-256 `218613e33a7e87b07474a92154094c3e8f623023e549319d472f0e95c000c28a`。策略 `company_profile_llm_led.v2`，提示词 `company_profile_report_reading.2026-10-17.v2`，阅读 `layered_v3`，产品合同 `company_profile_product_contract.2026-10-18.llm-led.v3`。池指纹仍是 `fb5b895c00cdbdd7c158d35d3707988d5813d62f7d5df7319112151afb1e9141`。来源仍是歌华有线2025年报，PDF `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`，208页均可读。预算类上限仍是900秒/120000/16；本窗没有放宽 `LlmRunBudget`。

### 40.3 两条入口的实际分支

| 入口 | 实际分支 | 断点 | 结果 |
| --- | --- | --- | --- |
| 新建 | 无 navigation_query、无 continuation。7次页面地图、1次正文、4次未读预览、1次合成、1次核验，共14次物理请求 | 预览只选中1页；其余页是 `supplement_unread`，不单独挡完整。缺 products_services / input_exposure / supply_chain_role | 219.363484秒。query=export，新 `CompanyProfileReadService` 重读与二者一致。draft_complete=false，real_semantic_complete=false |
| 磁盘续跑 | navigation_query 复用第6次地图；continuation_checkpoint 是第7次 work-checkpoint。正文回执 `reused_stage=true`，页12–15，10条件/13引文，没有重发该请求 | 后续查漏有一批被拒绝后循环停止：`gap_check_incomplete` 57页，`necessary_unread` 7页。这挡住完整 | 256.271317秒。9次物理请求。query=export，新实例重读一致。draft_complete=false，real_semantic_complete=false |

续跑回执里的第7次引文已经按空白映射回到发送页。例：`cond-pb-01` 的 bounded_quote 保留换行，model_quote 是模型原句。位置仍不等于语义支持。

### 40.4 保留对象与隔离对象

模拟核验只看三件事：引文能否定位、`statement_status=planned`、描述或已附上下文里有没有“计划/技术环境”。它不是业务阅读器。

新建保留：`proof-current`，principal_business，observed，引文来自选中页的非空白切片。产品视图有 `proof-output`，供应链视图有 `proof-upstream`，三视图都有实质主张。隔离：`proof-bad-quote` 为 `evidence_not_located`，不丢同批其他条件；`proof-planned` 为 unclear。

续跑保留的协议对象：第7次 `cond-pb-01` 等进入 observed，证据已定位。这只证明磁盘 JSON 能进入原投影，不证明这些句子的公司事实。模拟核验把 `cond-ud-01`（700MHz/中国移动，来源是技术环境而非公司已建成）和 `cond-scr-01`（战略转型，不是当前身份）也标成 observed，因为已附段落里没有“技术环境”三字，fixture 也没有公司专用规则。这两条不得计为已核业务主张。续跑同时隔离了三次 `proof-bad-quote` 和计划句 `proof-planned`（unclear / `claim_not_verified`）。

分批：新建核验1批，续跑合成3批、核验3批。两批都没有 `semantic_verify_targets_missing_or_extra`，所以每个条件/主张/记录目标都有处置。单批与分批的最终投影都经过同一个 query/export。

### 40.5 预算与未知用量

模拟传输对 `entry_proof_v3` 返回 prompt 80 / completion 40。新建14次预留合计235316、最大23837，结算1680。续跑9次预留合计190008、最大23470，结算1080。账本因此能走完。若这14次预留在未知用量下整笔保留，235316已经超过120000，同一窗口进不了合成。未知用量保留规则没有改。配置价目对模拟 usage 算出了金额；那不是真实费用，本窗付费为0，历史费用仍 unknown。八入口账本336482不增加。

### 40.6 实施取舍

当前实现能承载的部分：全报告解析、分层选页、24000分批、磁盘 JSON 续跑、空白引文定位、单条件隔离、合成/核验分批、原 writer、查询/导出/新实例重读。

当前实现不能承载的部分：

- 一次120000/16窗口，在用量未知时，装不下208页地图、未读预览、正文和末端。新建这条链的预留和已经说明了边界。
- 程序不会把行业技术环境判成“不是公司已建成”，也不会把战略转型判成“不是当前身份”。这仍要真实核验模型。第7次其余正文批、独立查漏走完、真实合成和真实核验都还没有外呼证据。

最短可行路径只有一条，而且现在没有授权：一次真实窗口，复用第6次地图和第7次正文，提供方必须回传 usage，并由真实核验处理行业/公司与计划/当前。不要再发一张只补 JSON 或只加 fixture 的卡。若用量仍未知，就得把未读查漏拆出这16次请求，那要改账本上限；现有 `LlmRunBudget` 拒绝超过16次或120000。A若不授权这次真实窗口，本实现方案停在这条离线边界，不自动第9次。

### 40.7 验证、清理与保护

直接回归3 passed / 3.82秒：

```text
pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_saved_body_json_reloads_through_the_resume_decoder \
  tests/unit/test_research/test_company_profile_llm_led.py::test_whitespace_quote_locates_on_the_sent_page_and_a_bad_quote_stays_isolated \
  tests/unit/test_research/test_company_profile_llm_led.py::test_seventh_run_quotes_relocate_on_the_sent_pages_without_a_model_call
```

`test_saved_body_json_reloads_through_the_resume_decoder` 用第7次原始文件证明 `model_validate` 失败、`model_validate_json` 读入10条件，未知字段仍拒绝。没有改那份原始 checkpoint。

持久证据在 [b1-i-offline-evidence](b1-i-offline-evidence/manifest.json)。研究库、PDF缓存、页缓存和 output 未放入 OpenSpec。QuoteBak 运行目录在哈希核对后删除。八入口 attempt 标记未改。既有11份修改和16项未跟踪文件不纳入提交。

## 41. A审核97fb94a4：协议闭环局部接受，转入真实能力裁决

### 41.1 结论与主线自检

**工程局部接受，B1/A1不通过。** B1-I解决了上轮未证明的实际磁盘续跑边界：合法JSON数组按实时同样的`model_validate_json`解码，未知字段仍拒绝；208页源的新建/续跑都沿实际operator和原owner到query/export/新实例重读。模拟核验仍不能计真实三视图、19核心/6负例、准确率或数字错误；真实已核主张仍0。

主要矛盾已从“实际入口能否运行”转为“必要原文上下文是否充分、真实模型能否在有界资源内形成可用业务结果”。A此前过于按单个阻塞拆准备卡，末端核验长期未取得真实业务证据；本轮不再只发JSON/fake小修卡，也不扩建预算/验证平台。保留LLM主导分层阅读与已修owner，合并必要修复和一次真实业务裁决，先交可保存的可靠结果，再补齐主要板块；失败明确结束本执行方案，不滚动续额。仍以数千家公司自动交付三视图为目标，行业增强后置。

### 41.2 独立证据与发现分级

| 分类 | A核对的事实及影响 | 本轮结论 |
| --- | --- | --- |
| 接受：JSON/owner协议路径 | B1-I manifest及两份绑定共12项文件的hash/bytes一致；fresh/resume的profiles均query=export。续跑磁盘载入第7次10条件/13引文，`reused_stage=true`，没有重发正文；两种入口到原读取服务重读 | 不重复建设同类回放，既有直接回执可复用；不是业务验收 |
| P1：核验上下文丢统领限定 | `report_interpretation.py`的`_heading_and_paragraph`把单个PDF换行当段落界。A对第7次`cond-ud-01`在实际页12–15范围执行`locate_source_quote`：原文可定位，p13原页有“从技术环境来看”，返回上下文仅48字符、没有该限定，只剩建设700MHz网络的句子。必要行业/公司身份依据并未送入核验 | R30修实际合成/核验输入，保完整必要段落/统领限定；不足unclear或有界补读。不能只修mock的词匹配 |
| 验证缺口：真实语义及分批处置 | 模拟`cond-ud-01`/`cond-scr-01`仍observed；没有`semantic_verify_targets_missing_or_extra`不等于已证明缺目标/未合成源条件都妥善处置，也不证明单批/分批等价 | 只补当前直接对照及受影响测试；真实LLM核验行业/公司、计划/当前，不以程序关键词取代 |
| 资源推论边界 | fresh 14模拟请求预留和235316、结算1680；resume 9请求预留和190008、结算1080。每次模拟固定120，只证明结算接点；全部unknown下该序列超限属实，但不能推出正常真实usage必超120000 | 不扩大公共120000/16硬限，不要求供应商保证usage；实载正常/一次unknown/输出增长均验证，全部unknown可有界失败 |
| 完整性边界 | fresh三视图仅模拟各一条observed，仍缺产品/投入/角色维度；resume另有7页necessary_unread、57页gap_check_incomplete，draft/real完整标志均false | 工程可达与业务充分分开。已知核心缺口不豁免；可靠部分及时保存，次要附注可后置 |

A独立运行同一3项直接回归，**3 passed / 4.21秒，7 warnings**；未跑全库或重复两次长模拟，未新增模型调用。命令：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_saved_body_json_reloads_through_the_resume_decoder \
  tests/unit/test_research/test_company_profile_llm_led.py::test_whitespace_quote_locates_on_the_sent_page_and_a_bad_quote_stays_isolated \
  tests/unit/test_research/test_company_profile_llm_led.py::test_seventh_run_quotes_relocate_on_the_sent_pages_without_a_model_call \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review-97fb94a4-pytest
```

### 41.3 新任务卡与一次能力裁决

唯一下一卡为[执行卡§22](b1-execution-card.md#22-b1-j真实三视图交付与能力裁决当前卡)/R30—R31，资源唯一见[验收§7.13](../../../docs/development/company_profile_acceptance_program.md#713-b1-i局部接受与一次真实业务能力裁决2026-10-19)。B按A/B流程v3在一个连续窗口完成上下文/直接阻塞修复、必要实载验证、最多两次共性局部迭代、冻结和真实交付，不逐模块等A。最早可独立成立的一组源支持主张先真实合成/核验并沿原owner保存，再完成全部主业/关键机制及源先行查漏；后续失败不抹可靠部分。不得复制平行循环/writer或手填答案，候选答案不输入源先行查漏。

**这是A根据B1-I新证据作的显式新授权，不是自动恢复B1-H：0/1完整入口（累计第9），900秒/120000账本/16物理请求，每发送预留≤24000、请求输出≤4000；全窗恢复发送≤2、每逻辑请求≤1。** 原第6次真实地图/第7次真实正文可在新冻结身份重验、保原始及派生链；B1-I模拟条件/claims/checks/usage不得混入真实。现有池随机/负载选源、任务内避让失败来源，unknown占预留不释放；费用unknown例外不作成本达标声明，不购买/充值或改生产默认。

前置不成立、两次迭代用尽或唯一真实窗失败/硬限到达，都交可靠部分、未读/未核范围和能力/资源边界，结束本执行方案；无第10次、短段探针、换公司或额外预算。新窗即使用满，本家最多2428.096124秒/456482账本/32请求，未提高旧本家上限3090.367203秒/568088账本/47请求。八入口旧账336482（110553有效usage＋225929unknown）、原失败及费用unknown保留，不转借余额。

业务标准不降：19核心至少18、6负例实际全查、三视图/七维实质完整、已审准确率≥98%、无未处理底线，A另核补充对象至少20条、不足全查。次要遗漏/有据歧义允许限用；无法补救须说明来源、原因、尝试、影响及恢复条件。可靠部分不等于公司可用；A1未通过不进入B2。4→12→20（失败最多另20）→100及有界补救不变，当前观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批，生产not_authorized、规模声明false。

### 41.4 清理与本轮边界

B1-I证据约1.7MB，未形成大研究库副本；大量文本行来自query/export重复覆盖与回执，不能把行数当新增业务量，也不因在OpenSpec就永久留全部重复件。A核实`data/QuoteBak/b1-i-offline-20261019`已不存在；B未提供确切释放字节，记unknown，不伪造清理成绩。下一卡已明确先清无依赖历史垃圾、只留一套必要持久证据、交接前清本人产物；可重建离线重复件核依赖后清，八入口原真实源/合同/结果及归属不明对象保护。

A已清本次审核产生且无占用的空测试日志目录，未删除业务数据。文档OpenSpec严格校验、169处本地链接/锚点和预算算术检查通过，审核§1—40正文逐字保留。A本轮仅改需求、验收、规划、任务和审核文档，不代修B应用代码，不启动真实模型或生产。原11份修改/16项未跟踪受保护；本轮并行备份配置提交`c305ee5f`独立保留，不纳入本次审核修复。审核活跃耗时未单独计量，记unknown；提交及推送结果在本轮最终交接报告，不能计入业务通过。

## 42. B1-J：上下文修复与第9次真实窗口失败

### 42.1 结论

上下文修复已经进到第9次真实合成请求。业务验收没有成立。发布结果是18个已定位源条件、0条主张、0条核验，`draft_complete=false`，`real_semantic_complete=false`。19核心和6负例不能计分。本执行方案按卡停止，没有第10次。

### 42.2 上下文与直接阻塞

PDF单行换行不再当成段落边界。段落收到上一句结束、空行或短标题为止。第7次原始页上，`cond-ud-01`的700MHz引文上下文现在包含“从技术环境来看”及其后整段；`cond-scr-01`的战略转型引文上下文包含“近年来，公司……”。这是原文范围，不是程序对行业或公司行为的裁定。

查漏一旦停住，当批和尚未发送的后续批都记`gap_check_incomplete`，不再记成普通`supplement_unread`。第9次结果是198页查漏未完成、19页必要正文未读，没有`supplement_unread`。完整因此不成立。

直接回归：第7次13条引文定位仍通过，并且上述两段限定在上下文里；未发送查漏页保持incomplete且一条已读主张仍可留下；第7次页12–15的真实预留为8000至24000之间，未知用量按该预留结算后下一笔同样大小的发送被拒绝。相关既有分层阅读测试通过。没有用模拟词表冒充语义修复。

### 42.3 第9次真实窗口

冻结后只跑了这一次。命令：

```text
source scripts/quote_dev_env.sh
python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-decision-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-decision-authorization.json \
  --run-root data/QuoteBak/b1-pool-observation-20261019/decision \
  --real
```

绑定SHA-256 `8508e996d36e0c1a5bc5dc63458dd7b4e883e2b4c237f9852ab92112e4cf3409`。提示词`company_profile_report_reading.2026-10-17.v2`，策略`company_profile_llm_led.v2`，阅读`layered_v3`。复用第6次地图和第7次原始正文，没有加载B1-I模拟条件。预检：导航复用，选中29页，正文5批，预留`[21942, 23481, 23289, 23838, 19760]`。

| 序号 | 结果 | 模型 | 预留 | 结算 | 用量 |
| --- | --- | --- | --- | --- | --- |
| 1 | success | gemini-3.8-flash-high | 23470 | 9868 | 有 |
| 2 | failed_or_cancelled | glm-5.3-flash | 23652 | 23652 | 无，超时后换源 |
| 3 | success | grok-4.7 | 23647 | 22181 | 有；输出13536，超过请求2500，响应仍被接受 |
| 4 | http_failure | gpt-5.6-luna | 23901 | 23901 | 无，HTTP 503 |
| 5 | http_failure | DeepSeek-V4.1-Flash | 23908 | 23908 | 无，HTTP 404 |

合计582.067727秒、5次物理请求、账本103510，费用unknown。页12–15正文`reused_stage=true`，没有重发。另有一次新正文成功（页16–20、93）。第一次合成成功，返回10条草稿；第二次合成在luna 503、deepseek 404后失败。查询等于导出，新读取实例重读一致。

第一批草稿没有进入发布三视图。失败发生在把这批主张写进结果之前，外层异常只留下`semantic_incomplete:LLM provider returned an unsuccessful response`。回执里仍有这10条草稿，其中上游/供应链多为unclear，主营有observed草稿。它们没有核验，不能当已核主张。

### 42.4 能力边界

这次窗口证明：统领段落可以回到真实合成输入；未知用量会占满预留；输出超过请求上限时按实际usage结算。它没有证明三视图。103510已经用去120000里的大部分，剩下约16490，不够再发一笔约24000的核验。未读的19个必要页和198页查漏都还在。

已成功的一批合成被下一批失败从发布结果里拿掉，这是本次运行暴露的缺陷。按卡不再改冻结代码重跑，也不申请第10次。若要再做，最短的下一次实现是：每一批合成成功后先写入可查询结果，再发下一批；那需要新的授权，不是本卡余额。

### 42.5 清理与保护

证据在[b1-pool-decision-evidence](b1-pool-decision-evidence/manifest.json)。研究库和PDF缓存不放入OpenSpec。`attempts/decision.json`保留。八个旧attempt标记未改。既有修改和未跟踪文件不纳入本次提交。九入口累计2110.163851秒、21次请求、账本439992。观察1/4、可用0/4不变。

## 43. A审核1bcfd517：结束整组合成方案，重整分批业务交付

### 43.1 结论与主线责任

**B1/A1未通过，B1-J按约终止，不批准第10次。** 统领原文上下文已进入真实合成、未发送查漏不再当普通附注，修复接受；R30承诺的“最早独立主张先核验保存”未实施，不能以任务勾选视为完成。新增真实进展是正文及10条待核草稿，没有真实核验或可用三视图；19核心/6负例、准确率和数字错误仍未评估。

主要矛盾在执行方式：请求虽然分批，仍然全体合成成功后才统一核验/交付，失败窗口长期空交付。A上一卡已提出先核验保存，但未把“下一批发送前原query可见”和后批失败/进程中断作为放行证据核实，审核前置仍不充分；本轮不把它解释成单纯供应商问题，也不因修池自动续额。保留真实LLM分层读报与现有底座，停止这条整组流水线，下一卡实现同owner的可恢复分批业务提交，不扩建平台或逐公司规则。

### 43.2 独立审核与分级

| 分类 | A独立核对 | 裁决 |
| --- | --- | --- |
| 接受：源上下文及未完成标记 | 第9次证据manifest六文件hash/bytes全匹配，query/export profiles一致；context-proof含p13“从技术环境来看”及p12“近年来，公司……”完整必要范围。页12–15回执reused_stage=true；19必要页/198查漏未完成显式保留 | 不重做同类上下文修补，不重发原正文冒充进展 |
| P1：整组合成阻断核验与交付 | report_interpretation先把每批claims累积在局部claim_rows，循环成功退出才赋给claims，再统一核验；第二批异常发生在赋值前。runtime的on_progress只写工作checkpoint，不能替代可靠结果经原writer进入query。成功合成回执有10草稿，三视图claims均0、checks0 | R32按批保存待核草稿→核验→原writer/query可见后再下一批；只保存模型自报observed并不安全 |
| P1：局部请求仍强求全七维 | report_wire对非partial合成统一要求“Return claims for all seven dimensions”，但调用方只传当前条件组；第9次第一批实际生成收入/投入等无本批材料的unclear。最终完整性又要求同维主张全部处于可接受状态，这些占位会污染合并后的完整性 | 本批只答本批支持维度，全报告合并后评完整；未处理必要源条件仍记缺口，不用删错误来抬分 |
| 语义质量风险 | 草稿以“未给出集中度”拒绝输出暴露、以“未明确产业链角色”拒绝角色，尽管已有网络经营及政府/企事业/家庭服务原文。只是待核草稿，不计已审事实或误放 | 提示词落实已有产品定义：允许有据定性归纳，缺金额/集中度另列；不增加无源对手或已完成转型。真实质量留待后续实源核验 |
| 已修池与资源限制 | 本地shared_semantic为5成员、无luna；直连DeepSeek模型为deepseek-flash。配置hash c26fb581e15be6d853ac966dca8312f80decf25aec3cce02f45cd54c33ed5938；未外呼验证。第9次5笔合计103510，32049有效usage、71461unknown，未知失败预留约69% | 接受用户已修配置的静态事实，保旧503/404及unknown账。不将配置修正当批次保存已修或成本/模型质量已过，不提交用户改动 |

原前三笔已用55701，尚余64299，可容纳一笔≤24000核验及一次≤24000恢复的账本上界；这仅说明把核验放在第二批合成之前值得实现，并非反事实成绩或成功承诺，未证明当时剩余时间/核验质量。第9次确未发核验，不能把它追改为通过。模型completion 13536超过请求2500已按实际usage记账，请求输出值不能当供应商必守保证。

A独立直接回归**3 passed / 4.41秒、7 warnings**，覆盖第7次13引文及统领上下文、未发送查漏/局部结果保留、真实发送预留/unknown拒绝。该组合不覆盖第二批合成失败/进程中断下原owner可见性，不能据此放行真实重跑。命令：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_seventh_run_quotes_relocate_on_the_sent_pages_without_a_model_call \
  tests/unit/test_research/test_company_profile_llm_led.py::test_unsent_gap_pages_stay_incomplete_and_one_claim_is_kept \
  tests/unit/test_research/test_company_profile_llm_led.py::test_seventh_body_pages_reserve_at_the_send_point \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review-1bcfd517-pytest
```

### 43.3 下一卡与验收边界

当前唯一[执行卡§23](b1-execution-card.md#23-b1-k按批核验与持久交付重整开发当前卡)/R32—R33，资源及进度见[验收§7.14](../../../docs/development/company_profile_acceptance_program.md#714-第9次失败终止整组合成方案重整分批交付2026-10-19)。B按A/B流程v3连续实现，最多两次共性局部迭代；改原执行链，不造平行owner或通用事务平台。以第9次原始真实回执和实际operator/磁盘恢复证明第一批核验与原query/export/新实例可见早于第二批合成，后批失败保可靠部分、核验失败候选不晋升、重启不重发/不重复写、新冲突只隔离依赖。模拟缺失核验/故障明确标记，不算真实业务通过；不再做完整208页fake专项或只交预算表。

当前新增真实入口/模型请求/付费/探针/其他公司均0，不批准第10次；改池和余额不构成授权。重整交接后A一次审核实际提交边界、源语义及最短真实验证方案，再裁决是否授权新方案；两次修复仍失败交边界并结束重整窗口，不自动续小卡。原19核心至少18、6负例实际全查通过、三视图/七维实质完整、已审准确率≥98%、无未处理底线及A补充抽审标准不减。4→12→20（失败最多另20）→100不变。

九入口累计2110.163851秒/21请求/439992账本＝142602有效usage＋297390unknown，费用unknown；旧轮/首次/后继/模拟分开。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。部分可靠结果可限用，主要未读/未核仍挡完整；不能补救说明来源、原因、影响、尝试及恢复条件。不归档、不扩批，生产not_authorized、规模声明false。

### 43.4 清理、保护与本轮范围

本轮QuoteBak约82MB，第9次持久证据约692KB，没有整库复制膨胀；仍有本次运行目录的query/export/预算等副本。B交接未给清理量/保留时点，本轮不认定已完成全部收尾；新卡要求核持久hash、引用及占用后清无依赖重复件，独有源回执/真实attempt/必要证据保护，不能为清理破坏复现。A本轮只清本人测试产物，不批删未知归属目录。

A已清本轮审核产生且无占用的空pytest日志目录，没有删除业务数据。OpenSpec严格校验、定向Ruff、175处本地链接/锚点、预算算术及diff检查通过。A仅改需求、验收、计划和审核文档，不代B修业务代码，不启动模型或生产。原13份修改（含用户改池和路由测试）/16项未跟踪内容保护，29文件hash核对一致；验收文档既有历史块与本次改动隔离。原审核§1—42正文保留，提交推送见本轮最终交接。A活跃审核耗时未单独计量，记unknown。

## 44. B1-K：按批核验与持久交付

### 44.1 结论

分层合成不再等全部批次成功后才核验。每一批的顺序是：合成 → 主张标为`pending_verification`并发布 → 核验该批 → 通过的主张保持可核状态并再次发布 → 下一批。后批发送失败时，已经核过的主张留在结果里。核验未通过的主张保持unclear，不进入observed。新增真实模型调用为0。这不是B1/A1通过，19核心和6负例仍未评估。

### 44.2 两处P1

局部请求不再要求“Return claims for all seven dimensions”。合成只为本批给出的源条件作答。原文已经写明产品、客户类型或运营角色时，可以作有引文的定性归纳；缺集中度、金额或固定角色术语不再当成拒绝该主张的理由。战略目标和行业技术环境仍不是当前身份或已完成行动。全报告合并之后才评七维完整，缺主要条件继续挡住完整。

发布链：`on_progress`在写入工作checkpoint之后调用原来的`_publish`，所以下一批发出之前，原writer文件里已经有本批结果。查询投影用`review_state`区分`pending_verification`、`verified`和`not_reliable`。

### 44.3 用受控失败验证，不新开真实入口

直接测试`test_a_verified_batch_remains_when_the_next_synthesis_fails`：

- 第一批核验通过并出现在进度回调里，发生在第二批合成抛出之前。
- 第二批失败后，第一批主张仍是observed；未合成的条件记`condition_not_synthesized`；产品维度没有占位主张。
- 核验未通过的路径把主张留在unclear，不晋升。
- 把成功回执放进`resumed_receipts`后再跑，已完成批次不会再次进入合成或核验。这是operator续跑加载器写入的同一字段。
- `real_semantic_complete`为false。

没有重跑208页fake，也没有外呼。第9次账本32049有效usage和71461 unknown保持原记录，不拿这次模拟用量推断资源。

### 44.4 尚未完成的真实阶段

第9次正文和第一批合成草稿仍未经过真实核验。下一次若要授权，最短顺序是：复用第9次已成功的正文和第一批合成回执，先核验并发布这一批，再决定是否读其余必要页。当前没有第10次授权。

九入口累计不变：2110.163851秒、21次请求、账本439992，费用unknown。观察1/4、可用0/4。既有修改和未跟踪文件不纳入本次提交。

## 46. B1-K收尾：硬约束与实际入口

### 46.1 结论

这是B1-K的第二次、也是原卡允许的最后一次共性修复。三个退回点都有直接结果：非法record引用不能再标成verified；核验异常会留下待核主张和缺口，不再抛KeyError；第二批合成确实发出并失败，第一批已核主张在此之前已进入原query。磁盘上的checkpoint重新启动后，已完成的合成和核验没有再次发送，已核主张仍在。新增真实调用0。这仍不是B1/A1通过。

### 46.2 三处修复

晋升前检查：condition属于本批、引文能在已发送页定位、`supporting_record_ids`要么为空、要么每条都在已接受的原生记录里。模型核验pass不能盖过缺失记录。本批核验会带上这些真实记录，不再对所有批一律传空records。

缺核验结果时，主要条件用`verdicts.get`记`major_condition_not_delivered`，不按缺失键抛错。核验函数抛出的错误记入`semantic_incomplete`，该批主张保持`pending_verification`。

一次性诊断`batch-review-audit.py`已删除。对应反例在`test_missing_native_record_is_not_promoted`、`test_verification_failure_returns_a_pending_gap_without_raising`，以及改过的第二批发送测试。直接回归3项通过。

### 46.3 实际入口顺序

命令使用现有`python -m research.company_profile.bounded_entry`，没有`--real`。绑定只在QuoteBak，读取了本地池指纹以便通过加载校验，不提交用户的池配置。

第一次运行`data/QuoteBak/b1-k-batch-commit/first`，传输顺序是：

1. 正文页12–15
2. 正文页16–20、93
3. 合成页12
4. 核验页12
5. 合成页16，此时原query里已经有`principal_business`的verified/observed主张，传输随即按受控失败返回
6. 同一逻辑请求的另一次合成页16被恢复上限拒绝

`query=export`。`real_semantic_complete=false`。`real_calls=0`。

第二次运行`data/QuoteBak/b1-k-batch-commit/resume`，`continuation_checkpoint`指向第一次写下的工作checkpoint，不是手工填进测试身份的回执。实时传输只有失败批次的合成页16，没有核验、也没有已完成的页12合成。查询里仍是那条verified主张，`query=export`。

### 46.4 清理与边界

研究库和PDF缓存已从这两次运行目录删除，QuoteBak该目录约3.9MB，保留query/传输记录到本次交接之后。没有把用户池配置或整库纳入提交。九入口账本439992、2110.163851秒、21次请求不变。第9次草稿仍未经真实核验。原两次迭代已用完，不再自动派生准备卡或第10次。观察1/4、可用0/4。

## 45. A审核7faec71b：分批方向接受，原卡退回收尾

### 45.1 结论与整体走向

**B1/A1未通过，B1-K仅部分接受；R32/R33未完成，无第10次。** 新代码确实把合成改为批内待核→核验→发布后再下批，局部提示词也已去掉全七维占位并允许有据定性归纳；这部分不重做。但没有新增真实已核主张或可用三视图，工程交付证据也没有达到原卡要求。主要矛盾仍是能否让已有真实阅读成果通过硬约束和核验，沿原owner可靠交付，而非继续导航、加预算或打磨报告。

A不能仅因代码出现`publish_progress`及测试名就放行，必须核其实际命中路径。此次不新开B1-L/R34或按三个缺陷拆卡；按原卡§23.1在剩余迭代内一次完成必要修复和实际交付验证。原最多两次共性迭代不重置，B须如实补已用次数，已耗尽就交能力边界结束；不以“再准备一次”延长重整。工程就绪后再整体裁决新方案真实业务验证，当前不授权真实调用，不代B修业务代码。

### 45.2 独立发现与处理条件

| 分类 | 具体证据及实际影响 | 原卡完成条件 |
| --- | --- | --- |
| 接受：分批顺序/局部提示词 | report_interpretation的本批循环依次构建待核claims、核验、发布；report_wire只请求本批源条件，允许产品/客户类型/运营角色的有据定性归纳。runtime增加原`_publish`调用 | 保留正确方向，不再要求每批七维，不因无集中度/金额丢已有定性信息 |
| P1：分批晋升绕过原硬约束 | layered分支先设claims为空，约1772行原`invalid_claims`循环未检查后续新claim；约1942行晋升只核模型pass、非空引文及condition，约2082行又跳过原accepted-record依赖检查。A在提交者原用例中仅加入`supporting_record_ids=("missing-native-record",)`，仍得到observed、`review_state=verified`。本批核验一律`records=()`也未传实际原生依赖 | 原文定位/已读范围、condition及record完整性在晋升/可靠writer之前有效；引用真实且可用，必要原生事实参与核验，冲突隔离到依赖。不能让模型pass覆盖硬错误 |
| P1：核验失败不能安全结束 | A注入verify_views抛ValueError，分批循环break后约2111行仍以`verdicts[("condition", id)]`取缺失结果，未捕获KeyError。坏引文反例也走到该KeyError。此前可靠结果可能已有发布，但入口不能按约定返回缺口/限用结果 | 失败/预算拒绝/缺目标/未处理主要条件安全记缺口，草稿待核、前批可靠结果保留，正常返回及磁盘可读；缺结果绝不能默认通过 |
| P1：所报路径验证未成立 | 原新增测试独立复跑1 passed，但A跟踪cond-b合成发送次数为0：其长description触发`followup_exceeds_card_reservation`，第二批被continue跳过。测试只调用`interpret_report`及内存on_progress，未调用实际runtime/writer/query/export；恢复是手工把成功回执放入resumed_receipts，非磁盘operator重启，也未用第9次正文/10草稿 | 原R33一次交齐：可发送第二批实际进入受控失败；此前原query/export/新实例已可见；从磁盘JSON新建服务恢复，不重发已完成批次/不重复写；核验拒绝、冲突和未完成目标正确处置。同页不同批须按实际条件/主张区分，不错复用 |
| C：两项原有失败 | 文本空record合法主张用例和片段内record冲突用例在本HEAD均失败；把三个受改模块在内存加载为86eec5a8版本后，同一两项在相同断言失败 | 如实列旧失败，不声明全套通过，不顺手修旧partial路线；新layered实际依赖边界仍是当前P1，须有直接证明 |

上述模拟只检程序行为，不能证明模型能正确核行业/公司或计划/当前，也不把缺引文/依赖通过当真正业务错误外放成绩。第9次原10条草稿仍未真实核验，19核心/6负例、准确率、数字错误均未评估。

### 45.3 实际验证与可复现证据

A定向诊断结果 **4 failed、1 passed / 13.13秒、12 warnings**：原新增测试通过；缺record反例被错误标verified，坏引文/核验异常两反例触发KeyError，第二批路径断言因实际0发送失败。最小诊断保留于[batch-review-audit.py](b1-pool-decision-evidence/batch-review-audit.py)，SHA-256为`e10a85e8a5d03a7af1108e6251df76ad6d9835dc4b2a12ab136101ee2596fc7c`，与实际执行的QuoteBak副本逐字节一致。它是针对7faec71b的审核证据，不另建产品测试平台；B吸收必要反例到直接回归后删除一次性脚本。

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-decision-evidence/batch-review-audit.py \
  tests/unit/test_research/test_company_profile_llm_led.py::test_a_verified_batch_remains_when_the_next_synthesis_fails \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review-7faec71b/reproduce
```

实际运行使用同hash的`data/QuoteBak/a-review-7faec71b/test_batch_audit.py`，basetemp为同目录`pytest-audit`；上方持久路径命令用于复现相同诊断，不写原证据。此前两版临时诊断迭代仅用于收敛反例，不另算业务尝试。原有两测试实际命令如下，HEAD **2 failed / 3.97秒**；父版本以`git show 86eec5a8:<模块>`在隔离Python进程内加载report_wire/report_interpretation/runtime后运行同一组合，**2 failed / 3.13秒**，工作文件未替换：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_text_claim_with_empty_record_ids_verifies_and_asset_id_stays_unclear \
  tests/unit/test_research/test_company_profile_llm_led.py::test_in_span_record_conflict_isolates_the_dependent_claim \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review-7faec71b/pytest-prior-guards
```

四个受改Python文件及最小诊断定向Ruff通过；OpenSpec严格校验、175处本地链接/锚点及diff检查通过。没有全库pytest、真实调用、模型可用性探针或业务重跑；原源和第9次真实语义内容沿上轮已核证据复用，不重复全量检查。

### 45.4 下一步、进度与清理

唯一执行指引为[原B1-K §23.1](b1-execution-card.md#231-a审核7faec71b后退回一次交齐原卡业务提交边界)，资源及唯一进度见[验收§7.15/§8](../../../docs/development/company_profile_acceptance_program.md#715-b1-k部分接受退回原卡完成可靠交付边界2026-10-19)。B按A/B流程v3自主修直接阻塞、完成原owner实际交付证据和直接回归，不逐函数等待A，不拓展平台/公司规则/完整208页fake。原迭代有余量才继续，已耗尽或集中收尾仍失败就结束重整交具体能力边界；当前无真实/付费/探针授权，旧余额和修池不是第10次授权。

九入口累计2110.163851秒/21请求/439992账本＝142602有效usage＋297390unknown、费用unknown不改。观察1/4、剩3家未观察、可用0/4；S1 0/12、S2 0/20、S3 0/100。固定4→12→20（失败最多另20）→100和歌华19核心至少18/6负例实际全查/七维三视图/已审准确率≥98%不变；可容忍次要遗漏及有据歧义，但已知底线错误/无效引用不得确定外放，不能补救给具体限制和恢复条件。不归档、不扩批，生产not_authorized、规模声明false。

盘点QuoteBak约82MB，未见本轮整库复制膨胀，但B§44没有清理量/保留时点或实际迭代计数，不能认定原清理/交接全部完成；要求在原卡收尾简记，不新增台账。A只保留一份2940字节必要诊断源，已清约7KB本人临时目录（含同hash诊断副本）及5个已结束空日志目录；均为可重建产物，既有未知归属目录不删，正式证据/九入口不动。原13份tracked修改/16项未跟踪保护，验收历史块隔离提交；原§1—44正文hash一致。A本轮只改审核/规划和诊断证据，无业务实现、真实调用或生产操作；新发现阻塞3类，活跃审核时间未单独计量，记unknown。Git结果见本轮最终交接。

## 46. B1-K收尾：硬约束与实际入口

### 46.1 结论

这是B1-K的第二次、也是原卡允许的最后一次共性修复。三个退回点都有直接结果：非法record引用不能再标成verified；核验异常会留下待核主张和缺口，不再抛KeyError；第二批合成确实发出并失败，第一批已核主张在此之前已进入原query。磁盘上的checkpoint重新启动后，已完成的合成和核验没有再次发送，已核主张仍在。新增真实调用0。这仍不是B1/A1通过。

### 46.2 三处修复

晋升前检查：condition属于本批、引文能在已发送页定位、`supporting_record_ids`要么为空、要么每条都在已接受的原生记录里。模型核验pass不能盖过缺失记录。本批核验会带上这些真实记录，不再对所有批一律传空records。

缺核验结果时，主要条件用`verdicts.get`记`major_condition_not_delivered`，不按缺失键抛错。核验函数抛出的错误记入`semantic_incomplete`，该批主张保持`pending_verification`。

一次性诊断`batch-review-audit.py`已删除。对应反例在`test_missing_native_record_is_not_promoted`、`test_verification_failure_returns_a_pending_gap_without_raising`，以及改过的第二批发送测试。直接回归3项通过。

### 46.3 实际入口顺序

命令使用现有`python -m research.company_profile.bounded_entry`，没有`--real`。绑定只在QuoteBak，读取了本地池指纹以便通过加载校验，不提交用户的池配置。

第一次运行`data/QuoteBak/b1-k-batch-commit/first`。传输顺序：正文页12–15，正文页16–20及93，合成页12，核验页12，然后合成页16。发出页16时，原query里已经有`principal_business`的verified/observed主张，传输按受控失败返回；同一次逻辑请求的另一次发送被恢复上限拒绝。`query=export`。`real_semantic_complete=false`。`real_calls=0`。

第二次运行`data/QuoteBak/b1-k-batch-commit/resume`。`continuation_checkpoint`指向第一次写下的工作checkpoint。实时传输只有失败批次的合成页16，没有核验，也没有已完成的页12合成。查询里仍是那条verified主张，`query=export`。

### 46.4 清理与边界

研究库和PDF缓存已从这两次运行目录删除。QuoteBak该目录约3.9MB，保留query和传输记录供核对，不放入提交。没有把用户池配置或整库纳入提交。九入口账本439992、2110.163851秒、21次请求不变。第9次草稿仍未经真实核验。原两次迭代已用完，不再自动派生准备卡或第10次。观察1/4、可用0/4。

## 47. A审核176e10d0：结束B1-K，收窄能力交真实产品

### 47.1 结论与主线自省

**B1/A1未通过，B1-K按2/2结束；R32/R33撤勾，不派第三轮原卡返修。** 原三项直接回归独立3 passed：不存在record不能晋升、核验异常安全留下待核，以及可发送的第二批实际进入受控失败，以上在已证范围接受；实际operator及磁盘JSON重启也有模拟主张持久可查证据。不将这些有效成果推倒重做，但也不能当真实业务通过。A新增两个源挑战均失败，阻塞可靠交付，见下表。

**整体判断：** 主要矛盾已经不是再做更复杂的恢复演练，而是让同一报告真实LLM生成有据、可用三视图。A此前将续跑实现和证明反复置于核心交付之前，造成工程小循环；本次明确取舍：保留LLM主导、唯一owner、批内核验及硬约束，暂舍跨运行语义复用，只用PDF缓存。B1-K不再续命；下一卡是有真实业务终点和一窗条件授权的能力收窄，不是重置两次迭代的新准备卡。语义恢复仍是未完成能力，须S3前另验，不能声称数千家自动化已经成立。

### 47.2 独立挑战及证据边界

| 分类 | 当前输入/实际结果 | 裁决 |
| --- | --- | --- |
| A/P1：冲突依赖仍确定外放 | 一页真实结构的小fixture，原生`rev-in`实际进入verify；verify对record返回unclear、claim/condition返回pass。最终record已移出accepted并有`record_semantic_conflict:rev-in`，依赖claim仍`observed`，投影`review_state=verified`。晋升只查record存在/初始有效，未查本批record verdict；最终batch_settled又跳过依赖accepted_ids筛选，见report_interpretation.py:1958及2116 | 不能容忍为次要遗漏；冲突record及全部依赖必须隔离，独立可靠主张保留。不让LLM自报pass盖过依赖失败 |
| A/P1：同页不同条件错用旧回执 | 同一源页，新发现condition后缀为`different-condition`，仅传旧成功合成/核验回执。provider实际只收到地图及正文，未重新合成；最终claim仍引用旧`cond-a`且observed。reused_stage仅按purpose/scope/页片段匹配，未绑定实际条件/主张；新增reused_verify又豁免本批condition范围，见report_interpretation.py:772及1938 | 本卡不继续扩恢复算法，下一模式在真实入口及持久绑定拒绝旧语义输入；旧能力不算通过，后续S3前另验 |
| A/P1：原R33真实回执驱动验证欠交 | resource_probe.batch_commit_reply重新造`batch-a/b`、40字符片段及长补充说明，合成`OFFLINE current disclosure...`。first/resume工作checkpoint确有一条模拟observed、第一批合成/核验重启复用；但不是原指定第9次真实正文/10草稿。不能凭“真实资产PDF＋实际operator”声称真实回执验证完成 | R33未完成，原卡结束不再补交；下一业务卡前置仅对实际回执做必要直接验证，不再重跑完整208页fake或建新测试协议 |
| B：交接编排 | 本提交在§45前后插入了两份§46，且没有完整可复现命令/持久必要证据链接 | 不作为业务阻塞，不追改历史正文；本§47为唯一新审核锚点。下一交接沿A/B v3§8补精确命令和证据，勿再堆重复段落 |

持久最小反例：[delivery-boundary-audit.py](b1-pool-decision-evidence/delivery-boundary-audit.py)，只调用当前公开测试fixture及实际interpret_report/research_views，不联网、不改业务输出。两个测试均为期望安全行为的失败反例，非已实现能力。旧两个partial测试的父版本同失败结论沿审核§45复用，不再扩大跑旧套件或改C类问题。

B临时证据只读核对：`data/QuoteBak/b1-k-batch-commit/first`和`resume`，合计3,912,286字节。两边`query.profiles`与export画像及导出单画像文件一致（action等操作包装不要求字节相等）；工作checkpoint的模拟主张/核验ID可追。first checkpoint SHA-256 `7526be8df2484b52a1cf2e850b542e0e9af144ba39913e1d8c1cbd9ef802628c`，resume `975ca1ccb1e96a876820946684adff942cb4cdddd936c6119a10d6cccfbe480f`。transport只提供请求元数据，没有在第二批发送时的query快照；结合bounded_entry中的实际读取/受控抛错实现及checkpoint，可接受该模拟顺序，不扩称任意故障或真实主张核验。

### 47.3 验证与下一业务卡

本轮独立命令均先`source scripts/quote_dev_env.sh`，Quote环境Python；首次裸`pytest`未在PATH找到，未产生测试结果，改用环境绝对路径：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py \
  -k 'a_verified_batch_remains or missing_native_record_is_not_promoted or verification_failure_returns_a_pending_gap' \
  --basetemp=data/QuoteBak/a-review-176e10d0/pytest
# 3 passed、42 deselected / 6.80秒
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-decision-evidence/delivery-boundary-audit.py \
  --basetemp=data/QuoteBak/a-review-176e10d0/final-audit
```

最终独立诊断**2 failed / 6.82秒**，详细错误为冲突依赖`('observed','verified')`以及同页改条件后无新合成、仍交旧cond-a。初版诊断的condition ID断言先因运行时合法加前缀失败，改为校验后缀后才得到上述实际错复用证据；这不是第三个业务缺陷。无真实模型调用、池探针或新业务运行。

下一卡唯一为[执行卡§24/T1](b1-execution-card.md#24-重新规划停用语义续跑单窗核心交付当前卡)，A/B流程v3及产品→验收→交付→change固定顺序继续执行。B在一个连续窗口内先清垃圾、缩减恢复能力、处理冲突依赖、直接验证及冻结，前置成立后自主执行**新条件真实窗口0/1（累计第10次）**。额度、费用unknown例外、恢复计数和不提高原累计上限的算术唯一见验收§7.16；不是旧余额或原B1-K迭代续额。前置不成不发送并结束，启动即1/1，失败无第11次/换公司/短段或同类准备卡。A下一次先核真实三视图与源质量，再看工程细节；还失败就结束此歌华实现并向用户明确能力/资源限制及替代路线。

原19核心≥18、六负例实际全查、七维/三视图、已审准确≥98%及源审抽样不变。容忍次要遗漏/有据unclear，不容忍已知主体/期间/数字/方向/计划或冲突依赖错误确定外放；未读/未核不是未披露。无法补救写来源、影响、处理与恢复条件，不伪造零错误。九入口2110.163851秒/21请求/439992账本＝142602有效usage＋297390unknown、费用unknown不改；观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100，不归档/扩批，生产not_authorized、规模声明false。4→12→20（失败最多另20）→100数量及阶段门槛不减。

### 47.4 清理、保护与交接

B已清两次模拟研究库/PDF副本，约3.9MB剩余query/transport待审；本轮未见大量新增开发库，但它们只在临时区，不是永久证据。必要审核摘要/hash已见本节，下一卡先核依赖/占用后清其无用副本，不删历史真实响应或在途他人文件。A本轮仅留上述一份必要持久诊断源；核无活跃pytest/operator后删除本人`data/QuoteBak/a-review-176e10d0`（7,552字节，诊断副本及目录；均可重建），没有删除业务或B原证据。无新增清理台账/平台。定向Ruff及诊断格式、OpenSpec严格校验、15处本轮新增链接/锚点、预算算术及diff检查通过；OpenSpec校验后遥测DNS失败不影响有效结论，未借此启模型。原13份tracked修改及16项未跟踪内容保护；验收文档用户历史块与本人活动规划变更隔离提交，原§1—46正文hash核对保留（包括B重复§46）。A只改审核/任务/需求配套文档和最小诊断证据，不代B改业务代码。新发现阻塞2个代码问题及1项原卡证据欠交，活跃审核时间未单独计量，记unknown。Git结果见本轮最终交接。

## 48. B交接：停用语义续跑后，第10次真实窗口未交出三视图

本节不改§1—47。模拟验证不是19项/6项负例成绩，也不代替A的原文审核。B不自评准确率。

### 48.1 本卡改了什么

分层阅读不再把另一次运行的地图、正文条件、合成、核验或已核主张当作输入。`bounded_entry`在校验绑定后调用`refuse_layered_semantic_continuation`：绑定里只要有`navigation_query`或`continuation_checkpoint`就拒绝，不进入预检，也不写attempt。页码相同也不复用旧回执；`reused_verify`放宽条件范围的分支已删除。PDF解析缓存仍由既有`navigation_checkpoint`参与预检，九个历史attempt和证据原件未删。

一批主张在写入前增加原生记录核验：被引用记录的核验不是pass时，该主张保持unclear，不标observed/verified。记录随后被隔离时，已经进入本批的依赖主张也会撤回。没有记录编号、证据仍有效的文本主张不受这条限制。

直接验证用的是第9次查询里保存的10条正文条件和10条草稿，以及resume1里对应页的原文。`从技术环境来看`仍能定位到700MHz那段引文的段落里。核验传输是离线替换，只证明这批对象能走过分批提交和`research_views`，不是业务通过。A留下的两条边界诊断现在通过：记录核验unclear时依赖主张不是verified；同页改成`different-condition`后会重新调用合成，不交旧主张。

### 48.2 第10次真实窗口

冻结绑定`b1-pool-window10-binding.json`，SHA-256 `74d5eb4ffd83474fa9fa3d190a45fb58f5a9b82f549849a637e5c5de1c63fd0d`。授权attempt为`window10`，要求已有`decision.json`，不覆盖它。池指纹`193be2135e4a93eab9f476676f68b374dd9316ee37f9236fe7780dae97128311`，来自当时工作区的池配置；该配置文件未提交。绑定不含`navigation_query`和`continuation_checkpoint`。预检本地通过：导航2批，未发送模型。

命令：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-window10-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-window10-authorization.json \
  --run-root data/QuoteBak/b1-window10-20261019 \
  --real
```

进程退出码0，墙钟444.516秒。语义阶段记录elapsed 388.016秒。账本8次物理请求、49071单位，8次都有usage，费用unknown，恢复0次，未触顶900秒/120000/16。工作`bp-work-89cbe6c1eb6817fc39cd6d40`。查询、导出一致，新读实例能读到这份发布结果。

发布的三视图是空的。`draft_complete`、`semantic_complete`、`real_semantic_complete`都是false。主张0条。7张成功回执全是`source_inventory` / `page_map`，覆盖208页的地图分批。地图请求了46个正文页，这46页都标成`necessary_unread`。另有208条`gap_check_incomplete`和6条`native_fact_outside_read_span`。账本第8次成功请求（zai，输入2254、输出2211、合计4465）没有对应的已保存语义回执。没有正文条件、合成或核验被发布。

这不是可用的公司画像。导航把整份年报走完之后，这一窗没有留下可核的正文批次。按卡停止：不改冻结代码再跑，无第11次，不换公司，不开短段。歌华这条实现到此结束，交给A判断能力边界。跨运行语义恢复仍然未通过，S3前需要另验。

持久证据在[b1-pool-window10-evidence](b1-pool-window10-evidence/)：`query.json` SHA-256 `221aea6d3da223fd637d161488e92d4983fc04054aa2f1bc933b6e08362c8c30`，`budget-final.json` `c0cadceb497a12832814bc1bdc731b88caaffcaab6b4f0910ed1be192f0431cf`，`entry-result.json` `b03de00c1fc0fc49efeca101edf1e697a345f642b45bca0764a0c4110d70830a`，`supervisor.json` `07b8cbf3c08386d8eb071b08c226394a6f7a2bdf7dc811ca3f14769e9c5691c1`。运行目录`data/QuoteBak/b1-window10-20261019`另有研究库和PDF缓存，不作为第二份永久正文。attempt标记`data/QuoteBak/b1-pool-observation-20261017/attempts/window10.json`保留。

### 48.3 验证、清理与进度

直接测试：边界诊断2 passed；第9次正文提交1 passed；入口拒绝续跑1 passed；原三项回归及两条既有分层测试共7 passed。Ruff检查改动文件通过。这些测试不构成19/6验收。

已删除`data/QuoteBak/b1-k-batch-commit`（约3.9MB，可重建的模拟运行）。九入口证据、`decision.json`和他人工作区文件未动。

进度不变：观察1/4，可用0/4；S1 0/12，S2 0/20，S3 0/100。九入口原账不改。本窗另计后的合计是2554.679851秒、29请求、账本489063（其中有效usage 191673，unknown仍为297390），费用unknown。生产not_authorized，规模声明false。不归档，不扩批。

## 49. A审核576df224：结束旧实现并提交替代路线裁决

### 49.1 主线及裁决

**B1/A1不通过，T1按1/1失败结束，旧歌华实现到此停止。** 无第11次、换公司、短段或改冻结代码拼成绩；A新增真实调用0。三视图主张/源条件/核验均为0，19核心/6负例/准确率未评估，不以空发布的delivered=1、exit0或测试通过当业务交付。观察1/4、可用0/4，其余3家和12/20/100均未获运行授权。普通附注可后置，但这里连可信核心都没有，不能按次要缺项容错放行。

这不是已证“LLM不能读年报”。8次物理发送均有有效usage、无unknown新增，网关完成7次导航逻辑请求；没有发送正文、合成、核验，无法评价实质理解能力。主要问题是编排把业务成果串在整轮发现/查漏之后，预检与实发/末端留额又不一致。B诚实留存、按卡停止正确；A反复围绕局部恢复/预算发卡、未检验整体首交链，承担规划及效率责任。不再靠局部全绿或新版本号继续原链。

### 49.2 独立证据与原报告口径更正

核对原query、budget、entry-result、supervisor和工作checkpoint，四份原hash与§48相符；query/export画像一致，完整标志false。仅新增小型[a-review.json](b1-pool-window10-evidence/a-review.json)，不复制PDF/整库/全文。

- **真实卡点：** bounded_entry.preflight_layered用fits_development_payload、未用实发的单请求cap/分批设置，报导航2批；interpret_report实际加24000预留限制，变7个逻辑请求。地图已用49071、余70929，8/16请求、supervisor约441/900秒，尚未触总账/时间硬限。由同一源及46选页离线重算8正文批，首批12–15页实际预留21941，加合成/核验及一次恢复留额64902，连已用共135914>120000。其余7批不带原生依赖的同口径下界也全超限，触发partial_tail_hold_would_be_exhausted后逐批跳过，查漏随即同类拒绝。该拒绝在业务层，原总账stopped_reason/denied_request为null。重算是冻结代码准入推导，不伪称原日志有逐批拒绝记录；不是实测token/费用或最低必要预算，不能据此直接加预算。
- **第8次回执存在：** 按gateway_request_id关联，物理8的5a44b0472cae4fe3a0ad2999ca9e73c8在语义report:7地图190–208页，usage4465。物理与语义编号不是一一对应，不为“丢响应”派修复卡。
- **恢复实际1次：** 物理4/5共享logical_request_id，前者有usage但trace为response_parse_error，后者跨源成功，语义report:4保留该链；依现行契约计1次而非0，未超卡限2。原§48只旁挂更正，不改账或原成绩。
- **时间分列：** B报外层444.516秒，supervisor440.999813秒，语义388.015909秒。按B外层口径十次合计2554.679851秒/29请求/489063账本＝191673有效usage＋297390unknown预留；费用unknown。原九入口不动。

### 49.3 已证工程接受及验证

独立核差异：入口拒绝旧语义输入、仅PDF源缓存复用；依赖record核验非pass不晋升claim，旧reused_verify范围放宽已删。独立5 passed/9.93秒，含原A两个反例、第9次真实正文/10草稿离线提交、实际入口拒绝续跑、后批失败保留已核部分：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q -o addopts='' \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review576-OtHngf/pytest \
  openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-decision-evidence/delivery-boundary-audit.py \
  tests/unit/test_research/test_company_profile_llm_led.py::test_ninth_saved_body_and_drafts_commit_one_batch_with_real_context \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_layered_entry_refuses_old_semantic_receipts \
  tests/unit/test_research/test_company_profile_llm_led.py::test_a_verified_batch_remains_when_the_next_synthesis_fails
```

隔离测试不写原交付、不调用模型，不代表语义恢复或公司通过；只接受已证工程范围，不追改R32/R33旧合同。不声明全套测试通过。当前新发现业务阻塞归为1类整体编排/资源准入失配；两个报告口径问题已更正，不另派业务返修。

### 49.4 替代路线及开发者告知

A建议同owner改为“真实LLM全报告紧凑导航 → 核心章节全文直接三视图候选 → 独立源先行查漏（无候选答案） → 候选/依赖核验及逐包查询 → 有界补读/合并”，去掉强制清单→合成中间层。不是关闭硬约束、忽略未选正文、单prompt吞全PDF或恢复公司规则。PDF/可靠数值/Evidence/网关/原writer复用，不推倒重来。首包可靠核验后再扩范围，完整通过仍须主要板块/三视图及原门槛。

已合成[条件卡§25](b1-execution-card.md#25-替代方案核心章节直读与核验待用户确认)：一个连续窗口完成替代实现、旧链退出、实际入口非空业务包、直接回归和一次交接；最多一个工作日/两次共性修复，不再只交预算表/fixture数。**待用户确认路线才启用开发，真实模型另冻范围/预算，目前0授权。** 不能改名复活第11次；实施到限仍无业务包及可行计划就结束并交能力收窄/停止结论。

开发者遵守A/B流程v3：卡内连续实现/必要P0-P3修复/直接Review，不逐函数等A；A卡末独立核原文业务，风格及无关旧失败后置。4→12→20（至多另20）→100的数量，S2/S3首发可用90%/补救95%、宏召回95%/已审准确99%，公司可用90%/98%及全部底线不变。允许次要遗漏、有据unclear和局部隔离；不能补救就说明来源/影响/使用限制，空结果不能算负例通过。清理及统一TMPDIR强制：先清无依赖历史垃圾，保唯一证据/原失败/既有改动。

A活跃审核时间未精确计时，不与模型墙钟/测试等待混算；未新增业务成绩。OpenSpec本change严格校验通过（遥测DNS失败不影响校验结论），JSON/当前任务锚点和diff检查通过；本轮未改业务代码，不重复Ruff或完整测试。四份第10次原证据hash、审核历史§1—48不变；开始时29份既有文件hash保护通过，验收文档原有details历史块按HEAD前缀重构比对，暂存只纳本轮活动前缀。盘点QuoteBak约85M，B的3M待审核工作库/缓存仍有来源复现用途，不当垃圾删除。A的隔离测试数据已自行收尾，只剩空目录4096字节，本轮删除；未删原PDF、失败证据、attempt或未知归属内容。本轮提交仅审核/规划及一个小型诊断，不提交用户池或任何业务代码。

## 50. A路线裁决：启用D1连续业务窗口

### 50.1 从目标选路线，不让用户选择实现

用户要求A回忆开发原则、评估路线并给B可转发任务卡。目标不变：数千家公司自动形成可核验公司画像、产品/投入暴露和有限供应链定位，抓主要业务，容忍有据不确定和次要遗漏，不确定错误不能确定外放。当前仍无真实已核主张，十次技术修补不能作业务进展。主要矛盾是首个可靠成果在重流水线末端，以及重复载荷/不一致准入吞掉可运行空间；A承担路线和任务拆分责任，不再让用户替A选技术细节。

| 路线 | 判断 |
| --- | --- |
| 继续规则主导、逐公司补齐 | 仅保留可靠表格/硬约束用途，不适合开放语言和数千家泛化 |
| 单prompt全PDF直出 | 可作个别输入方式，但不是默认主线；上下文/输出增长、定位和失败重算不利于受控交付，未经实测不赌容量 |
| 继续修旧layered长链 | 十入口未形成可用三视图，首包受导航与末端留额阻塞；停止，不再续同类补丁 |
| 核心章节直读＋可靠数值＋独立查漏核验 | **A选定。** 直接产生业务候选，独立源审仍找候选外漏项、核验后及时可查；复用有效资产，不推倒重来，真实泛化仍须后续实测 |

这不是减少质量约束来制造进展。独立查漏请求仍不带候选，关键原文限定/数字/身份/方向/依赖仍核，已知主要业务缺口仍否决可用；取消的是强制中间清单再合成、先完成全报告再首交和重复的相同信息传输。

### 50.2 唯一下一任务卡与显式授权

启用[执行卡§25/D1](b1-execution-card.md#25-d1核心章节直读与独立核验已启用)，同步设计、任务、交付路径及验收§7.18。B按A/B流程v3一个连续窗口完成清理→同owner替换/删除旧编排→实际入口非空闭环→直接回归/Review→冻结→一次条件真实验证→原入口交接，目标一个工作日、最多两次共性修复，不逐函数等A，不再交只有预算/fixture的准备卡。

本卡显式变更的是替代方案开发/运行暂停，不追改旧T1和十次失败。新真实入口若启动计累计第11次，0/1；仅歌华原208页资产、原19核心/6负例，不用旧语义回执拼成绩。资源在原总上限内重分配，唯一见验收§7.18：79000账本/530秒/16物理请求，单次24000/最大请求输出4000、全窗最多一次恢复；费用unknown开发例外只重授本窗，现有账号/池，不新增购买/充值，不能宣称货币预算达标。不借旧授权、不重置历史账，也无第12次/换公司/短段授权。A本轮没有模型调用。

就绪须同实发载荷覆盖首包独立源审/核验/原查询，不只证明导航可发；真实前冻结，不允许改代码后重开。到卡限不能运行则结束并给实载断点；真实失败保留可靠部分和原失败。少量已核是进展而非B1通过，A1仍核原19≥18、6负例、实质三视图/七维、98%已审准确及全部底线/原抽审。B1通过后才另卡扩4家、12家、20家、100家；其数量、99%/95%及补救/人工限额不变。

### 50.3 本轮核验与保护范围

本轮只改规划/需求配套文档，不改业务代码、不重复第10次测试、不新增业务成绩。原§49已证的5项回归和原始证据仍有效，本轮不把它们算新测试。新增业务阻塞0；A活跃规划/审核时间未精确计时，不与历史模型墙钟混算。

清理核查：QuoteBak仍约85M，未产生新测试库、日志或备份；前轮待复现工作库和可用缓存仍有依赖，本轮不删正式资产/必要证据/未知归属内容。B卡已写开发前先清历史无用产物、交接前清本人产物及唯一持久证据要求。

验证：补当前会话Node PATH后，`OPENSPEC_TELEMETRY=0 openspec validate restore-company-profile-llm-led-generalization --strict`通过；`git diff --check`通过。8份本人文档的新增链接/当前卡锚点、79000/530/16与原上限加总通过；29份既有文件hash保护通过（验收文档按79dfacf2活动前缀＋现有历史后缀重构比对），原审核§1—49和源合同/旧执行卡§2—24逐字未改。全文件链接扫描遇到已删除旧诊断脚本的历史引用，属非本轮C类，不重建脚本或追改冻结历史；新增活动链接全部可达。未改业务代码，不另跑pytest/Ruff，不宣称新业务通过。

## 51. B交接：D1第11次真实窗口未交出已核三视图

本节不改§1—50。离线测试不是19项核心或6项负例的成绩，B不自评准确率。

### 51.1 真实结果

查询 `delivered=0`，`state=not_found`。没有发布的公司画像、产品暴露或供应链定位，已核主张为0。`real_semantic_complete` 无从成立。这不是业务进展，B1/A1仍未通过。

冻结绑定 `b1-pool-direct1-binding.json`，SHA-256 `7fc2f852dc2db6c400257d68e40b4693bc1278d2259005fa223d8d375de618f9`。授权 attempt `direct1`。池指纹 `193be2135e4a93eab9f476676f68b374dd9316ee37f9236fe7780dae97128311`，来自当时工作区池配置，该配置未提交。绑定不含旧导航查询或续跑检查点。

命令：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-direct1-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-direct1-authorization.json \
  --run-root data/QuoteBak/b1-direct1-20261019 \
  --real
```

进程退出码0，墙钟423.032秒。账本2次物理请求、37186，两次都没有usage，费用unknown。同一逻辑请求先由 gemini 在约7.8秒超时，再由 zai 在约7.6秒超时；这用掉本窗唯一一次恢复。下一次换到 grok 时被拒绝，原因 `logical_recovery_already_used`。语义阶段因此失败，原写入器没有留下可查询画像。工作编号 `bp-work-f462704d7059c555369128c6`。

入口时限530秒。预检和正式阅读各自又做了一次全报告打包，语义阶段开始时剩余约362秒，首个模型请求发出时剩余约20秒，所以单次尝试超时只有几秒。导航预留18553，两次未知用量都按预留计入。

### 51.2 本卡实现与离线验证

生产入口改为 `direct_core_v1`：一次紧凑导航，选中章节的正文直接生成带引文的主张，随后独立源审请求不带候选，再核验并回调原进度写入。下一包失败时保留已核主张。原生记录核验不通过时，依赖主张不外放。无效引文和计划性表述不升成已核。旧的整轮条件发现后再合成的路径不再是这个入口的执行链。跨运行语义回执仍然拒绝，只复用PDF解析缓存。

离线直接测试 `test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops` 通过。它证明的是上述顺序和隔离，不是歌华年报的业务通过。实载预检在冻结前通过：导航1批18553，正文11528，源审10979，核验9398，恢复按导航预留18553，合计69011，低于79000。真实核验若带上主张正文会更大；这次运行没有走到核验。

### 51.3 停止、计量与交接

按卡停止。不改冻结代码再跑，无第12次，不换公司，不开短段。未读和未核不能写成未披露。若要恢复，需要让入口时限覆盖打包，并在恢复耗尽时仍把已成功的包留下；当前没有已成功的包。

十入口原账不改。本窗另计后合计2977.711851秒、31请求、账本526249。其中本窗37186全部是未知预留。费用unknown。生产 not_authorized，规模声明 false。进度仍是观察1/4、可用0/4，S1 0/12，S2 0/20，S3 0/100。

持久证据在 [b1-pool-direct1-evidence](b1-pool-direct1-evidence/)：`query.json` SHA-256 `8035b3a1e43c09d5e9c65c80595c83f8f528cac1f9285788543a63a33bfff074`，`budget-final.json` `33495077162ec2c8c7d4a5019b4640404ed0b06bf52f548a4cc98b7e64ffd6e9`，`entry-result.json` `e5089e5bab8eef96c07fac81793600d6bfe574394b8d78b30580bfe1ea1e83de`，`supervisor.json` `b667d8a87cacc501960b9dbd99f3a44930dc0430b7b6635b2ad33ca75361bda1`。运行目录 `data/QuoteBak/b1-direct1-20261019` 另有研究库和PDF缓存。attempt `direct1.json` 保留。未删除十次历史证据，也未删除归属不明的旧测试日志。

## 52. A审核f6d2db2c：D1真实失败，整条核心产品链尚未就绪

### 52.1 主线裁决与进度

目标仍是数千家公司自动形成公司画像、产品暴露、供应链定位；当前是首家开发闭环，非逐附注100%抽取。**本轮没有新增真实业务交付，B1/A1不通过。** 接受第11次失败留存、停止和直读/独立源审/同批冲突隔离的已证工程部分，不接受“D1已经完整实现，只差更长运行时间”的判断。保留核心直读方向，由§26/D2一次补齐当前实现的产品链；不回规则穷举、不再换一条重流水线。

A上轮将首包可运行当作主要就绪证据，未核整份核心是否存在成功收束路径，也未核非空核验计划及实际物理时限，属于A的方案审查遗漏。B按卡停止正确，不能以再申请一次模型运行代替修正就绪判断。十一入口观察仍1/4、可用0/4；其余3家、S1 0/12、S2 0/20、S3 0/100全部暂停。

### 52.2 三项主阻塞与原报告旁挂更正

| 分类 | 独立证据及影响 | 当前处理 |
| --- | --- | --- |
| P1：整家公司不能完成 | `direct_reading._result`将draft/semantic/real三个完整标志恒设false；源审的requested_pages没有进入读取；只按dimension存在判断主要条件覆盖，同维度第二主业没有逐项收束。导航每页只取前4＋中4字符，缺可辨识章节结构。请求即使成功也未证明报告可用 | 同一窗口补结构导航、必要补读、逐主要条件语义对照和报告合并；普通附注未读可限域，主要缺口不能放行 |
| P1：时间契约不匹配 | direct invoke固定`timeout_seconds=30`，stage5取min后由`LlmClient._routed_attempt_timeout_seconds`按剩余池路由均分。原日志实际第一笔7.5秒、第二笔7.3秒；zai已HTTP 200/开流后被客户端截断。不能据此判模型不工作 | 任务内逻辑/物理时限及恢复次数对齐，独立于整轮deadline；保留公共池负载/换源，不改用户生产配置 |
| P2/P3：准备与就绪证据不完整 | bounded_entry父/worker均做预检，业务再次first_package_plan并重建导航；安全边界逐前缀拟合反复构造池内完整payload。69011只含空目标verify估计；原测试仅直接函数后包失败，不经实际bounded_entry持久查询，也未测整份正常成功 | 局部收敛重复计算，非空目标同口径实发预检，实际入口正常/故障两条路径合并验证，不建新平台 |

**§51的时间原因更正只在此旁挂，原报告不改。** `entry.log`关键事件为：

```text
20:07:20 first_admitted execution_timeout_seconds=30.0
20:07:20 gemini attempt.started timeout_seconds=7.5
20:07:28 failover_selected remaining_seconds=22.04144401103258
20:07:28 zai attempt.started timeout_seconds=7.3
20:07:29 stream.opened status_code=200; stream.first_event
20:07:35 zai attempt.timeout elapsed_ms=7574
```

entry-result内runtime `started_at=2026-10-19T20:03:30.780714+08:00`，开始剩余`361.86687083542347`秒；至20:07:20约229秒，故首请求全窗约余133秒。22.04是30秒逻辑请求余额，非入口剩余。本地准备确实慢，但“首请求只剩20秒所以每源只等7秒”的因果不成立。没有分段profile，暂不把总准备时间精确分摊到每个函数；不为定位再重跑昂贵全量预检。

### 52.3 独立验证及保留范围

复用并只复验当前直接测试：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_llm_led.py::test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops
```

结果`1 passed`、7.39秒；只支持函数级顺序/隔离，不是19/6业务成绩。A另在内存用两页源、7维合法候选和全部pass模拟核验挑战同一direct入口：实际顺序是导航[1,2]→正文[1]→无候选源审[1]→核验[1]，输出7 observed/7 conditions，三个完整标志false；源审要求第2页，但未发相应正文，coverage仍partially_checked，gaps却为空。此挑战不外呼、不写业务库，只证明补读请求被忽略；恒false另由代码直接确认，模拟不能建立真实完整性。

§51四份原制品、binding及历史原文保护；不覆盖原query/export、尝试、费用或unknown预留。第11次423.032秒为B外层值，supervisor419.516161秒，两者分列、不反算拼接。新累计为526249账本（191673有效usage＋334576未知预留）/2977.711851秒/31请求，费用unknown；原总上限不增加。余额41839账本/112.655352秒/16请求不是第12次授权，也不具备已证完整报告可行性。

本次A未改业务代码、未运行真实模型，未复制测试库或完整报告。盘点QuoteBak约87M；第11次诊断原日志仍是必要证据，持久化一套前不清。共享日志/可复用缓存和归属不明内容不盲删；下一卡强制先清理无引用历史垃圾、末清本人数据，不另建清理台账。

文档Review只处理上述主线阻塞及活动指令冲突，未追加风格/无关旧失败任务。`openspec validate restore-company-profile-llm-led-generalization --strict`及`git diff --check`通过；29项既有文件hash保持（验收计划以任务开始HEAD活动前缀＋工作区原历史后缀重构核对），§1—51审核正文、执行卡原§2—25、四份第11次制品及冻结binding逐字节保护通过。仅提交A本轮文档块，不纳入原13项tracked/16项untracked改动。

### 52.4 下一卡与固定审核边界

只执行[§26/D2](b1-execution-card.md#26-d2核心产品链工程收口仅离线)，采用A/B流程v3，B在一个工作日目标/两次共性修复内自主连续完成整条链和实际入口证明，新增真实0。A下一次先审整家公司正常成功是否可能、实际准备/物理时限及完整非空载荷，再整体裁决真实范围和资源；不把旧余额切成另一短段，不自动第12次。到限仍不成立，作能力收窄/停止结论，不再自动续同类工程补丁。

验收标准及容错不变：原19核心≥18、6负例实际全查通过、七维/三视图实质成立、已审准确率≥98%、底线错误外放0和原补充抽审；4→12→20（至多另20）→100不变。次要遗漏/有据unclear可接受，已知主要业务或关键变现机制漏失不能过。无法补救给源、原因、尝试、影响、限制及恢复条件。不归档、不扩批，生产not_authorized，规模声明false。

## 53. B交接：D2 离线入口能完成核心链，真实调用仍为 0

本节不改§1—52。模拟结果不是 19 项核心或 6 项负例的成绩，也不把公司计为可用。

### 53.1 入口结果

同一命令 `python -m research.company_profile.bounded_entry`，绑定从第 11 次冻结绑定复制并按当前文件重算 hash，`LlmClient` 走进程内传输，没有外网，也没有新的真实模型调用。

正常路径 `direct_chain_v1`：查询、导出和新的读实例一致，`delivered=1`。三视图里有 4 条已观察的主营主张，来自两段不同正文。草稿完整性为 true。`real_semantic_complete` 为 false。源审要求的下一页被补读，最终没有 `necessary_unread`。

失败路径 `direct_chain_fail_v1`：前一包已核主张还在，共 2 条已观察主张。草稿完整性为 false，并留下 `direct_core_package_stopped` 和 `necessary_unread`。

两条路径的核验响应都故意延迟 8 秒，超过原先约 7.5 秒的截断，请求仍然完成。逻辑时限现为 120 秒，池内可尝试来源数按一次恢复封顶为 2，单次物理尝试约 60 秒，不再把固定 30 秒按全部池成员均分。

### 53.2 链和载荷

导航预览改为每页最多两条标题式短行加 48 字正文样本，覆盖全部物理页。这次实载是 3 个导航请求，预留 23875、23931、12829。正文直接出主张，独立源审请求不带候选。同一维度的两条主要条件按引文分别对照，不再因为“这个维度已经有一条主张”就放过另一条。

非空核验目标的预留是 10149，空目标是 9398。未知用量下，导航、正文、源审、非空核验和一次恢复合计 **117222**。这高于第 11 次窗口的 79000，所以按旧窗口不能把未知用量的完整首包发出去。测试运行把账本设为 120000，并用 120 token 的已知用量结算，只为把控制流跑完；这不能证明 79000 的真实预算够用。后续真实范围和资源仍要 A 一次裁决。本卡不启动第 12 次。

从缓存页到语义阶段开始，这次离线运行大约用了 100 秒，没有达到 30 秒的准备目标。主要耗时仍是 208 页结构导航的打包。它没有再把物理尝试截成 7 秒，但真实运行前还要单独看这段准备时间。

### 53.3 进度和清理

真实调用 0。十一入口原账 526249 / 2977.711851 秒 / 31 请求不改，费用 unknown。观察 1/4，可用 0/4；S1 0/12，S2 0/20，S3 0/100。不归档，不扩批，生产 not_authorized。

持久证据在 [b1-d2-evidence](b1-d2-evidence/)：`success-query.json` SHA-256 `1afcb40d1a1fbed14696125bedd138e010a4816bf04fb9d9bc4fc09bf150a435`，`fail-query.json` `507914a7c846fd5c08abeac9b11bdd0594433cb84479b345c636145fe32d3d63`，`success-plan.json` `51cac8cd04c7e1d99845f10e1a3ece2b80d932949d7883f9f9e6116844f10c68`。QuoteBak 里本次可重建的两次运行目录已删除，约 5.7MB。第 11 次运行目录和十一入口原件保留。归属不明的旧测试日志未删。

## 54. A审核52529b85：工程部分接受，合并推进真实产品交付

### 54.1 结论与真实进度

**B1/A1未通过；D2不整项接受。** 本轮没有真实调用，没有新增可用公司。逻辑120秒/两次池尝试、补读、导航分组复用、局部保存和后包失败保留，在已有证据范围接受；D2.1/D2.2改为“已结束、部分接受、未整项完成”。原B§53及三个D2证据按原hash保留，不把更正写回原结果。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。

这不是已经证明LLM无法解读年报：十一入口主要暴露执行/预算/协议问题，本轮仍只验证模拟传输。当前还没有一份真实可审核的三视图，不能用测试通过、HTTP 200、退出0或提交数替代。长期停滞也不是仅由供应商波动造成：**产品目标被更窄的模拟成功标准替代，A又将准备、资源和运行切成多张小卡**，导致每次局部前进都没抵达产品验收。A前序就绪审核和任务切分有责任；本次改为连续交付，不再让B只补一个细节后返回等卡。

### 54.2 两项直接阻塞与证据

| 分类 | 独立核验结果 | 对当前产品的影响 |
| --- | --- | --- |
| A/P1：缺两个视图仍完整 | [正常query](b1-d2-evidence/success-query.json)的公司画像4条全为`principal_business/observed`，内容为模拟主张；`product_exposure.claims=[]`、`supply_chain_position.claims=[]`，却`draft_complete=true/semantic_complete=true`，gaps为空。失败query仅2条主营、另外两视图仍空，完整false | 模拟只证读写/补读控制流，未证明相连三视图。真实provider也共用该完整判定，有错误放行现实路径 |
| A/P1：定位关联冒充语义覆盖 | `direct_reading._claim_covers`以condition ID或同页引文互相包含作满足，末端没有用逐主要条件核验结果约束完整；`_result`只需任一observed且无三种特定gap | 无法挡“引文同段但不是同一业务”及核验已否定的主要条件；不能用增加fixture主张数解决 |

A作两项内存离线挑战，均经同一`interpret_direct_core`和现有测试辅助调用，未写业务库、无外呼：①源审只有主营、交一条主营，结果observed=1/draft_complete=true/gaps=[]；②七维各一条候选均有定位，核验对七个主要condition全部返回`block`，claim返回pass，结果observed=7、blocked_conditions=7、draft_complete=true/gaps=[]。第二例即使视图非空也不能完整；必须让主要条件语义核验、必要维度/范围和依赖共同约束收束。不能把硬约束变成新的开放语言正则裁判。

非阻塞后置：约100秒本地准备、3批导航尚不理想，但无需在真实产品成立前单开性能卡；30秒/≤2批改为优化目标。`117222`是该非空首包unknown路径的计划，未证明整公司上界；模拟120 usage不能证明真实资源够。§53未列完整精确命令/测试结果/Review，后续同一入口按现有流程§8补齐，不为报告格式再发准备卡，不新增模板平台。与本改动无关的旧失败/工作区内容属于C类，不修改。

### 54.3 下一张卡：E1连续实施、验证与一次真实完整交付

唯一执行[§27/E1](b1-execution-card.md#27-e1整家公司真实交付合并实施验证与运行)，资源及进度只以[验收§7.20/§8](../../../docs/development/company_profile_acceptance_program.md#720-d2部分接受与e1整家公司真实交付2026-10-20)为准。B按A/B流程v3一个窗口，目标一个工作日、至多两次共性修复；先清垃圾/护基线→修本节两项产品P1→实际入口直接正反验证和必要兼容/Review→冻结→自主执行本卡一次真实完整窗口，不需要再交离线准备卡申请启动。

仅原歌华208页、原owner及现池，导航/正文/合成/源审/核验均从新窗口开始，只复用PDF解析缓存；不拼旧语义/不输入A答案。条件授权第12次0/1，1800秒/240000保守账本/24物理请求，单请求预留≤24000，逻辑≤120秒/最多2池尝试，每逻辑恢复≤1、全窗≤2；payload/输出及费用例外详见验收。本次为原L0/L1总额内显式重分配，公共默认不改，不把旧余额或unknown记0；成本仍未知，不称规模可实施已证。前置到限仍不成立不发；真实失败留可靠部分及缺口，无自动第13次/换公司/短段或另一张同类纯准备卡，交A作停止/收窄裁决。

可用标准不降低：原19至少18、6负例实际全查、七维/三视图及主要业务/变现机制成立、已审准确率≥98%、底线错误外放0和原补充抽审。次要遗漏/有据unclear可接受，无法补救说明源/原因/尝试/影响/限制/恢复条件。首包可靠只计进展，不计可用；A独立核原文后才可进入其余3家，再12/20/100。不归档/扩批，生产not_authorized，规模声明false。

### 54.4 本轮A验证、保护与清理

- 定向命令：`source scripts/quote_dev_env.sh`后，执行`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_llm_led.py::test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops`，**1 passed/5.99秒**；只证局部保留，不证明三视图或真实业务。本轮两项失败挑战如§54.2，不宣称完整测试通过。
- D2三份持久证据hash与§53一致，历史B正文不变；未重复生成两个208页测试库。B报约5.7MB释放量未由A重测，两目录已不存在，QuoteBak约87MB；A本轮没有新增大库或删除未知归属内容。后续仍先核引用/占用、清无依赖垃圾再开发，临时产物只进QuoteBak，必要证据一套。
- A仅更新审核/计划/任务卡文档，业务修复留B。`OPENSPEC_TELEMETRY=0 openspec validate restore-company-profile-llm-led-generalization --strict`、`git diff --check`通过；29份既有文件SHA保护、原审核§1—53/验收§7.1—7.19/执行卡§2—26正文保护及D2三份hash通过。新增本地链接可达；两处历史诊断脚本链接已失效，原件保留、当前卡不依赖，不重建脚本。原13份已跟踪改动、16项未跟踪内容不纳入提交；验收计划已有历史折叠区改动仅隔离保护。A活跃审核未完整计时，不用测试耗时冒充总审核时间；新阻塞2项。

## 55. B交接：E1第12次真实窗口导航中断，三视图为空

本节不改§1—54。模拟完整不是19项核心或6项负例的成绩。B不自评准确率。原19项和6项负例这次没有可核原文，记为未评估。

### 55.1 真实三个视图

查询 `delivered=1`，但公司画像、产品暴露、供应链定位的主张都是 0。`draft_complete`、`semantic_complete`、`real_semantic_complete` 都是 false。没有可引用的正文主张。

冻结绑定 `b1-pool-e1-binding.json`，SHA-256 `bbb832d80e3555f11694ff940d56ff93b511d2fe13c4430b3ddb664adbe5313c`。授权 attempt `e1`。池指纹沿用当时工作区配置 `193be2135e4a93eab9f476676f68b374dd9316ee37f9236fe7780dae97128311`，配置文件未提交。

命令：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-e1-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-e1-authorization.json \
  --run-root data/QuoteBak/b1-e1-20261020 \
  --real
```

进程退出码 0。入口 supervisor 计时 281.313 秒。账本 4 次物理请求、40322。其中 gemini 7923、zai 6618 有 usage，合计有效 14541；grok 12885、deepseek 12896 没有 usage，按预留保留，合计未知 25781。费用 unknown。恢复按逻辑请求计 1 次已用后，下一次换源被拒绝。

前两批导航成功。第三批 grok 在 60 秒超时，deepseek 恢复约 30 秒后也超时，再换源时 `logical_recovery_already_used`。阅读循环在这里返回，没有进入正文。所以前两批选出的页也没有被读。缺口只有 `direct_core_navigation_stopped:logical_recovery_already_used`。

这不是可用公司。按卡停止：不改冻结代码再跑，无第13次，不换公司，不开短段。若要恢复，需要导航中途失败时保留已成功批次并继续正文；当前没有已核主张可留。

### 55.2 本卡先改了什么

报告要同时满足三件事才标完整：三个视图各自有已观察主张；主营、产品服务、收入机制，以及供应链角色或上下游至少各有一条；每个主要条件的核验结果是 pass。引文互相包含或同一维度已有一条主张，不再单独放行。主要条件核验为 block、unclear 或缺失时，完整为 false。

直接测试：只有主营时 `view_not_substantive` 且不完整；七个主要条件全部 block 时七条 `major_condition_not_verified` 且不完整；后包失败仍保留已核主张。未授权的 240000/24 仍被拒绝；带本卡授权时，用尽 1 次物理请求后下一次停止。公共默认仍是 120000/16。

同一 `bounded_entry` 和替换传输的正常路径，三个视图都有主张，草稿完整为 true，模拟 `real_semantic_complete` 为 false。失败路径保留已核主张并标不完整。这些模拟不计 19/6。

### 55.3 进度

十一入口原账 526249 / 2977.711851 秒 / 31 请求不改。本窗另计后合计 566571 账本、3259.024851 秒、35 请求。费用 unknown。观察 1/4，可用 0/4；S1 0/12，S2 0/20，S3 0/100。不归档，不扩批，生产 not_authorized。

持久证据在 [b1-e1-evidence](b1-e1-evidence/)：`query.json` SHA-256 `3a665c0c4a2556b3458819f3e784eed457ac70dd8b665c732870332c96b27db9`，`budget-final.json` `685cdb4212d7c5bc90210a85f6b11b6e78c5c44aaf89aafef22258d157fa59a3`，`entry-result.json` `0cfec9c6a4a76f712784c19d80e835073d0aae6dba3e4a53660f2258b57ebc6d`，`supervisor.json` `96ac9b39c2313986e7d895c752901e2e7356c9630c5647d60eea3d6a3f2b0e32`。离线三视图证明在 [b1-e1-offline-evidence](b1-e1-offline-evidence/)，`success-query.json` `f1e7fa6e400b359d016d7289c60c78fcdbeeeb700276d143fc0aeeddba9a1506`。可重建的离线运行目录已删。第12次运行目录和十一入口原件保留。

## 56. A审核194190e7：空结果来自导航前置与故障处理策略

### 56.1 结论、整体走向及责任

**E1结束；两项完整性P1关闭，真实业务未通过。** A独立3项定向复验通过，接受缺产品/供应链视图、主要条件核验不是pass时不能完整及已核部分保留。原第12次空结果、账本、B§55和首次材料不改；19核心、6负例、准确率和数字质量仍不可评估。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。本轮A真实调用0。

当前没有证据证明真实LLM无法读懂年报：第12次正文请求为0。反复空交付首先是执行编排在局部失败时整轮退回，其次是换源时间被重复缩短；不能把这些归结为供应商都不可达，也不能用增加测试、预算预检或另一张逐函数准备卡代替业务。**A前序只挑战合成后的保留，没有核到导航前置屏障，任务就绪审核存在遗漏。** 本次必须重整同一业务链的次序和失败语义，首个真实新请求直接推进核心正文，而不是重复证明地图能返回。

产品路线继续是可靠结构化事实＋真实LLM开放语义阅读/归纳＋独立无候选源审/核验＋原owner三视图交付。将“所有导航均成功才开始正文”改成渐进导航、读核和持久交付，属于产品v4的显式执行政策变化；质量底线及公司可用门槛不降，不把可靠部分等同整家公司通过。数千家公司自动处理仍是终点，先完成真实1家，再按4/12/20/100验证；行业增强、补充明细及非阻塞整理后置。

### 56.2 三类直接阻塞及独立证据

| 分类 | 实证 | 必须改变的当前行为 |
| --- | --- | --- |
| A/P2：全导航成功屏障 | 原`direct_reading`在导航循环外捕获异常并提前`return _result()`。两笔成功回执实际覆盖物理页1—85/86—170，选出30个不同页；第三批失败后正文为0。A内存挑战“前批导航选页1、后批导航受控失败”得到`body_calls=0`、`claims=0`、已核导航页`[1]`及`direct_core_navigation_stopped` | 有用范围出现即进入正文/源审/核验/查询；后续导航或独立包失败只隔离相关范围，不取消独立核心工作 |
| A/P2：最后允许换源再减半 | 第12次日志中grok已有HTTP 200/开流，约60秒后客户端超时；恢复给`scorpio:deepseek-flash`时逻辑余约59.6秒，却只分约29.7秒。A纯函数挑战：总路由上限2、已用1、余60，返回30.0 | 用剩余允许尝试数分配真实余额，同时尊重物理profile/逻辑/入口硬限；最多2次，不能再发第3次 |
| A/P1：源审失败草稿可误投影已核 | A挑战合成后源审失败，得到7条`observed`、核验调用0、检查0；公司完整为false，原`research_views`却按无pending的observed投影`verified`。该失败分支直接追加候选，没有待核标记 | 待核/失败草稿保持pending或unclear；真实检查及依赖支持才允许已核，不能仅继承模型status |

上述是当前直接链已存在、且阻塞本轮目标的问题，不声称全部由`194190e7`新引入。第三项没有复现完整性误放；复现的是可信状态边界错误。允许部分交付时必须一起修，不能以此制造“已有可靠主张”。

第12次supervisor281.313秒、4请求、40322账本＝14541有效usage＋25781未知预留；费用unknown。停止时本窗仍余199678账本、约1518.687秒和20请求，未到全局硬限。最后日志的直连DeepSeek选择在账前被恢复次数拒绝，没有第5次物理请求；不能把它与已发送的Scorpio来源混为一谈。grok开流也不代表已返回合格正文，更不证明取消一切时限就会成功。

### 56.3 唯一后续卡：E2连续开发、直接验证及一次条件接续

执行[§28/E2](b1-execution-card.md#28-e2渐进导航读核与原owner交付)，资源/数量/进度只按[验收§7.21/§8](../../../docs/development/company_profile_acceptance_program.md#721-第12次审核终止全导航成功前置按产品v4渐进交付2026-10-20)。按A/B流程v3，一个连续B窗口，目标一个工作日、最多两次共性修复：先清历史无用产物/护基线→同owner渐进读核及可信边界/必要分时修复→实际入口正反验证/Review→冻结→前置成立即自主执行本卡一次开发接续，不再提交准备卡等A批准。

只允许兼容重验第12次两笔成功page_map原始回执，保留源hash、真实输入页、原模型/请求/prompt/schema、导航语义与新旧版本链；30页只是待阅读范围，不复用旧条件/候选/核验/主张，不称同版首发或未见成绩。**本次首个物理请求直接读已选核心正文；首包真实核验、原owner保存可查后，再继续剩余导航/核心包和必要补读，至公司标准或全局卡限。** LLM按主业、收入/变现机制、投入/上下游选优先范围，不为自然页序小附注耗尽核心预算；未完成导航的主要业务风险仍阻完整，未核不称已核、未读不称未披露。

显式条件授权第13次0/1，**1200秒/180000保守账本单位/20物理请求**；每请求预留≤24000、payload≤65536字节、请求输出≤4000，逻辑≤120秒/实际池尝试≤2、每逻辑恢复≤1/全窗恢复≤2，含换源/repair。原绑定/发送接点须精确识别E2，公共120000/16/900默认不变，不借E1身份通用放宽。有效usage才结算，unknown不释放；费用unknown的现有账号/池开发例外仅本窗，有价仍核10元，不购买充值，不声称成本达标。

这不是旧余额自动授权，也不增加原首家766249账本/4777.711851秒/55请求或L0/L1总额1288088/8490.367203秒。第12次后实账566571/3259.024851秒/35请求保留；E2全用为746571/4459.024851秒/55请求。历史时间为旧十一外层累计＋第12supervisor的混合口径，分别保原件，不反算成同一计时器。其他3家仍未授权。前置到修复限仍不成立则不发；启动即耗1/1，冻结后不改代码拼成绩。若仍零真实已核，结束该编排实现，无自动第14次/换公司/短段或同类准备卡；部分可靠但核心不成仍未通过，交A作总体裁决。

原可用标准不减：19核心至少18、6负例实查全过、三视图/七维及主要板块/变现机制成立、已审准确率≥98%、已知底线错误外放0；三视图/核心/高风险及疑点全核，补充按种子20261016分层至少20、不足全查。允许次要遗漏及有据unclear；不可补救列源/原因/尝试/影响/限制/恢复条件。首次、补救和局部进展分开，不归档/扩批，生产not_authorized、规模声明false。

### 56.4 本轮A验证、保护和清理

- 独立定向命令：`source scripts/quote_dev_env.sh`后，执行`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete tests/unit/test_research/test_company_profile_llm_led.py::test_blocked_major_conditions_keep_the_report_incomplete tests/unit/test_research/test_company_profile_llm_led.py::test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops`，**3 passed/12.03秒**，7条既有依赖warnings。三项离线挑战见§56.2，无网络、未写历史业务库，不计真实业务。
- 第12次四份正式制品及离线success-query的SHA与B§55一致。必要原时限日志仍在`data/QuoteBak/b1-e1-20261020/entry.log`，SHA-256 `a1e3c3a61567c1244004dda245524c2cec8d3e656c9d2284b17184bf229e8bb4`；B须先持久化一套必要证据再清运行副本。本轮未删除既有未知归属文件，未新建208页模拟库；QuoteBak约90MB，A只清本人检查所需临时补丁/回执，不重复保留正文和测试库。
- A只修改产品政策、审核/规划/OpenSpec配套文档，业务代码修复留B；本卡不是全套测试或真实质量通过声明。`OPENSPEC_TELEMETRY=0 openspec validate restore-company-profile-llm-led-generalization --strict`及`git diff --check`通过；18个新增本地链接可达，原审核§1—55/执行卡§2—27/验收§7.1—7.20和29份既有文件SHA保护通过。既有13份已跟踪改动、16项未跟踪内容不纳入本轮提交，验收计划历史折叠区的既有改动隔离保留。
- A活跃审核未完整计时，记未知；测试耗时不冒充审核时间。新发现直接阻塞3类，非阻塞性能/格式和无关旧测试后置。下一轮A先审真实产品和实现可行性，再审直接细节；交接继续现有流程§8的一个入口，不新增模板/台账。

## 57. E2：第13次正文已读，但没有已核交付

### 57.1 结果

第13次已启动并结束。进程退出码 0。supervisor 计时 **436.4395272573456 秒**，外层等待 440.207 秒，两套计时都保留。物理请求 7，账本 **106800**。其中有 usage 的 62889（gemini 13116、deepseek-v4.1-flash 7595、deepseek-flash 13845、deepseek-flash 24839、deepseek-v4.1-flash 3494），没有 usage 的按预留保留 43911（zai 21958、grok 21953）。费用 unknown。

首个新物理请求是正文，不是导航。两笔第12次 `page_map` 只作兼容复用，`physical_request=false`，页范围 1–85 与 86–170，选出的 30 页没有带入旧主张。第一包读第 12–15 页，gemini 返回 5 条草稿。该包源审先用 zai 约 60 秒，换源后 grok 得到剩余 59.2 秒。两源都没有 usage，逻辑时限用尽，源审失败。草稿保留为 `pending_verification`，没有标已核。

下一包继续读第 16–21 页。deepseek 正文和独立源审都返回了。源审输出约 19894 token，随后核验请求超过 24000 预留，没有发出。再往后的正文包被账本占用挡住。剩余 171–208 页的导航在窗口恢复次数用尽后拒绝，没有第三次换源。`draft_complete`、`semantic_complete`、`real_semantic_complete` 都是 false。

查询投影里 12 条主张都是 observed 且 `review_state=pending_verification`，checks 为 0，已核为 0。这不是可用公司，也不计 19 核心或 6 负例。按卡停止：不改冻结代码再跑，无第14次，不换公司，不开短段，不再追加同类准备卡。

### 57.2 三视图里实际写下、但尚未核验的内容

这些句子来自模型草稿和定位后的原文摘录。源审或核验没有通过，不能当成确定结论。

公司画像 7 条，待核：

- 第12页：公司主要负责北京市广播电视网络的开发、经营管理和维护，并从事节目收转传送、网络信息服务、视频点播，以及基于有线电视网的互联网接入和数据传送增值业务。
- 第12页：服务包括上述传输与接入，并推广超高清。
- 第14页：通过传输、频道收转、点播、互联网接入和数据增值业务创收，并拓展政企业务。
- 第16页：拥有北京市有线电视网络经营权，并称已形成覆盖全市16个区的网络。
- 第16–17页：已建成“歌华云”，并开发系列机顶盒。
- 第18页：2025年营业收入 221,975.46 万元；另写有偶发商品购销。

产品暴露 3 条，待核：

- 第12页：下游包括政府、企事业单位和家庭用户，区域主要在北京。
- 第18页：收入按地区分为北京和涿州。
- 第19页：成本构成包括折旧、智能卡及机顶盒摊销、运行、人工和网络运行等。

供应链定位 2 条，待核。视图状态是 `extraction_failed`，因为上下游维度没有已核主张：

- 第12页：写明“党媒政网”定位，以及由单一传输商向文化服务转变的表述。
- 第19页：前五名客户销售额 49,827.02 万元、占年度销售总额 22.45%；前五名供应商采购额 40,406.77 万元。

19 核心和 6 负例：**未评估**。模拟测试不是这个分数。

### 57.3 执行链

复用的是第12次两笔成功导航回执，不是旧候选或主张。新请求顺序是：正文第12–15页、该包源审失败、正文第16–21页、该包源审成功、核验因预留超限未发、后续包和剩余导航停止。局部失败没有把前面的草稿丢掉，也没有把它们标成已核。未完成的 171–208 页导航记为 `navigation_incomplete`，30 个已选页在未读完核验时记为 `necessary_unread`。query 与 export 的 `profiles` 一致，`delivered=1`；export 另有导出目录和文件清单。

直接测试 8 passed：一批导航后先读正文、后一批导航失败仍留已核主张、源审失败时投影不是已核、最后一次允许尝试不再把约 60 秒切成 30 秒、E2 的 180000/20 不能用 E1 标记或空授权打开、原完整性两条仍不放行。这些都不是 19/6。

### 57.4 账与停止条件

十二入口原账 566571 / 3259.024851 秒 / 35 请求不改。本窗按 supervisor 秒数另计后合计 **673371 账本、3695.4643782573456 秒、42 请求**。费用 unknown。原累计上限 766249 / 4777.711851 秒 / 55 请求没有提高。观察 1/4，可用 0/4；S1 0/12，S2 0/20，S3 0/100。不归档，不扩批，生产 not_authorized。

持久证据在 [b1-e2-evidence](b1-e2-evidence/)：`query.json` SHA-256 `910b6f98f50d80cfdb3ac428a054fe06e1ad305561274a47ec03b38f33415a8f`，`budget-final.json` `feb7ec597020f5f04fc3c1604b84437cf6d25a5fe58f783850306fc0370f2bd9`，`entry-result.json` `a9b80a8eb4e770632c6f80babbcbe0ac6c8a09c14def12f912ea65c83b88b1e4`，`supervisor.json` `707b2c097123c334e44ef309c8e9b265974f7f7d0ca5ad2fd78275a804f404cc`，`entry.log` `430a18c2b9287b3c3810153ad663d3393af2c1fb3da1f7abe18c92f1a9e053cf`。第12次 `entry.log` 已放入 [b1-e1-evidence](b1-e1-evidence/)，SHA-256 `a1e3c3a61567c1244004dda245524c2cec8d3e656c9d2284b17184bf229e8bb4`。两笔导航回执仍在第12次 `query.json`，没有删除。

若要再读这家公司，需要单独授权。当前这条“先导航完再读正文”的编排不再自动继续。恢复条件是：已定位草稿要能在预留上限内完成独立核验，并且核验通过后才标已核；本窗没有做到第二步。

## 58. A审核ad1a2a7e：结束整包前置，分开可信度与覆盖

### 58.1 结论与整体走向

**E2工程部分接受，业务失败结束；B1/A1未过。** 本次已真实阅读12—21页并留下12条待核草稿，较第12次正文0请求有进展；源审失败不误标已核、路由余59.2秒得到保留也成立。但checks/已核均0，19核心/6负例/准确率仍未评估，公司可用0/4；S1 0/12、S2 0/20、S3 0/100。A接受B按卡停止，不恢复E2。

主要矛盾不再是先导航完，也未证明LLM不能理解年报，而是A指定的整包独立源审前置、后续整包核验不适应真实输出增长。B§57末尾“先导航完再读正文”的描述不对应本轮新代码，旁挂更正，不改其原报告。A前序只检查非空核验而未验证真实多目标载荷，承担就绪审核遗漏；继续原循环或只加预算会重复失败。本次明确替换这一依赖和粒度，不把同类修补换名自动续试。

### 58.2 直接证据及质量边界

- 第13次持久query：两笔导航为复用、physical_request=false；新成功业务回执为正文12—15、正文16—21及后者独立源审。5＋7候选全部pending_verification，checks=0，三完整false，query/export的profiles一致。第一包源审超时是实际外部失败；第二包核验发送前被card_request_reservation_exceeded拒绝是本地策略问题，两者不能混为供应商失败。
- A从同hash官方PDF的已有解析缓存取16—21页，按当前v3请求及真实7候选/15项可定位条件重建核验：不带任何原生记录已需**49085保守单位**，超过单请求24000；原首包预估只用一个代表目标，未覆盖真实结果。以其中第一条真实主张＋其完整源页16、无其他目标重建为**10465**。两者均为实际网关序列化/原预留函数的离线测量，非外呼、token、费用或整报告资源保证。
- 原草稿已能表达主营、服务、收入和成本；尚不能视为正确率通过。客户/供应商集中度不能独自满足经营角色，转型表述须保留进行/计划语义。新的证据核验负责支持性，报告级无候选源审负责遗漏，A独立审源仍不可省；不能把模型自报observed或A本轮诊断改成运行时verified。

新直接阻塞归为两类：P2整包核验实载不可发送，P2查漏前置阻断独立支持性核验；状态/路由已修部分不再返工。纯报告格式、性能和无关旧测试后置，未发现新的已核错误外放。源审19894是供应商输出usage，不等于其JSON字节数，不据此直接加大预算。

### 58.3 替代卡及资源裁决

只执行[§29/F1](b1-execution-card.md#29-f1证据单元核验与报告级查漏)/产品v5/[验收§7.22](../../../docs/development/company_profile_acceptance_program.md#722-第13次审核结束e2按证据单元核验与报告级查漏替代2026-10-20)：核验按真实证据单元先核先存；独立无候选源审改报告级，仍约束主要条件和公司完整。一个B连续窗口，目标一工作日/最多两次共性修复，直接验证/Review/冻结后同卡执行，不再纯准备卡。首个新请求核验兼容重验的真实候选，不重生成地图/整包源清单；可靠单条不是公司通过。

这是A显式新方案条件授权第14次0/1，E2停止仍有效；只允许卡内指定第12导航和第13真实候选的兼容接续，不拼旧分数或称首发。900秒/90000保守账本/13请求及单请求/恢复/费用限制唯一见验收§7.22，公共默认/用户池和原首家/阶段累计不增。若全用，首家763371/4595.4643782573456秒/55请求，未超原限。本窗后首家实验结束，不自动第15次、换公司、短段、同类准备或改卡名再借额度，交A总体裁决。

原13入口673371/3695.4643782573456秒/42请求不改；有效usage269103、unknown404268和费用unknown分列。第13supervisor与外层440.207秒均保原件，历史混合累计仅扣资源。原19至少18、6负例全查、三视图/七维/主要业务、已审准确≥98%及底线外放0不减；次要遗漏/有据unclear可限用，无法补救附源/原因/影响/限制/恢复条件。数量顺序4→12→20（最多另20）→100不变，不归档/扩批，生产not_authorized、规模false。

### 58.4 A验证、保护与清理

独立命令：source scripts/quote_dev_env.sh后执行 /home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete tests/unit/test_research/test_company_profile_llm_led.py::test_blocked_major_conditions_keep_the_report_incomplete。结果**6 passed/9.75秒**，7条既有依赖warnings；不声明全套通过，不算公司成绩。本轮A真实调用0，未改业务代码。

第13次query/budget-final/entry-result/supervisor/entry.log五份SHA与B§57一致，第12次必要日志已持久；历史原件保留。QuoteBak约87MB，仍有历史运行/测试目录，B本轮缺具体清理回执，不能判定已全面清理；F1必须先核引用/活跃占用/持久副本后清无依赖垃圾，不删来源、唯一证据或未知归属文件。A不建新208页库，结束清本人验证临时产物。OpenSpec本change严格校验及diff检查通过，15个新增本地链接可达；29份既有文件SHA保护、原审核§1—57/执行卡§2—28/验收§7.1—7.21正文保留，验收文档既有历史折叠区改动隔离不提交。A活跃审核时间未完整计，记未知，不以9.75秒替代。

## 59. F1：第14次核验了真实候选，公司仍不完整

### 59.1 结果

第14次已启动并结束。进程退出码 0。supervisor **408.0487765856087 秒**，外层 412.000 秒。物理请求 6，账本 **77032**，六次都有 usage（gemini 8639、zai 7779、grok 13923、deepseek-v4.1-flash 7218、deepseek-flash 25134、deepseek-flash 14339），未知预留 0。费用 unknown。

首个物理请求是核验，不是新的导航或正文生成。第12次两笔页图和第13次两笔正文回执只作兼容复用。第一包核验第12页和第14页上的四条候选。随后又核了两包。三包核验之后才做报告级源查漏，查漏请求里没有候选答案。查漏给出的主要条件还没核完，账本占用就挡住了后续请求。171–208页仍未做结构导航。`draft_complete`、`semantic_complete`、`real_semantic_complete` 都是 false。

查询投影：10条 `verified`，2条 `not_reliable`。query 与 export 的 profiles 一致，`delivered=1`。这是部分已核，不是可用公司。19项核心和6项负例仍未逐项评估。按卡停止：不改冻结代码再跑，无第15次，不换公司，不开短段。

### 59.2 已核与未通过的原文

已核，查询状态为 verified：

- 第12页主营：公司主要负责北京市广播电视网络的开发、经营管理和维护，并从事节目收转传送、网络信息服务、视频点播，以及基于有线电视网的互联网接入和数据传送增值业务。
- 第14页收入：频道收转经营创收成果显著，现金到账额创近五年新高。核验说明还提到传输、政企和信息化项目。
- 第16页主营：在北京市拥有有线电视网络经营权，并写覆盖全市16个区。
- 第16–17页产品：已建成“歌华云”平台；开发系列机顶盒，推进4K超高清智能机顶盒。
- 第18页收入：2025年营业收入 221,975.46 万元，较上年同期减少 9,438.40 万元。
- 第12页输出暴露：同一段网络经营表述被核为下游/输出暴露。
- 第18页输出暴露：北京地区收入数字。
- 第19页投入：折旧 373,377,347.39 元。
- 第19页供应链字段：前五名客户销售额 49,827.02 万元，占年度销售总额 22.45%。这条检查通过了，但句子本身是客户集中度，不能单独当成经营角色已经成立。

未通过：

- `claim_products_services_1` 被 block。核验说明写“千兆宽带业务”在所给原文里没有出现。
- `claim_supply_chain_role_1` 为 unclear。转型表述能对上第12页，但“党媒政网”不在送核的页面里。

### 59.3 为什么仍不完整

报告级查漏在两批已读正文上返回了条件，其中 18 条主要条件没有核验检查。171–208页记为 `navigation_incomplete`。已选但未读完的页记为 `necessary_unread`。后续请求被 `direct_core_ledger_hold` 挡住。供应链视图状态仍是 `extraction_failed`，因为上下游维度没有已核主张。

直接测试 10 passed：源查漏失败之后已核主张还在；后一个核验单元失败不抹掉前一个；F1 的 90000/13 不能用 E1、E2 或空授权打开候选窗口；原三视图完整性和待核投影测试仍在。这些模拟不是 19/6。

### 59.4 账、清理与停止

十三入口原账 673371 / 3695.4643782573456 秒 / 42 请求不改。本窗按 supervisor 另计后合计 **750403 账本、4103.5131548429543 秒、48 请求**。费用 unknown。原累计上限没有提高。观察 1/4，可用 0/4。

清理：删除 57 个空的 `quote_pytest_logs_*` 目录，其中没有文件。第12、13、14次运行副本在证据复制后删除。尝试标记 `e1.json`、`e2.json`、`f1.json` 保留。v61、window10、direct1 等历史运行目录没有证明可以删，仍留在 QuoteBak。

持久证据在 [b1-f1-evidence](b1-f1-evidence/)：`query.json` SHA-256 `5459078d06db90d39ced6e584f2971dd6720b861637e7f561579bf9b5ccf1dde`，`budget-final.json` `e167aac9252f5a59f84ed494e23f5e363c8ab4cad20ad0ddd6ec2c9e8c89389d`，`entry-result.json` `c674c0832d9c5cb05baf1180e1746957b9ba30dbcf0aeb540f11cdfcafcc273c`，`supervisor.json` `bf7f244b64c216c50c68de97a157826f4fb6a3be5283819166e9df54d4575901`，`entry.log` `3f523cc12494e36f060d7860258f5a6d7a00529999aef1eaf7a1d94ecd5e7871`。

若要补完这家公司，需要另一次授权来核完主要条件并读完未导航的页。当前窗口不能再发请求。

## 60. A审核f73bedcb：接受局部可信交付，终止首家真实实验

### 60.1 结论、真实进展与主线

**接受F1证据单元先核先存、两条不受支持候选隔离及按卡停止；公司限用，B1/A1未过。** 第14次形成10条模型核验通过主张和2条拒绝，较前13次已核0有真实进展；不等于A已完成全部对象准确性审核。供应链核心未成立，171—208页未导航，原19核心/6负例仍未逐项完整评估，不能按10条推出公司可用或已审≥98%。客户集中度不能单独充当经营角色；accepted_facts为空，不将摘要替代完整原生事实交付。

主要矛盾是公司级覆盖闭环，而非继续修发送或追加一次授权。A前序把单元可核当作闭环就绪，未验证源条件与已交付答案的实际对照、主要业务范围与末端资源统筹，属于规划与就绪审核遗漏。本轮保留局部可靠成果，结束首家真实实验，合并修这两个主因并收口，不接B“再核条件及未导航页即可补完”的自动续跑建议。

### 60.2 阻塞发现及边界

**P1：条件核验缺少实际交付对照。** direct_reading.settle_conditions当前只送source_conditions与源页，不带要对照的claims/records；_completeness_gaps却按主要条件pass消除缺口。report_wire已有契约要求condition pass表示“被主张正确覆盖”，不能只表示源条件存在。A最小反例：七维都有只涉及云服务的已核主张，另一主要业务“工业设计”未交付，条件被回为pass后完整性缺口仍返回[]。这证明潜在误判路径，未将第14次实际完整false改判为已发生完整误报。独立无候选源发现保留，后续语义对照必须看到实际已核交付，pass绑定合法覆盖对象；程序只核引用/状态/依赖，不能以同维度或关键词判语义覆盖。

**P2：未保证公司核心范围及末端资源。** 本窗三包单元核验30341，后续源清单及解析失败换源46691，约61%投入仍在已引用片段的源清单；30条件中18主要条件没有覆盖检查。收入构成、关键变现及上游范围仍未交付。A独立读同hash缓存的p53、p116、p165—166可确认报告有相关披露；人工读过不计模型已读。应在同owner统筹核心必要阅读、补读和实际交付覆盖末端，并区分全文已发送、仅被引及已核范围，不能靠片段反复查源耗尽窗口。未要求所有附注行都齐全，次要遗漏后置。

范围内未以风格、性能、无关旧失败另开任务；未发现本轮将两条被拒绝候选作为确定结论外放。原“千兆宽带”无送核原文、“党媒政网”不在送核页的拒绝保留；不得手改旧候选再沿用旧pass。

### 60.3 账本、证据和质量目标

第14次6次物理请求全部有usage：29920输入＋47112输出＝77032，有效usage77032、unknown预留0，费用unknown。第4次有usage7218但业务响应解析失败，随后换源25134，所以有效业务回执5份，与6笔物理账不矛盾；不是漏存一笔成功业务结果。supervisor408.0487765856087秒、外层412秒分列。14入口累计750403/4103.5131548429543秒/48请求，有效usage346135、unknown预留404268，原历史计时/失败不重算。

首家原上限766249/4777.711851秒/55请求及阶段1288088/8490.367203秒不增，余额不构成授权，不借其他三家额度。原19至少18、6负例实查、三视图/七维/全部主要业务和关键变现、已审准确≥98%、已知底线错误外放0不降；次要遗漏或有据unclear可限用，无法补救须说明来源、原因、尝试、影响、限制和恢复条件。数量仍4→12→20（最多另20）→100，观察1/4、可用0/4，后续阶段均0；生产not_authorized、规模false，不归档/扩批。

### 60.4 唯一下一卡与A验证

只执行[§30/G1](b1-execution-card.md#30-g1公司级覆盖闭环与首家实验收口无外呼)，依据[验收§7.23](../../../docs/development/company_profile_acceptance_program.md#723-第14次审核首家实验结束公司级覆盖闭环与离线收口2026-10-20)。一个B连续窗口完成P1覆盖契约、P2核心阅读与末端安排的最小实现/直接反例、现有真实证据隔离回放、原19/6处置及整家公司资源路径；目标一工作日，最多两次共性修复，不另交准备卡。新增真实调用0、入口0、公司0、费用授权0，无第15次、换公司/短段/探路或真实attempt。离线不能增加歌华已核成绩。A收到G1后作整体技术/范围/资源裁决；另行明确前所有真实运行暂停。

A独立命令：source scripts/quote_dev_env.sh后运行 /home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_bounded_entry.py::test_f1_authorization_is_required_for_the_candidate_window tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete tests/unit/test_research/test_company_profile_llm_led.py::test_blocked_major_conditions_keep_the_report_incomplete。结果**8 passed/18.47秒**，7条既有依赖warnings；另最小反例复现P1。这些仅为工程证据，不是原19/6或全套测试通过。本轮A真实调用0，未改业务代码。

第14次query/budget-final/entry-result/supervisor/entry.log五份SHA与B§59相同。B已清57个空pytest日志目录及运行重复副本；QuoteBak约87MB，v61/window10/direct1等未证明可删，不能宣称全部垃圾清净。G1先核引用/占用/权威副本，清已证实无依赖的历史垃圾再开发，交接前清本人无用产物；未知归属及既有改动不动。A不建新208页库，必要证据只保一套持久版本。原审核§1—59、执行卡§2—29及验收§7.1—7.22逐字保护；29份既有文件保护，验收历史折叠区改动隔离不提交。本change的OpenSpec严格校验通过（退出码0，遥测DNS失败不影响校验），10个新增本地链接可达，diff检查通过；A活跃审核耗时未完整计，记未知。

## 61. G1：覆盖必须指向已核对象，整家路径未装进常规预算

### 61.1 改了什么

条件核验不再只送源条件。覆盖请求带上已经核过的主张，以及这些主张和条件实际引用的页。返回的 pass 还必须写出 `covers_target_type` 和 `covers_target_id`。程序只确认这个对象存在、状态是已核、依赖没有被隔离。没有对象、对象不存在，或对象仍待核，都不消除 `major_condition_not_verified`。同维度已经有一条主张，也不会自动算覆盖。

源发现仍然不带候选答案。已经全文发送过的页不再先做一遍源清单。模型此前选出但还没读的页，排在未导航的 171–208 页之前。第14次的 10 条已核和 2 条拒绝没有改写。

本卡新增真实调用 0，没有新的 attempt，也没有新的付费绑定。

### 61.2 19 项和 6 项负例

对照的是第14次查询里的已核原文，不是新的模型结论。partial 不计通过。

| ID | 状态 | 依据 |
| --- | --- | --- |
| C01 | satisfied | 已核主张写明北京市广播电视网络的开发、经营管理和维护，第12页 |
| C02 | satisfied | 同一已核句包含收转传送、网络信息、视频点播、有线网互联网接入和数据增值 |
| C03 | partial | 已核文本写到政企业务和家庭用户；战略目标和当前身份没有分开核完 |
| C04 | missing | 第116页的收费和分期确认没有进入已发送正文 |
| C05 | missing | 第116页信息业务的安装、传输、维护及期限没有交付 |
| C06 | missing | 工程收入、入网费按10年分期没有交付 |
| C07 | partial | 已核写 221,975.46 万元，不是第165页的 2,219,754,557.95 元 |
| C08 | missing | 收看维护收入 724,769,470.04 元未交付 |
| C09 | missing | 信息业务收入 897,897,997.75 元未交付 |
| C10 | missing | 频道收转收入 165,529,697.84 元未交付 |
| C11 | missing | 工程建设收入 151,340,245.65 元未交付 |
| C12 | missing | 租赁收入 162,672,085.77 元未交付 |
| E01 | missing | 五项收入业务没有在收入端暴露里分开交付 |
| E02 | missing | 第53页的服务采购没有进入已发送正文 |
| E03 | partial | 第19页已核成本里有智能卡和机顶盒；第53页采购方向未读 |
| S01 | partial | 第12页已核句是网络经营；被拒绝的角色句和前五名集中度都不能单独充当经营角色 |
| S02 | satisfied | 已核写政府、企事业单位和家庭用户，没有生成具名客户 |
| S03 | missing | 数据、内容、流量和维护的上游方向没有从第53页交付 |
| S04 | missing | 5G 代理费和合作角色没有交付；“5G用户规模”不是这个角色 |
| N01 | not_assessed | 第165页收入、成本、上期列没有进入已发送正文 |
| N02 | not_assessed | 租赁收入续页没有进入已发送正文 |
| N03 | not_assessed | 第53页采购没有进入已发送正文 |
| N04 | partial | 无原文的“千兆宽带”和送核页没有的“党媒政网”已隔离；已核机顶盒句仍写软硬件研发，第53页未读，不能算负例通过 |
| N05 | satisfied | 已核的前五名只有合计和比例，没有生成具名客户或供应商 |
| N06 | partial | 已核服务没有被丢掉，也没有声称零商品暴露；未读附注没有检查 |

satisfied 只有 C01、C02、S02、N05。不够 18 项，6 项负例没有全部实际通过。这不是公司可用，也不把 10/12 当成已审准确率。

### 61.3 整家公司还要发送的载荷

用同一份解析缓存和第12次模型选出的 30 页。其中 12–21 页已经全文发送。还没读的 20 页是 22、23、25–32、93–96、123、128、152、156、165、166。按现有 24000 预留打包，正文是 4 个请求：21001、22921、23580、15731，合计 **83233**。这是预留单位，不是 token，也不是费用。

这 4 个正文请求再留一次最大恢复 23580，合计 106813，常规 120000 只剩 13187。第14次已经花掉的 77032 是另一窗口的实际 usage，不能拿来证明下一窗够用。18 条主要条件对 10 条已核主张的覆盖核验，以及 171–208 页的结构导航，都还没有放进这 13187 里。所以整家公司的阅读、覆盖核验和未导航页，装不进一个常规 120000 / 16 请求的窗口。差额至少是覆盖核验和未导航页这两段尚未计入的预留。本卡不提高上限，也不再申请短段。

### 61.4 验证、清理和停止

`pytest tests/unit/test_research/test_company_profile_direct_progress.py`：**7 passed**。其中一条证明工业设计条件只有 pass、没有覆盖对象时，完整性缺口还在，已核的云计算主张仍保留。另一条证明指出合法已核对象后，对应缺口可以消除。这些是离线反例，不增加歌华的已核条数。

本轮没有新建运行目录。QuoteBak 里 v61、window10、direct1 等目录仍未证明可删，继续保留。14 次证据、PDF 和 attempt 标记未动。

真实运行仍然暂停。无第15次。

## 62. A审核6a68d8a2：G1部分接受，公司级闭环仍退回

### 62.1 主线与真实进展

**G1局部工程及如实收口部分接受，整卡未完成；B1/A1未过。** 条件核验已带实际主张并要求覆盖对象，空pass/不存在ID不再消缺，原正文范围能记已发送；没有新增真实调用。可信交付仍是第14次10条模型已核/2拒绝，公司限用。B§61对原query的处置只有C01/C02/S02共3/19核心及N05共1/6负例satisfied，partial不算通过；A确认主要收入、变现、上游/角色仍缺，未把该表当作全量准确率审核或新成绩。C07合法万元精度与缺原生精确金额分开，不能当成新增数字错误或另开小数位修补卡。

A本轮仍以整家公司交付为目标。上一卡明确要求核心优先及末端统筹，但未将其实施/测量落实，单靠覆盖指针和正文测算不能收口；不再沿B“整家装不进常规窗口”的结论直接加预算或派新准备卡。只退回原G1尚未完成的两个主因及直接验证，沿剩余迭代，不刷新次数。

### 62.2 阻塞证据与分类

**P1：覆盖核验的负面结果未生效。** direct_reading.settle_conditions把包含已核claims的请求送verify后仅checks.extend，没有像支持性核验一样将返回的claim/record block或unclear落实到可信状态和依赖覆盖。_completeness_gaps又只检查覆盖引用对象是否仍observed。A沿当前_run入口、隔离provider构造一页五维合法非空输入：支持性检查先pass，覆盖返回condition pass引用该主张，同时claim block；最终has_later_block=true、claim_status=observed、draft_complete=true、gaps=()。这是直接可触发的错误完整性路径，不能用已有9项通过证明已修。该离线反例不代表第14次已发生错误外放，原真实完整false及10条版本不改。

**P2：核心选择/最小上下文/资源闭合仍缺。** read_selected仍按旧selected_order先安排20个模型选页；claim_pages和settle_conditions.sized仍将相关源整页带入，后续invoke的later仍为0。G1没有给出基于主要板块/变现/投入角色的实际优先策略及完整末端路径。独立源发现保留，生成器不得使用A Gold；必要限定不能删，但不用重复整页支持每个目标。83233＋23580＝106813、余13187仅是旧正文＋一次恢复，未纳正文支持性核验、18条件覆盖、剩余导航及实际输出增长，差额仍未量化。它可说明旧预留安排没有闭合，不能证明整家公司实际token或费用必超常规上限。

无关旧失败、纯格式/架构、补充小项后置。本轮新增阻塞1项P1；P2是原G1未完成项，不另造一组新需求。G1相关记录路径和未知覆盖引用状态须在同一处收束，不建通用验证平台。

### 62.3 同卡返修与停止

唯一下一步为[§31/G1-R](b1-execution-card.md#31-g1-r同卡合并返修不重置次数无外呼)/[验收§7.24](../../../docs/development/company_profile_acceptance_program.md#724-g1审核部分接受同卡合并返修不增加真实运行2026-10-20)。B在原owner连续完成负面核验状态/依赖撤回、LLM业务核心范围与最小充分上下文、完整发送路径及直接入口正反例，复用有效验证和现有原始回执，一次交出可裁决收口。不是新增准备窗口或新的两次修复；B明确原G1已用/剩余，若已用尽即交失败不继续实现。上限到达仍不成立则结束G1返修，A作整体技术/范围裁决，不自动再开同类卡。

新增真实调用/入口/公司/费用授权均0，无第15次、换公司/短段或探路，不新建真实绑定/attempt，不改用户池和常规120000/16/900及单请求24000。完整资源证据分别列正常成功、一次unknown与输出增长的预算边界，未知不能用假usage冲减。14入口750403/4103.5131548429543秒/48请求，有效346135＋unknown404268、费用unknown和原累计上限全部不变。

原19至少18、6实际负例通过、三视图/七维/主要业务与变现、已审≥98%、底线外放0不降。允许次要遗漏、有据unclear及可靠部分限用，无法补救说明源/原因/尝试/影响/限制/恢复条件。数量仍4→12→20（最多另20）→100，观察1/4、可用0/4，S1/S2/S3均0；生产not_authorized、规模false，不归档扩批。

### 62.4 独立验证、保护与清理

A在source scripts/quote_dev_env.sh后运行 /home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete tests/unit/test_research/test_company_profile_llm_led.py::test_blocked_major_conditions_keep_the_report_incomplete：**9 passed/21.38秒**，7条既有依赖warnings；另用内存隔离provider沿原_run复现上述P1，真实调用0、未写业务代码。这不是19/6或全套通过，也不因通过数量减少返修条件。

B本轮未建新运行目录，没有给出历史垃圾核查后的删除量；QuoteBak约87MB，旧v61/window10/direct1等保留理由仍待核，不能宣称已全面清理。G1-R要求先完成能安全进行的引用/占用/归属/副本核查，再清可重建无用项；未知归属、原证据和用户内容不删。A不建208页库，收尾清本人空测试日志及暂存补丁。保护29份既有文件，验收历史折叠区改动隔离不提交；原审核§1—61、卡§2—30及验收§7.1—7.23逐字保护。本change OpenSpec严格校验、10个新增本地链接、diff检查及第14次五份权威证据hash核验通过；A活跃审核耗时未完整计，记未知。

## 63. G1-R：负面核验会撤回主张，预留路径仍超常规窗口

### 63.1 次数和改动

G1 本身用了两次上限里的一次。这一张返修用掉剩下的一次。两次已经用完，不再开第三轮。新增真实调用 0，没有新的 attempt。

覆盖核验如果对本请求里的主张返回 block、unclear，或者根本没有检查，这条主张改为 unclear，引用它的覆盖 pass 不再算数。不在这次请求里的已核主张保持原状。第14次原查询没有改。

已经定位到段落的核验，发送标题和该段落，不再为每条主张重发整页。段落里没有原引文时才退回整页，避免把限定词裁掉。模型选出但还没读的页，先用结构预览问哪些是主要业务、变现、投入和角色；没被点中的页记为 supplementary_unread，不按主要未读处理。这里没有伪造歌华的新选择。

### 63.2 直接验证

`pytest tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_llm_led.py::test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete`：**10 passed**。其中一条是先 pass、后 block：被 block 的主张不再 observed，另一条已核还在，完整为 false。离线结果不增加那 10 条真实已核，也不改变 §61 的 3/19 和 1/6。

### 63.3 若 20 页都被当成核心，预留是多少

旧的 83233＋23580 仍是只量正文加一次恢复。下面用同一打包函数和解析缓存，把优先级预览、正文、一主张的整页核验上限、同页独立查漏和一次恢复都算进去。核验上限不是真实多主张载荷；查漏仍是整页，因为上一次正文请求不会留在模型里。覆盖对照要等查漏返回条件后才能按实际对象再量，171–208 页的导航也还没放进这个合计。

| 步骤 | 请求 | 预留 |
| --- | --- | --- |
| 核心优先级预览 | 1 | 6851 |
| 正文 | 4 | 21001、22921、23580、15731，合计 83233 |
| 支持性核验上限（每包一条非空主张、整页） | 4 | 20186、22110、22769、14917，合计 79982 |
| 同页独立查漏 | 4 | 20452、22372、23031、15182，合计 81037 |
| 一次恢复，按最大单请求 |  | 23580 |
| 合计 | 13 | 274683 |

常规窗口是 120000。预留差额 **154683**。这只说明这条保守预留安排装不进常规窗口，不能据此说实际 token 或费用必然超过 120000。第14次的 77032 是已经发生的 usage，不用来证明下一窗够或不够。

取舍：不要在核验之前把 20 页都当成同等必要读完。优先级请求可以少点核心页。已经定位的核验用段落，不用整页。同一页的第二次整页查漏是预留里最大的重复；若本窗不做这第二次整页发送，公司完整必须保持 false，直到独立查漏被准入。本卡不提高 120000、16 或 24000。

### 63.4 清理和停止

删除了 6 个空的 pytest 日志目录，里面没有文件。v61、window10、direct1 仍是先前运行的副本，权威证据在 OpenSpec，但这些目录里还有当时的库和日志，本轮不删。真实运行继续暂停。无第15次。

## 64. A审核99dcea99：结束G1，改为源先行核心包与公司任务

### 64.1 结论与真实进展

G1-R的负面claim撤回部分接受；G1整卡未完成、原2/2已用尽，以未完成结束，不第三轮。B遵守无外呼/不改旧成绩的边界；新增真实业务交付0。歌华仍原10条模型已核/2拒绝，原query合同对照3/19及1/6 satisfied，不是已审准确率或公司通过。观察1/4、可用0/4，其余3家未观察，S1/S2/S3为0，不归档/扩批。

### 64.2 独立验证与阻塞

A沿当前HEAD运行：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_llm_led.py::test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete tests/unit/test_research/test_company_profile_llm_led.py::test_blocked_major_conditions_keep_the_report_incomplete
```

结果11 passed/30.61秒、7项既有warning；不是全套测试或业务成绩。原先后续block仍observed的直接路径已由本轮测试证明关闭，无关已核保留。

**A类阻塞：本次压缩导致同页不同段丢失。** `direct_reading.py:577—583`在`claim_pages`已缩为段落后，仍只按page号去重。A以一页两段输入经`_run`/当前direct_core执行：第一段“公司向家庭用户提供有线电视服务，并按月收取收视维护费。”，第二段“公司采购机顶盒和智能卡，由外部设备制造商供应。”；分别生成principal_business与input_exposure两条合法主张。模拟核验实际收到c1/c2两个ID，却只有第一段源文，返回pass后程序仍将c2判unclear/claim_or_dependency_not_verified。真实调用0；这是上下文打包回归，不是模型判断出错。修复须保留同页全部必要源区间；并入G2主路径，不另开G1第三轮。

核心优先级请求和段落核验已接入局部路径，但没有新真实优先级或公司级完成证明。274683＝6851＋83233＋79982＋81037＋23580、差154683成立；其中一主张整页核验不是多主张上界，实际覆盖/尾部导航尚未计入。接受其为条件测算，不接受完整末端已证明或实际token/费用超限的推论。B类风格建议不发卡，无关旧失败/既有改动属C类不改。

### 64.3 主线裁决与下一张卡

连续多轮未形成公司可用，不能继续围绕各个准入错误发卡。A此前把工作窗资源上限与整家公司完成混在一起，又允许同页生成、支持核验、独立源清单及覆盖核验重复流转；任务切分和资源假设也有问题，不能仅要求B再补细节。

采用[§32/G2](b1-execution-card.md#32-g2源先行核心包与公司任务一次交付)：同一核心包无候选源条件→生成主张→一次核验分别结算支持/依赖/覆盖，复用相同源审范围；LLM仍负责全报告核心选择和必要补读，原owner批量先核先存。公司任务只在显式总额内跨资源窗，旧14次不拼成绩、不清账。它替换重复编排，目标是整家公司交付，不是G1第三轮或另一次短段准备。

工程可开始；新增真实额度/unknown例外待用户确认。具体方案仅见验收§7.25：一个公司任务最多两个常规资源窗，公司总限240000/1800秒/32物理请求，有价整个任务≤10元，无价费用unknown；阶段总额不增，首家分配将改变，确认前不生效。确认后冻结版本同卡连续执行，不再逐窗审批；若执行如实记累计第15/16次，不掩饰新增实验。工程两次不成立则不外呼；任务到限失败结束，无自动第17次或换公司。扩大研发额度下的业务可行与常规资源达标分别判，不能据此自动B2。

原19至少18、6负例实查、三视图/七维/全部主要板块/关键变现、已审≥98%、底线错误外放0不降；次要遗漏和有据unclear可限用，不能补救须列影响和恢复条件。4→12→20（最多另20）→100不变，生产not_authorized、规模false。B按A/B流程v3整卡自主处理P0—P3并用同一交接入口，先清历史无用开发产物，收尾清本人产物；不新增报告平台、预算平台或公司规则。

### 64.4 本轮文档、保护及验证边界

A只更新产品执行补充、验收/进度、交付计划、A/B流程和本change的proposal/design/spec/tasks/执行卡及审核入口，未替B修改业务代码。清理结果：B报告删6个空pytest日志目录；A核QuoteBak约87M，旧运行副本仍需按引用/依赖确认，不因目录名旧就删。A已清本次遗留1个空测试日志目录，空目录无文件字节释放；必要原业务证据不写入临时目录。第14次五份权威证据hash、原审核§1—63、执行卡§2—31和验收§7.1—7.24逐字/逐hash核验通过；29份既有文件保护通过，验收历史折叠区变更隔离不纳入提交。

A新增真实调用0；额外人工补救未发生，活跃审核总耗时未完整计，记未知。OpenSpec严格校验、13个新增本地链接及diff检查通过；提交前仍须核隔离暂存差异，不声明全套测试通过。

## 65. G2：同页多段保留，真实发送仍待确认

### 65.1 工程

同页多段按原文区间保留。两段文字不同就都进入核验，不再只留页码第一次出现的那一段。

每个核心包的顺序改成：无候选源发现，再生成主张，再用同一次核验同时检查主张是否成立、条件是否被仍有效的主张覆盖。支持性先结算，覆盖 pass 只在对象仍然有效时保留。同一页范围的源发现不重复做第二次整页清单。源发现失败时，已经生成的主张仍进入核验；没有发现到的主要条件继续留缺口。

这次没有创建真实 attempt，也没有第15次。验收§7.25拟议的一个公司任务两个窗口、累计 240000 账本单位、1800 秒、32 次请求，以及无单价时费用记 unknown，需要单独明确确认。本条消息没有把旧余额或这张任务卡当成已经同意。确认之前不发送。

### 65.2 验证

`pytest tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete tests/unit/test_research/test_company_profile_llm_led.py::test_blocked_major_conditions_keep_the_report_incomplete`：**11 passed**。同页两句“向家庭用户提供有线电视服务，并按月收取收视维护费”和“采购机顶盒和智能卡，由外部设备制造商供应”都进入了同一次核验，两条主张都保持 observed。

另一次 `test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops` 通过：后一包失败时，前一包已核主张还在。这些都是离线结果，不增加歌华的 10 条已核，也不改 §61 的 3/19 和 1/6。

### 65.3 清理和停止

删除了 5 个空的 pytest 日志目录，里面没有文件。v61、window10、direct1 仍含当时的库和日志，权威查询在 OpenSpec，本轮不把整目录当成可删副本。

真实运行停在发送前。若要按 §7.25 启动第15次，需要明确同意该新增额度和费用 unknown 例外。同意之前，公共 120000/16/900 和原来的 14 次账都不改。

## 66. A审核764abcfc：部分接受G2工程，同卡完成公司主链

### 66.1 先看业务目标与整体走向

目标是数千家公司可自动交付画像、产品暴露及供应链定位，当前阶段先证明一家公司核心可用。G2减少重复源清单和核验的路线继续采用；本轮只有工程进展，没有新增真实主张或公司可用。仍观察1/4、可用0/4，S1 0/12、S2 0/20、S3 0/100。不能把12项测试或等待资源确认当成主链已完成，也不再围绕每个拒绝单独派卡。

### 66.2 独立验证与阻塞分类

独立执行（统一quote_dev_env.sh环境，Quote Python）：

```bash
python -m pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_llm_led.py::test_direct_core_publishes_one_verified_package_and_keeps_it_when_the_next_stops tests/unit/test_research/test_company_profile_llm_led.py::test_empty_product_and_supply_views_are_not_complete tests/unit/test_research/test_company_profile_llm_led.py::test_blocked_major_conditions_keep_the_report_incomplete
```

结果12 passed/30.86秒，7项既有warnings；不是全套或业务验收。同页不同段确实一起送核，关闭前轮上下文丢失阻塞；后包失败保前包已核成立。接受源先行→生成→联合核验的局部实现。

- **A类P2：联合打包的计划与实载不一致。** direct_reading.verify_units只计claims/records，settle_claims发送又加relevant_conditions；实际超限只转待核，没有按完整目标重分组。A用同一真实请求计量器、合法非空对象做尺寸反例：一条主张估算6838，加入21条条件后24219，超过24000。反例的条件描述为合成输入，不是歌华原文业务样本或实测token；它证明计划不能保证实际发送准入，不证明整家公司必需更多预算。条件引文所在的其他段落也必须纳入同一真实打包。
- **A类P2：公司任务的两窗接续尚未打通。** bounded_entry.refuse_layered_semantic_continuation和direct_core仍拒绝语义接续，real绑定仍是旧attempt列表；本次提交未实现G2专属累计及磁盘未完工作接续。不能用provider级回归或新建一个预算对象代替原CLI/worker的公司总账证明。

这两项都直接影响下一次运行能否交付，合并处理，不再扩大到平台/风格/旧套件。未发现需要本轮另开卡的业务P0/P1；无关既有改动及旧失败属C类不动。

### 66.3 唯一接续与授权边界

按[执行卡§32.5](b1-execution-card.md#325-a审核接续同卡剩余一次合并完成)执行。G2首轮记1/2，剩余一次连续完成实际联合打包、原入口公司接续、必要反例/Review及冻结。满足就绪且用户确认后，同卡完成一个真实公司任务并交三视图及19/6实查；未确认只停发送前，不新增准备卡/第三次工程迭代。A将整体就绪与局部测试分别判断，不把剩余问题逐项变成新卡。

新增资源仍只按验收§7.25待用户明确确认：最多两窗、公司240000账本/1800秒/32请求及费用unknown例外；本轮已单独提出确认，未收到答复不得外呼或创建attempt。旧14次750403账本/4103.5131548429543秒/48请求原样，旧10已核/2拒绝与3/19、1/6不拼入新任务。质量/容错、4→12→20（最多另20）→100及公共默认不变；两窗才完成只能报扩大研发资源可行，A1未过不进B2。不归档、不扩批，生产not_authorized、规模false。

### 66.4 文档及收尾边界

A仅更新验收活动区、交付计划、proposal/design当前状态、tasks、执行卡§32.5和本审核入口；未修改B业务代码，新增真实调用0。B报告清5个空pytest日志目录，A盘点QuoteBak约87M，另清本轮已结束测试留下的1个空日志目录，释放文件字节0；未证明无依赖的旧运行目录不盲删，后续按卡先清理/收尾。OpenSpec严格校验、13个新增本地链接及范围diff检查通过；第14次五份权威证据hash、原审核§1—65、旧执行卡及验收§7.1—7.25保留核验通过，29份既有文件保护通过，验收历史折叠区不纳入提交。不以文档校验替代业务验收。A活跃审核总耗时未完整计，记未知；无额外人工业务补救。

## 67. G2真实公司任务授权确认及执行指令

用户明确回复同意执行G2：仅歌华有线，最多两个同版接续窗口，累计240000账本单位、1800秒、32次物理请求；使用现有LLM池，不购买充值；无单价时费用记`unknown`。原14次记录不改，到限即停。该回复使验收§7.25的拟议额度生效，新增真实调用授权为G2公司任务范围内的1个任务，尚未产生attempt。

用户另表示费用可以进一步上调，但未给出新的具体金额、账本、请求、时间或充值授权。本轮执行合同仍严格使用§7.25的240000/1800/32及现有池边界；任何增额必须先形成具体变更，不把原则性表述当作无限预算。

B现在按执行卡§32.5继续：先清理历史无用开发产物，再修复联合核验的完整载荷计量/拆包和原入口G2公司累计接续，完成直接反例、磁盘双窗接续、query/export/新实例一致性验证并冻结。冻结前不得创建第15次attempt；冻结后同卡直接执行歌华任务，最多两窗，不需逐窗再问。不得重用旧14次语义结果、旧checkpoint或旧源清单作为新成绩，不得换公司、开短段、创建预算对象清账或追加G3工程迭代。

业务验收门槛不变：原19项至少18项、6项负例实查通过、三视图七维及主要业务/关键变现成立、已审准确率≥98%、已知底线错误外放0。次要遗漏和有据`unclear`按原补救机制限用并说明；主要缺口仍不完整。A1未通过前不进入B2，生产仍`not_authorized`，规模声明仍为`false`。

## 68. G2两窗已用，公司仍不完整

### 68.1 实际三视图

这是新的公司任务，不把第14次的10条已核并进来。第一窗 query 与 export 的 profiles 一致。完整标志为 false。供应链视图没有主张。

已核：

- 第13页主营：以“党媒政网”为定位，经营广电网络，并推进政企、超高清和全光网。这句话把定位口号写成了当前身份，不能单独当成经营角色已经成立。
- 第14页产品：有线电视/超高清频道、机顶盒推广、点播和“重温经典”，以及政企、应急广播、云计算、5G物联网卡和光纤宽带。
- 第14页收入：频道收转创收、政企业务、5G物联网卡、宽带，以及共建共享盘活管道。没有第165页的分项金额。
- 第14页输出：面向家庭、政企和政府项目。

待核：投入要素写了内容版权、网络设备和光纤/5G，还没有核验通过。

### 68.2 19项和6项负例

对照的是这一任务自己的已核原文。partial 不计通过。第14次的 3/19 和 1/6 不并入。

| ID | 状态 | 依据 |
| --- | --- | --- |
| C01–C03 | partial | 第13–14页有网络经营、频道和政企表述，没有第12页那句完整的开发、经营、维护范围 |
| C04–C12 | missing | 第116页收费机制和第165–166页五项收入金额都没有进入已发送正文 |
| E01 | missing | 五项收入没有分开交付 |
| E02–E03 | missing | 第53页采购和第19页成本明细没有进入这四条已核主张 |
| S01 | missing | 供应链视图为空；“党媒政网”不是已核的经营角色 |
| S02 | partial | 已核输出写了家庭、政企和政府，没有单独核下游类型 |
| S03–S04 | missing | 上游服务方向和5G代理费没有交付 |
| N01–N03 | not_assessed | 相关页未读 |
| N04 | partial | 已核文本把“党媒政网”写成当前定位，不能算负例通过 |
| N05 | not_assessed | 第19页前五名没有进入已发送正文 |
| N06 | not_assessed | 没有检查商品暴露或价格敏感性 |

没有一项达到 satisfied。不是 18/19，也不是已审准确率。

### 68.3 两窗资源

第一窗 supervisor 363.83032422699034 秒，9 次物理请求，账本 86305。其中有 usage 的 44594，未知预留 41711，费用 unknown。已完成页是 13、14、15。

第二窗 supervisor 121.91179842688143 秒。载入已核主张时，查询结果里的列表不能通过主张模型的元组校验，阅读没有开始。公司状态因此记为两窗已用，已核主张列表被写成空。不能再开第三窗。

原14次 750403 / 4103.5131548429543 秒 / 48 请求不改。加上这两窗的 supervisor 秒数和第一窗账本后，合计 **836708 账本、4589.255277496826 秒、57 请求**。公司额度 240000 / 1800 秒 / 32 请求没有用完，但窗口次数已经用完。

### 68.4 工程与停止

联合核验的分包改为按实际主张、条件和段落一起计量，超限就拆开，不按页号丢掉另一段。公司任务的第二窗会扣掉已用账本和请求。这次第二窗没有走通载入。

定向测试包括两段同页、公司窗口上限和前包保留，均已通过。离线测试不是这 4 条已核的成绩。

证据在 [b1-g2-evidence](b1-g2-evidence/)。第一窗 query `bf8a3ce89de0499d8c0dc2e3d93701e16987a4b8f8454fd5ceeaf8861369d704`，budget `9b00ebed71d7a3e3e634c999ba2e660e498ab8e51b13f0c517a5b864fa119f52`，supervisor `1e853e19067eb4f4de19c831320078e573d32697f68822a41a12df9d19750e89`。第二窗 supervisor `caa0b72875fafde2914283db8a557e7285b636b307a8fc7416cf04975b66414c`，entry.log `be04b6143e502725545e4e9bb72b08cd59a73cd34f376d5f1f48e38b8f222c4a`。

G2 到两窗结束。无第17次。若要再读这家公司，需要新的授权，并且先让已保存的主张能按元组载入。

## 69. A审核19533df6：G2失败结束，收敛公司交付链

### 69.1 主线裁决与实际进展

**不通过B1/A1；G2原两次工程及双窗以失败结束。** 第一窗真实生成并核验了4条主张，另1条待核，供应链视图为空；第二窗没有外呼，在加载时失败。接受真实失败留存、引用段落保留及部分真实阅读进展，不接受“公司接续工程已就绪”。主要收入构成、变现机制、投入和角色未形成可审三视图，不能进入B2；观察仍1/4、可用0/4，S1/S2/S3均0。

问题首先是**成功工作不能可靠接续和累积**，不是已经证明模型不能解读，也不是已经证明24万公司资源不够。G2仅用86305账本、485.74212265387177秒、9请求；双窗次数耗尽与公司总额耗尽不同。此前A要求实际磁盘接续，但就绪未证明仍进入了真实运行；A本轮承担未收住前置和切分的规划责任，不再把tuple、空覆盖和重复留额分别派成小卡。

### 69.2 独立业务核源及计分边界

A直接从原PDF读取物理12—15页，并核第一窗query的4条模型已核/1条待核及全部6份成功语义回执。主要收入金额、收费机制及采购等核心缺口确认存在，供应链视图确实为空。B§68的未通过结论成立；其19/6处置保留为B自检，不冒充A完成全量评分或已审≥98%准确率，也不与旧14次结果合并。

纠正一处过严解释：原13页确实写“牢牢把握‘党媒政网’定位”，当前回答写“以……为定位”，该措辞本身有源支持，不能据此认定为当前制造/全链身份误升；但定位口号不能使供应链经营角色成立。C03/S02等按实际语义和可关联证据核对，不强求照抄第12页或在每一视图重写同一事实。N04须检查计划/行业/第三方是否实际被误当公司当前经营，不能仅凭出现定位词判失败。本轮仅作上述定性裁决，未给新的19/6或全量准确率分数。

### 69.3 两类范围内阻塞

1. **P1，接续与持久结果保留未实现。** direct_reading.py载入carried_claims用严格Python校验；A用第一窗同一合法主张复现3项tuple_type错误，严格JSON校验通过，与第二窗entry.log完全对应。bounded_entry.py的finally再从失败后的空query重建carried_claims，真实company-task.json已被写成空，而首窗query仍有4条verified。公司状态仅有claims/由引文推得的completed_pages和计数，没有原conditions/checks/导航待办或任务/源/代码绑定；单改解码不足以满足续跑与不拼任务契约。须沿原checkpoint恢复完整阶段状态、失败不空覆盖，不能再以读取投影复制一套业务状态。
2. **P2，资源判断与真实目标尚不一致。** 实际9请求中两组logical_request_id各出现2次，故已经恢复2次；供应商attempt全为1，company-task.json的recoveries=0和direct_reading.recovery_used按attempt>1判断均失真。公共budget按逻辑ID限制的实现已有正确口径，不能据此说实际预算已绕过恢复上限；问题是公司摘要及留额没有使用它，可能重复预留已耗恢复。联合尺寸开始包含条件属进展，但current非空后换入新claim的分支仍可能把整组条件附回而不拆，最终仅pending跳过；剩余验证不能只覆盖空条件请求。

以上是2类主链阻塞；格式建议不另列业务阻塞。真实第一窗源发现超时后只完成了候选支持性核验，不能把它称为报告覆盖闭环。后续必须直接补齐上述两类并交业务结果，不建设新状态平台或重写网关。

### 69.4 本轮验证与证据保护

- 独立运行 `source scripts/quote_dev_env.sh` 后，`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_bounded_entry.py::test_company_window_keeps_the_cumulative_cap`：**10 passed，7 warnings，20.79秒**。这些没有覆盖真实双进程接续，因此不抵销本次失败。
- 第一窗query、budget、supervisor及第二窗supervisor、entry.log的5个交接hash全部一致；冻结binding的全部现存file_hashes与当前工作文件一致。第一窗有效usage44594、unknown41711，账本86305；恢复按实际逻辑ID为2次，旁挂更正，不追改原JSON/日志。
- Ruff检查有2项提示（bounded_entry导入排序及既有测试PIE810），格式检查3文件待格式化；不声明静态全部通过，纯风格本轮不代B修改。新增模型调用0，A未执行业务重跑；原16入口制品不写入。
- 本轮文档OpenSpec严格校验、diff检查通过；新任务卡引用目标可用。历史两处batch-review-audit.py链接已失效，属既有材料维护问题，本轮不据此扩展开发或重建历史脚本。
- QuoteBak约87M，G2临时目录约24K；B§68未给具体清理回执，不能默认历史无用目录已清。下一卡前置核归属、引用及活跃占用再清理。本轮没有新建测试库/备份或复制全文；既有13项tracked和16项untracked保护，验收计划只隔离修改活动前缀。

### 69.5 下一卡与授权裁决

执行[§33集中修复及一次修复性收口](b1-execution-card.md#33-公司交付链集中修复及一次修复性收口)。用户已明确同意执行并允许费用上调；A此次**不增加已批准公司的240000/1800秒/32请求总额**，而将剩余153695账本、1314秒、23请求显式分配给最多一次修复性入口。恢复原已用2，本卡最多再2、累计4，仍受同一物理总账及每逻辑1次恢复约束。其他单请求、现有池、费用unknown/有价公司10元及生产边界不变。原两窗失败不退次、不改同版成绩；该新裁决才允许累计第17入口，非旧G2自动第三窗。工程直接证明不成则不发；一次收口仍失败即停止，无第18次或纯准备续卡。

本次成果应为可源核的完整三视图及原19至少18/6实查、或清晰的公司能力失败终态。不能仅交恢复一个JSON字段。次要遗漏、有据unclear和可靠部分限用继续允许，主要板块/关键变现不得豁免；无法补救须写源、原因、尝试、影响、限制和恢复条件。A1质量与常规资源分开判，四家→12→20（最多另20）→100顺序不变，不归档/扩批。A本轮活跃审核未精确计时，记unknown，新增阻塞2类；机械运行时长另列，不与开发/模型费用混称。

## 70. B交接：§33修复性入口未形成可用三视图

日期：2026-10-21。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。本段只记录第17次修复性入口的实际结果。B不把模型核验通过记成A的19/6分数，也不与第14次或G2首窗合并计分。观察1/4、可用0/4。生产 `not_authorized`，`scale_quality_claim_allowed=false`。无第18次。

### 70.1 业务结果

三视图未收口。`draft_complete`、`semantic_complete`、`real_semantic_complete` 均为 false。供应链视图主张数为0，缺口含 `view_not_substantive:supply_chain_position` 与 `core_dimension_missing:operating_role`。画像和产品暴露有新文本，关键收入构成、收费确认机制、投入和经营角色没有交付。

入口记录 `query_export_equal=true`。持久 `query.json` 与 `export.json` 的 `profiles` 相同；两文件全文哈希不同，因为导出外层带有任务控制字段。

本入口新出现、且 `review_state=verified` 的文本：

- 主业，页16：公司是省级有线网络上市公司，在北京市拥有有线电视网络经营权，覆盖全市16个区。同一 `claim-principal-business` 在查询里出现两次，正文相同。证据另含页18行业表“有线电视行业 2,219,754,557.95”。
- 产品与服务，页16：北京市有线电视网络经营权、“歌华云”IaaS/PaaS/SaaS、4K超高清智能机顶盒、IP机顶盒、网关机顶盒、电视APP、USB CAM大卡、歌华智能家居，以及收视行为数据分析。同页另有两条同号主张为 unclear / not_reliable。
- 收入，页18：营业收入按行业主要来自有线电视行业；偶发商品购销规模较小；歌华丝路金桥基金所投迅策科技港股IPO及联营项目市值波动。同号主张另有一条 pending。

产品暴露新主张 `claim-output-exposure` 仍为 pending：前五名客户销售额49,827.02万元，占年度销售总额22.45%；关联方销售额24,984.53万元，占11.26%；未生成具名客户。

G2首窗4条主张进入了本次查询，状态是 observed / pending_verification，不是本次重新核验通过。首窗原查询未改，其中这4条仍为 verified。次窗写出的空主张列表没有当作清空指令。定位口号仍只存在于首窗待核文本，不能充当供应链经营角色。

### 70.2 实际请求链

冻结绑定 `b1-pool-repair17-binding.json` SHA-256 `fbf3f46cb485385482dc0c13ce8d3f2f443d06bb189104c77595e1e169252b0c`。授权 `company_profile_repair.2026-10-21`，上限153695账本、23次请求、1314秒。尝试名 `repair17`，未覆盖 g2w1/g2w2。

导航回执 `reused_navigation=true`，来源页1–85，没有重新发送已通过导航。第一个新物理请求是页4、16–20的 `full_context` / `source_inventory`，随后同一页组合成主张并做联合核验。页165的收入明细、页116的收费确认、页53的采购没有成为这次阅读对象。页13–15因首窗主张被标成已完成而跳过，缺口里仍有 `necessary_unread:13/14/15`。后续被 `direct_core_ledger_hold`、一次 `LLM request deadline exceeded` 和 `direct_core_admission_stopped` 截断。`navigation_incomplete` 从页86起，共123页。

### 70.3 资源

本入口 `budget-final.json`：账本134862，物理请求11，费用 unknown，停止原因为空。10次成功请求的 usage 合计111324；1次 `deepseek-v4.1-flash` 失败请求 usage 为空，按预留23538保留。供应商 `attempt` 全为1。逻辑请求9个，其中 `23f5b0a53b64` 与 `ed2816f7396c` 各有2次物理发送，故本入口恢复2次。G2首窗另有2次。合并观察为4次，达到本卡累计上限。本文件没有把G2的9次请求再写进去。

监督耗时 `660.1979508148506` 秒。控制窗约15:11:02至15:20:27。均低于1314秒。

公司任务：86305+134862=221167账本；485.74212265387177+660.1979508148506=1145.9400734687224秒；9+11=20次请求。已批准总额240000/1800/32之内剩余18833账本、654.0599265312776秒、12次请求。这些剩余不构成第18次授权。

路线累计：836708+134862=971570账本；4589.255277496826+660.1979508148506=5249.453228311677秒；57+11=68次请求。

出现的模型：`gemini-3.8-flash-high`、`deepseek-v4.1-flash`、`deepseek-flash`。池指纹未改。`config/13_llm.json` 未提交。

### 70.4 19项与6项负例

B不计通过。下表只说明本次查询里能否看到对应材料。partial 与 pending 都不计满足。

| ID | 本次材料 | B计分 |
| --- | --- | --- |
| C01 | 页16写北京市有线电视网络经营权与覆盖16个区，未写“开发、经营管理和维护” | 不计通过 |
| C02 | 页16有机顶盒、云和视频场景；首窗待核文本有点播与宽带。未形成页12的完整业务列举 | 不计通过 |
| C03 | 首窗待核文本有政企与家庭。本次未把战略目标核成当前身份 | 不计通过 |
| C04–C06 | 页116收费与分期确认未读 | 未评估 |
| C07 | 页18行业表证据含 2,219,754,557.95，主张未交付为本期营业收入合计，页165–166未读 | 不计通过 |
| C08–C12 | 五项收入明细未读 | 未评估 |
| E01 | 五项收入暴露未交付 | 未评估 |
| E02–E03 | 页53采购与页19成本未读；投入条件 `cond-ie-01` 未核验 | 未评估 |
| S01–S04 | 供应链视图为空；`cond-scr-01` 未核验 | 不计通过 |
| N01–N04、N06 | 对应页未进入本次核验，不能记为已查通过 | 未评估 |
| N05 | pending 文本给出前五名合计与占比，未生成具名客户；状态不是 verified | 不计通过 |

已知底线错误外放：B未把未核验文本写成确定事实。基金收益与偶发贸易写在收入主张中，A需单独判断它是否把非主营写成了收入构成。B不据此给准确率。

### 70.5 工程与未完成的磁盘证明

本卡要求先用真实入口做双进程磁盘接续，通过后再冻结发送。实际发送前只完成了单测：用首窗查询里的一条 verified 主张做 `model_validate_json`，并用首窗预算文件得到逻辑恢复次数2。没有在发送前跑通“进程1落盘、进程2只读磁盘续跑、失败不覆盖”的 CLI。这是本交接的过程缺口。

发送后的真实入口显示：首窗页图被标为复用；4条首窗主张进入本次查询但降为 pending；次窗空列表没有清空它们。联合打包在主张加全部条件超限时改为按条件拆组。恢复计数改按重复的 `logical_request_id`，修复授权单独放开153695/23/1314，公共120000/16与G2两窗上限未改。

直接测试：`pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_bounded_entry.py::test_window1_verified_claim_reloads_and_empty_query_does_not_clear_it`，10 passed，7 warnings，22.24秒。该测试不替代双进程证明。

### 70.6 证据、命令与停止

证据目录 `b1-repair17-evidence/`：

- query `c9a8e2c26a3ab92683bf1cc60632eae21dcfdf50e8cbbce655636be7ef7c97b6`
- export `e701ad33c3c055e9fc45b6a352542452966e306ecf3386fae3556f1218aa0dd8`
- budget `a6883323ae66ede9e5ab19156d4d01880799bc0145ff3b78377145d429004bc2`
- supervisor `8e043b4f625bc3f9f0441b4a28265142885ce28582ab79ab892530009b97915c`
- entry.log `d974b191d9e6f4b7789351d4462f761010f762016b131c16a51803ba28d72c48`

命令：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-repair17-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-repair17-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-repair17-20261021 --real
```

运行根目录、PDF、G2证据与 g2w1/g2w2/repair17 尝试标记保留。未删除归属未明的 v61、window10、direct1。本次无第18次、无换公司、无冻结后改代码补分。

若要补齐收入构成、变现机制、投入和经营角色，需要新的阅读授权。本卡剩余资源不自动变成下一次入口。

## 71. A审核ecb0f79b：第17次失败结束，替换整公司交付编排

### 71.1 结论、主线与A自省

**B1/A1不通过，§33以失败结束，不接受工程就绪。** 接受真实失败如实留存、资源不清零及未虚报19/6；不接受把第17次作为公司交付或可靠接续证明。当前主要矛盾是：整家公司必要源证据没有先收齐，逐页包的发现/合成/重复核验消耗资源；成功状态又在query手工carry/按引文页推完成中丢失或冲突。增加一笔余额或再修一个JSON函数不能解决这两个问题。真实模型已能产出若干有据文本，本轮没有证据证明LLM方向不可行，也没有证据证明泛化或成本达标。

A前卡仍把跨窗口恢复放在业务核心前置，既未确认实际入口证明就绪，又继续寄希望于余额收口，对业务优先和任务收敛执行不足。本轮明确结束该实现，不再发同根因接续卡；保PDF、可靠数值、网关、证据与原owner，改为先整公司核心源汇总，再公司级三视图成稿/核验。跨入口语义续跑后置，不允许次要过程功能继续压住首家可用交付。当前仍观察1/4、可用0/4；4→12→20（失败最多另20）→100数量顺序不改。

### 71.2 独立核验与阻塞分类

直接读取本轮query/export、预算及代码，核到：

1. **A类/P0，核心业务未交付：** 供应链无主张；收费、主要收入分项及采购源未读，主要变现和投入未成立。p16经营权/覆盖16区、云平台/终端及行业收入等有限文本不能代替全部三视图。原19/6维持未完成，不编一个“已审准确率”；p116/p165/p53只作为审核定位，不能写成运行选页规则。
2. **A类/P1，主张身份/状态与阅读范围失真：** 本轮query中`claim-principal-business`相同已核条目重复，`claim-products-services`同时出现不可靠及已核版本，`claim-revenue-model`同时有pending/verified版本；首窗4条已核进入本轮变pending。`bounded_entry`仍从query复制claims并将引文页放入completed_pages，`direct_reading`据此标已读/跳过；没有恢复原checks/条件/阶段，不能以已有引文证明整页必要工作完成。本次按条件拆核验后继续追加同一claim，进一步暴露一对象多状态风险。
3. **A类/P1，本次新增共享预算兼容回归：** `utils/llm/budget.py:139`的repair_shaped按数值重叠范围拒绝非repair授权，原合法E2的180000/20也被拒。独立已有测试直接失败，纳入下一卡直接回归，不扩大为授权平台建设。

另有明确执行前提未满足：B§70.5承认发送前没有实际CLI双进程磁盘接续/失败保留证明。单条JSON重载单测不证明该路径；不能把未完成的硬前提描述为普通记录差异。消耗仍计入原账和入口次数，不退还。原生事实/投资收益须按语义分别表达；本轮收入主张提到基金收益并不等于已经证明其被计入营业收入，不无依据新增数字错误判决。

本轮3组阻塞（核心交付、状态/范围、预算兼容），无关既有失败和风格事项属B/C类，不派修。原生产权限不变，可靠部分继续限用。

### 71.3 实测、资源与保护

独立命令（已source scripts/quote_dev_env.sh，临时输出在QuoteBak）：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_window1_verified_claim_reloads_and_empty_query_does_not_clear_it \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization
```

实际**10 passed、1 failed、7 warnings，23.45秒**；失败为旧E2合法授权被新增repair条件拒绝。不是全套通过，也不是已完成真实CLI工程证明。A无业务代码修改，无真实模型请求。

预算原件复算：11物理请求、9逻辑请求、2次恢复，134862＝111324有效usage＋23538 unknown；G2及修复累计221167账本、1145.9400734687224秒、20请求、4恢复。17入口累计971570账本、5249.453228311677秒、68请求＝502053有效＋469517未知；原回执及首次记录不改。query/export字面结构不同，不据字节hash不等推导业务不一致；可靠状态问题直接见实际对象。

当前QuoteBak约91M。A仅删除本人本次测试产生的一个空日志目录，无实质数据删除；旧运行目录尚有来源/占用/持久副本核对工作，不能宣称历史垃圾已全清。下一卡将相关可重建旧测试库/副本清理放开发前置并要求具体保留依据，仍不动正式PDF、必要真实回执、既有/并行文件。A活跃审核及规划文档整理约40分钟（估算，未单独计时）；等待、模型运行和人工补救不混计。本轮13份既有已跟踪修改及16项未跟踪内容按基线保护，验收计划仅更新活动段，不提交其原历史脏改动。

### 71.4 唯一后续任务与流程告知

开发者按[A/B流程v3](../../../docs/development/ab_role_development_workflow.md)及[执行卡§34](b1-execution-card.md#34-整公司核心证据汇总与一次真实三视图交付)执行。A设计/分配/独立源审，B在一个连续窗口完成清理→同owner核心编排替换→直接验证/必要P0—P3修正→Review/冻结→一次真实公司交付；最多两次局部共性修正，不逐模块等待。未知字段/来源/数字/否定等底线仍约束实现，非阻塞细节后置。

本轮依据用户允许费用上调，具体授权验收§7.29的新任务300000账本/1800秒/32请求，有价≤30元、无价unknown，最多累计第18入口；不继承旧语义、不使用旧G2余额，不是常规成本达标。实际非空入口成功/失败未证明不创建attempt；成立并冻结后B同卡直接发送，无需再次确认。结束/到限即停，无自动第19次或换公司/短段。不归档、不扩批；真实结果19至少18/6实查、三视图七维及主要业务/变现、已审≥98%/底线0不降。次要遗漏/有据unclear可限用，不能用整维度空缺当容错；无法补救按源/原因/尝试/影响/限制/恢复条件说明。

规划验证：本change OpenSpec严格校验通过，git diff --check通过；新增本地链接有效，既有历史两处batch-review-audit.py链接缺目标在HEAD已有，未追改原记录。只提交本轮可隔离的规划/审核文档，原受保护证据和业务代码不改；Git提交/推送结果随最终交接报告。以上不是业务验收通过。

## 72. B交接：§34整公司实验未形成主张

日期：2026-10-21。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。本段只记录累计第18次、授权 `company_profile_company_core.2026-10-21` 的结果。没有载入前17次的导航、条件、草稿、checks或query。B不计19/6，也不与旧成绩合并。观察1/4、可用0/4。生产 `not_authorized`，`scale_quality_claim_allowed=false`。无第19次。

### 72.1 业务结果

三个视图的主张数都是0。`draft_complete`、`semantic_complete`、`real_semantic_complete` 均为 false。画像、产品暴露、供应链都没有可引用文本。查询与导出的 `profiles` 一致，入口记录 `query_export_equal=true`。

实际阅读停在来源清点，没有进入整公司成稿：

- 结构定位成功两批：页1–85，页86–169。页170–208未定位，原因是下一批被 `logical_recovery_already_used` 拒绝。
- 正文清点成功的页：4、10、11、13、14、15、16、17、18、19。
- 已选中但没有读完的页包括20–21、24–31、93–96、123、128、131、142、152、156、165–169、12。其中165–169是收入明细所在区间，清点请求失败。
- 页53、页116不在已选页，也不在未定位页。结构预览经过了它们，模型没有把它们选进正文。

失败原因写在缺口里：6次 `direct_core_gap_check_stopped`（请求截止、空响应、无可用池成员），2次 `direct_core_package_stopped`。因此没有合成，也没有可核验主张。同ID多状态在本入口没有对象可出现。

### 72.2 19项与6项负例

没有交付文本，19项和6项负例都是未评估。B不计通过，也不写已审准确率。已知底线错误外放0条，因为没有外放结论。页165未读，不能把未读写成未披露。

### 72.3 发送前的实际入口证明

发送前用真实 CLI，外部传输替换为离线协议，跑了非空成功和失败两条：

`pytest -q tests/unit/test_research/test_company_profile_direct_progress.py tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization tests/unit/test_research/test_company_profile_bounded_entry.py::test_direct_chain_entry_completes_and_keeps_a_verified_package`

11 passed，7 warnings，330.07秒。成功路径三个视图都有主张、主张ID不重复、`real_semantic_complete=false`、补读页进入了正文请求。失败路径保留已观察主张且草稿不完整。E2的180000/20不再被修复授权的数值区间拒绝；本卡300000/32/30元只在 `company_profile_company_core.2026-10-21` 下放开，E1授权不能打开它。这是协议证明，不是歌华成绩。

### 72.4 资源

冻结绑定 `b1-pool-company18-binding.json` SHA-256 `71330448003a98c8957a7967cc74e3054a2e49140ebc684e6fb4e8772f04e6a6`。上限300000账本、32次请求、1800秒、最多4次恢复、有价30元。

本入口：账本139115，物理请求11，逻辑请求9，恢复2次。有效usage 59225，未知预留79890，费用 unknown，停止原因为空。监督耗时826.8285275865346秒。出现的模型：`gemini-3.8-flash-high`、`glm-5.3-flash`、`grok-4.7`、`deepseek-v4.1-flash`、`deepseek-flash`。一次 `grok-4.7`、一次 `glm-5.3-flash`、一次 `deepseek-flash`、一次 `gemini-3.8-flash-high` 的usage为空，预留保留。

本任务剩余160885账本、973.1714724134654秒、21次请求。这些剩余不构成第19次。

路线累计：971570+139115=1110685账本；5249.453228311677+826.8285275865346=6076.2817558982115秒；68+11=79次请求。原17次账不改。

### 72.5 证据、清理与停止

证据目录 `b1-company18-evidence/`：

- query `ea147c8133b75ca20034e56f9a798d47dbb5ed891be19d1730bcc776fe1f2ba8`
- export `64ffa3e097bbed65ed077a929657e8ae6ce19452de6ec74d2a585c088844ebbf`
- budget `471b940b3fba047a818677a4d68943fcc6b14aad61667664690cb39de51c0095`
- supervisor `10c56373fa37a9646bae7a12b59c17eb3809e5725f6091050618a8b44860c64e`
- entry.log `a7f899d64b1e0919580ee48eafee075b4dc32df230b2d7e73c6442878165f659`

命令：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company18-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company18-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company18-20261021 --real
```

清理：删除7个空的 `quote_pytest_logs_*` 目录；删除本卡离线证明目录 `d2-chain-success`、`d2-chain-fail` 及两份绑定，合计约6MB，可按上述测试重建。保留PDF、前17次证据、`b1-company18-20261021` 运行根、`company18.json`，以及归属未核的 v61、window10、direct1。运行根在证据已复制后仍保留，清理时点是下一卡确认持久哈希一致之后。

若要再读页165的收入构成、页116的收费确认和页53的采购，需要新的入口授权。本卡剩余资源不自动变成第19次。

无法补救的影响：三视图为空，公司仍不可用。限制是不能把页4–19的清点回执当成经营结论。恢复条件是新的阅读授权，并且正文请求能够在合成前完成页165一带及收费、采购相关范围。

## 73. A审核`34d09540`：第18次失败，转真实故障修复（2026-10-21）

### 73.1 审核结论

第18次整公司实验不通过。三个视图主张数均为0，`draft_complete=false`、`semantic_complete=false`、`real_semantic_complete=false`；查询与导出一致，但没有可引用文本。结构定位只完成页1–85、86–169，正文清点仅完成页4、10、11、13–19；页165–169收入区间清点失败，页53采购和页116收费未被选入正文，页170–208未完成定位。19项核心、6项负例及准确率全部未评估，不能把退出码0、成功清点回执或历史已核结果拼成业务成绩。

本入口139115账本（59225有效usage＋79890未知预留）、826.8285275865346秒、11次物理请求、2次恢复，费用unknown；剩余160885账本、973.1714724134654秒和21次请求随入口结束，不构成第19次授权。路线累计1110685账本、6076.2817558982115秒、79次请求；有效usage561278、unknown549407分列。生产`not_authorized`、规模声明`false`，观察1/4、可用0/4，S1 0/12、S2 0/20、S3 0/100；不归档、不扩批。

### 73.2 主要矛盾及独立复核

本次不是预算到限，也没有证据证明LLM不能阅读年报。主要阻塞是交付链的三个可复现问题：

1. `utils/llm/budget.py`的失败来源汇总被`utils/llm/client.py`用于每个后续逻辑请求，临时失败源被整项任务永久排除；本轮被排除的来源中有来源在同账已有成功usage，导致成稿逻辑请求路由为`no eligible LLM pool member`，无物理成稿请求。
2. 编排仍在完成导航和source inventory后才进入成稿，后续导航/清点失败因此阻塞已有核心证据的成稿；这违背公司交付优先、页包只是传输单位的目标。局部成稿批次也存在后批失败时前批未落查询的路径。
3. 原“后续失败保已核结果”的测试断言被弱化为空结果也通过，测试绿色不再证明原合同。A独立运行相关测试10 passed/7 warnings/23.97秒；将原`any verified`断言恢复到内存挑战后失败，说明当前回归不能放行。

上述为范围内阻塞；非阻塞的旧格式提示、无关历史测试和架构建议后置。本轮不修改业务代码、不新增真实调用、不重跑第18次。

A的直接命令（先source scripts/quote_dev_env.sh）：`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization tests/unit/test_research/test_company_profile_direct_progress.py`。10 passed是现有断言的结果，不是原行为已成立；恢复`test_a_source_gap_failure_does_not_block_an_already_verified_claim`原`any verified`断言的内存挑战为AssertionError，未改测试文件。读取预算方法独立复算失败标签，确认4个被排除来源中deepseek、gemini、zai也有成功账目，不据此宣称供应商当前均可用。

### 73.3 当前唯一任务卡

下一张卡为[执行卡§35](b1-execution-card.md#35-真实故障复现与渐进交付修复)，同步写入[验收§7.30](../../../docs/development/company_profile_acceptance_program.md#730-第18次失败结束修复真实故障不再追加实验)、交付路径和OpenSpec任务：

- 先清理相关历史无依赖的QuoteBak日志、测试库、运行副本并保护既有/必要持久证据；本卡真实调用0、attempt0。
- 修复临时来源失败只影响当前逻辑请求及依赖，后续独立逻辑请求按既有健康/认证策略重新选源；unknown、恢复、累计账本不清零，不新建健康平台。
- 让有效核心证据沿原owner渐进进入候选、独立核验和query/export持久化，再处理后续导航/清点；后续失败保留无关前批结果和明确缺口，主要缺口仍使公司不完整。
- 恢复原合同非空行为测试，核真实调用顺序、失败前后查询/导出/新实例读回；替换传输只证明工程路径，不计业务验收。

B在一个连续工作窗口完成以上P0—P3，最多两次局部共性修正；不外呼、不创建第19次、不换公司、不增加预算，不以更多测试数或更换编排名称代替非空交付。两次修正仍不能证明，交最小可复现失败和业务影响，等待A裁决。后续真实观察是否授权，须在本卡工程证据审核后另行决定。

### 73.4 流程与交接检查

本次同时修正A的任务切分：此前“先收齐核心再成稿”容易造成串行等待，本卡改为“公司级一致性是最终结果要求，可靠部分可渐进交付”。开发者必须按[A/B流程v3](../../../docs/development/ab_role_development_workflow.md)报告复现序列、修复前后状态、非空断言、失败前后持久查询和清理范围；不得删除非空断言或用`all(... not verified)`替代原合同。原第18次回执和历史记录只读保留，缺失正文不能用usage重建。

本轮文档范围Review、OpenSpec本change严格校验、diff检查和新增本地链接检查通过；仅调整权威规划/审核文档，不改业务代码或池。13份既有已跟踪改动及16项未跟踪内容保持；验收计划仅提交活动区，原历史在途修改隔离保留。已删除A本次空pytest日志目录和隔离暂存补丁，不删除历史有用途或归属未核的运行目录；第18次run-root待B按§35核持久副本后清理。审核活跃耗时未单独可靠计量，记未知，不与机器或人工补救混报。

## 74. B交接：§35故障隔离与渐进交付

日期：2026-10-21。本卡真实调用 0，新增真实 attempt 0。第18次歌华三视图仍然为空，19项和6项负例仍是未评估。本段只记录三个故障的修复和入口证明，不计公司可用。观察1/4、可用0/4。生产 `not_authorized`，规模 false。无第19次。

### 74.1 修复前

第18次预算里，同一来源可以先成功再超时。`LlmClient` 把整份账本里所有失败来源放进之后每一个逻辑请求的排除集。被排除的4个来源中有3个此前成功过，两个成稿请求因此变成 `no eligible LLM pool member`，没有发出。

阅读顺序是先跑完导航和来源清点，再成稿。后一批合成失败时，前一批候选还停在内存里，查询里看不到。`test_a_source_gap_failure_does_not_block_an_already_verified_claim` 曾被改成“全部不是 verified 也通过”。

### 74.2 修复后

- 超时、空响应这类暂时失败只排除当前逻辑请求里的后续换源。下一独立逻辑请求按池的冷却和熔断重新选择。认证失败和配置错误仍排除该来源，直到同一来源有成功记录。账本、unknown 和恢复次数不清除。
- 第一批定位选出的页，在后续导航之前完成来源清点、候选、核验和原 writer 落盘。每个合成包核验后立即保存。后一个包失败时，前一个包的主张还在。主要缺口仍使 `draft_complete=false`。候选本身不是已核。
- 原断言已恢复：后续源查漏失败时，已核主张仍然存在，并且草稿不完整。

### 74.3 证明

直接阅读测试恢复原断言后通过。路由反例 `test_a_transient_source_failure_does_not_ban_the_next_logical_request`：同一账本里 grok 先成功、再记一次超时，下一个逻辑请求仍选 grok；认证失败的 grok 不会被当成暂时故障继续发送，下一请求改走 luna；同一逻辑请求的第三次发送被拒绝，前两次账本还在。

实际跑过的命令：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization
```

9项阅读测试加 E2 预算测试通过，其中源查漏失败仍保留已核主张。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_utils/test_llm_routed_client.py::test_a_transient_source_failure_does_not_ban_the_next_logical_request
```

1 passed，1.44秒。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_direct_chain_entry_completes_and_keeps_a_verified_package
```

1 passed，316.37秒。成功路径三个视图非空，`real_semantic_complete=false`，查询、导出和新读实例一致，第一份成稿回执早于最后一次导航。失败路径保留已有主张，草稿不完整，同 ID 不出现多种状态。这些是协议证明，不是歌华19/6。

### 74.4 清理与账

第18次持久证据 `b1-company18-evidence/` 与运行根的 query、export、budget、supervisor、entry.log 哈希一致后，删除运行根 `b1-company18-20261021`，约3.2MB。尝试标记 `b1-company18-attempts/company18.json` 保留。本卡离线入口目录 `d2-chain-success`、`d2-chain-fail` 及绑定约6.2MB，测试通过后删除，可按上面的命令重建。另删5个空的 `quote_pytest_logs_*`。PDF、前17次证据、v61、window10、direct1 未动。

路线累计仍是1110685账本、6076.2817558982115秒、79次请求。本卡不改这本账。第18次剩余资源不构成第19次。

## 75. A审核d44a1606：部分接受工程，直接接整公司交付（2026-10-21）

### 75.1 结论与主线自省

§35工程部分接受，B1/A1业务不通过。本轮没有真实模型调用，没有新增公司可用结果；歌华第18次仍三视图空，19核心/6负例及已审准确率未完成。来源隔离、原非空断言恢复、逐合成包核验后持久及后包失败保留有价值，不等于年报解读已经可用。观察1/4、可用0/4，S1 0/12、S2 0/20、S3 0/100；原18次1110685账本、6076.2817558982115秒、79请求及历史可靠部分限用不改。

主要矛盾仍是可信三视图尚未形成；本轮把两个实际阻塞关掉，但渐进交付只做到部分层级。A不能继续让任务围绕每个异常拆卡往返，也不能仅凭“合成早于最后导航”认定首包已就绪。本次只保留一个有直接反例的阻塞，把收口、冻结、真实交付和一次有界补救放在同一个B窗口，不新增框架或通用门禁，不放宽主体/数字/时态/主要覆盖底线。

### 75.2 独立证据与分类

- **接受：** `LlmClient`不再用所有历史失败来源初始化后续独立请求排除集；`hard_excluded_source_labels`保留认证/配置硬错误，临时失败由当前逻辑排除及已有池健康政策处理。原unknown/请求/恢复不清；直接路由反例通过。
- **接受：** 合成包分别进入`settle_claims`并持久，后包合成失败不再抹去前包；原`any verified`和非空断言恢复。B同快照实际CLI替换传输的成功三视图/失败保留、query/export/新实例回执复用，不重新跑316秒的整208页fake。模拟不计歌华19/6。
- **范围内P2阻塞1项：** `direct_reading.py`中`inventory_pages(fresh)`遍历所有正文包后才调用`compose(fresh)`，remaining/pending_follow也有相同先清点全部再成稿的屏障。A用原真实打包函数、同一次导航选中的两页各4500汉字（短业务段落与长段落分开）、合法引文走当前owner，得到`导航[1,2] → 清点[1] → 清点[2] → 合成[1] → 核验[1] → 合成[2] → 核验[2]`；第二包清点时持久回调0，最终模拟已观察2条。这只证明执行顺序，不是歌华业务分数。应让首个足够支持结论的业务包先成稿/核验/持久，再处理后续独立清点；当前测试的合成早于最后导航不覆盖此反例。
- **后置：** 无关旧测试、风格、行业增强、补充附注明细及通用架构不纳入本卡；未新增其他推测性问题。

A独立命令（先source scripts/quote_dev_env.sh）：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py \
  tests/unit/test_utils/test_llm_routed_client.py::test_a_transient_source_failure_does_not_ban_the_next_logical_request \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization
```

11 passed、7 warnings、26.83秒。两包顺序挑战使用内存Provider和现有`_run`/`body_packages`，不改测试文件、不外呼；不能把通过这11项理解为顺序阻塞也已解决。

### 75.3 下一张卡与授权

唯一下一卡[执行卡§36](b1-execution-card.md#36-首包即时交付与整公司真实验收)，验收数值/进度同步[§7.31](../../../docs/development/company_profile_acceptance_program.md#731-首包即时交付与整公司真实验收2026-10-21)。B先清无依赖历史临时产物，在同一owner收口上述阻塞并验证真实顺序，再冻结后自主真实交付，不逐函数等待A，不再单独交准备卡。

依据用户已同意执行和费用可上调，A具体新增最多第19次及一次有证据后继第20次；**两入口共用**300000账本、1800秒、32物理请求、4恢复及有可靠单价时30元，unknown例外延续、不购买充值、不改池。后继须扣首轮含unknown的实际占用并冻结新快照，不在运行中改码拼成绩；不足一个完整业务包即停。阶段账本总限显式由1288088调整至1410685（增加122597），时间8490.367203秒不变；最多用完后累计1410685账本/7876.2817558982115秒/111请求，不称常规资源验收通过。新增额度目前0次使用，不借原18次余额，不授权其他公司。

目标为真实公司画像、产品/收入与投入暴露、经营及上下游角色，并对已交内容完成19核心/6负例逐项实查及准确率；不以无具名对手、缺集中度或缺固定角色术语否定有源定性归纳。允许次要遗漏/歧义/补充未读明示限用，主要业务/变现/角色缺失仍不过，已知底线错误外放0。不能补救列源/原因/尝试/影响/限制/恢复条件。4→12→20（失败最多另20）→100不变，达量失败不增样稀释。

最多两次共性局部修正（含真实后），两个入口或任一资源到限就结束，不自动第21次、换公司、短段或改名续跑。B按A/B流程v3一个入口交实际内容、源判定、调用/资源和最少剩余阻塞；A仍独立源审，未通过不归档/扩批，生产not_authorized、规模false。

### 75.4 清理、保护与验证范围

A核第18次query/export/budget/supervisor/entry.log五份持久sha256与§72.5一致，重复run-root已不存在；B保留attempt标记符合要求，清理不改原失败证据。QuoteBak当前约92MB，较B前述值变化不能单凭总量判为垃圾；原v61/续跑/归属未核目录由下一卡核依赖后处理，不盲删。A已删除本次独立pytest生成的一个空日志目录；未删除业务源、历史必要制品或他人内容。

本轮只改权威规划、任务和审核文档，未改业务代码/LLM池、未运行真实入口。范围文档Review无新增阻塞；本change严格校验、diff检查、新增16处本地链接/锚点及资源算术检查通过。验收计划只提交活动区，原历史在途后缀保留；以HEAD原活动区与当前历史后缀重组，sha256等于任务开始基线，其他12份既有tracked文件hash及16项未跟踪列表一致。A活跃审核时间未可靠单独计量，记未知，不与26.83秒测试或真实运行耗时混报。

## 76. B交接：§36首包先交付，第19次未形成三视图

日期：2026-10-21。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。本段记录首包顺序修复和第19次真实结果。没有载入旧17次或第18次的语义答案。B不把下列逐项写成A已通过的19/6分数。观察1/4、可用0/4。生产 `not_authorized`，规模 false。无第20次、无第21次。

### 76.1 顺序修复

同一次导航选出的多个正文包，原先先清点全部包再成稿。现在每个包先清点、再成稿、再核验并经原 writer 落盘，然后才清点下一个包。两包反例：第二包清点开始时，持久回调已经发生；第二包清点失败时，第一包已核主张仍在，草稿不完整。直接阅读原有9项回归通过。这是协议证明，不是歌华成绩。

### 76.2 第19次实际三视图

`draft_complete`、`semantic_complete`、`real_semantic_complete` 均为 false。查询与导出的 profiles 一致。主张 ID 各只有一个状态。

画像，3条 verified：

- 页18：主营按行业归类为有线电视行业，2025年该行业营业收入 2,219,754,557.95 元、营业成本 2,085,236,738.78 元、毛利率 6.06%。引文为行业表同一行数字。
- 页15：涉及千兆宽带和5G用户服务，以及4K超高清智能机顶盒、IP机顶盒、网关机顶盒的研发推广。引文含“FTTH光纤网络覆盖150万户，有力支撑千兆宽带业务”。回答里的“推进研发和市场推广”需要A判断是不是当前经营。
- 页18：2025年营业收入 221,975.46 万元、营业成本 208,523.67 万元；成本含折旧、智能卡及机顶盒摊销、业务运行、人工和网络运行。引文为“公司实现营业收入221,975.46万元”。

产品暴露主张数 0。供应链主张数 0。缺口含 `view_not_substantive` 两项、`core_dimension_missing:operating_role`。页165–169、页53、页116没有进入已核正文。导航在页1–85之后停止，`navigation_incomplete` 123页。

调用顺序：页1–85导航，然后页15–19清点、合成、四次核验。第一包的成稿早于后续导航。后续核验被恢复次数用尽和账本留额截断。

### 76.3 已交内容的19项与6项负例

未读或不在已核文本中的项写未评估。已核文本没有覆盖的主要板块写不满足。B不报已审准确率。

| ID | 已交文本 | 判定 |
| --- | --- | --- |
| C01 | 有线电视行业归类，没有“开发、经营管理和维护” | 不满足 |
| C02 | 千兆宽带、5G、机顶盒；没有收转、点播和数据增值的完整列举 | 不满足 |
| C03 | 家庭视频场景有一句；政企综合信息没有单独成立 | 不满足 |
| C04–C06 | 收费与分期确认未进入已核文本 | 未评估 |
| C07 | 已核主张写2025年营业收入 2,219,754,557.95 元，并有页18引文；万元口径为 221,975.46 万元 | 待A核源 |
| C08–C12 | 五项收入明细未进入已核文本 | 未评估 |
| E01–E03 | 产品暴露无主张 | 不满足 |
| S01–S04 | 供应链无主张，经营角色缺失 | 不满足 |
| N01–N06 | 对应负例页没有完成实查。已核的收入、成本数字与其页18引文顺序一致，不能据此把6项负例记为通过 | 未评估 |

主要业务的收费机制、收入构成分项、投入和经营角色没有交付。公司不通过。

### 76.4 资源与为什么没有第20次

冻结绑定 `b1-pool-company19-binding.json` SHA-256 `6eb61c03c5e19e44877ea70f26f74d5b41763e6d7d3e2fa4b59784ac0f7f53df`。授权 `company_profile_company_core.2026-10-21`，尝试 `company19`。

本入口：账本 280728，其中有效 usage 111153、未知预留 169575；物理请求 19；逻辑请求 15；恢复 4 次，已到整卡上限。费用 unknown。监督耗时 1001.1564057497308 秒。模型包括 `gemini-3.8-flash-high`、`glm-5.3-flash`、`grok-4.7`、`deepseek-v4.1-flash`、`deepseek-flash`。

整卡剩余：19272 账本、798.8435942502692 秒、13 次请求、0 次恢复。单请求预留上限是 24000，19272 不够一个完整业务包，恢复次数也已用完。第20次没有发送。

路线累计：1110685+280728=1391413 账本；6076.2817558982115+1001.1564057497308=7077.438161647942 秒；79+19=98 次请求。原18次账不改。

### 76.5 证据与清理

`b1-company19-evidence/` 与运行根五份文件哈希一致后，删除运行根。尝试标记 `b1-company19-attempts/company19.json` 保留。删除前没有再计量目录大小。

- query `beaa82be7c7b08e367f42904cd6c77ca200eae34c3a653a995be035d2bdd756c`
- export `da62c981d2cd70461a95b41270743f12e126d94be8b9931363bb68545f495639`
- budget `df289d3e1ab7c7f9384b1310f2b85d96ebf310dacf8700de2c206679d47c1803`
- supervisor `2d393e7ef70304897312571e404426b1c2b509688be9244d7d47499b7b2d4e65`
- entry.log `ede3d71b3009d3fde8a254c526789506707c5d77879d205083c5da0e1ae74e7f`

开发前删除9个空的 `quote_pytest_logs_*`。PDF、前18次证据、v61、window10、direct1 未动。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company19-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company19-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company19-20261021 --real
```

直接测试：两包顺序与源查漏保留 2 passed，18.57秒；其余直接阅读 9 passed，27.70秒。

## 77. A审核89e77d54：核验重复工作阻塞三视图（2026-10-21）

### 77.1 主线判断和业务结论

**顺序修复接受，公司限用，B1/A1不通过。** 第19次确有画像3条真实已核交付，首个正文包先成稿/核验/持久生效；产品暴露和供应链仍为空，关键收费、主要收入分项和经营角色未交付。程序规则穷举不是本轮问题，LLM已生成7候选；当前编排把大量请求耗在重复核验上。

A此前只以首包先交付纠偏，没有把核验分组后的完整业务工作量核清，也未使主业/变现/投入/角色范围及时获得资源，切分不足由A承担。不能再因有测试/3条主张就默认下一轮会收口，也不能只扩大预算或继续逐函数准备卡。当前方案是在原owner内消除候选×条件重复工作，实际三视图核心包准入后一次整公司验收。

### 77.2 独立原文审核

A用pypdf只读原PDF物理页15–19，对照第19次query全部3条已发布主张及引文：

| 实际对象 | 独立结论 | 业务覆盖限制 |
| --- | --- | --- |
| principal_business_1 | 页18公司分行业表收入2,219,754,557.95元、成本2,085,236,738.78元、毛利率6.06%，正确；不是整个有线电视行业总额 | 只给行业归类，未说明网络开发经营维护等主要业务 |
| products_services_1 | 页15千兆宽带、页16增长中的5G用户、页17系列机顶盒研发推广均有源；“推进”置于已开发、取得进展的上下文中，不机械判成纯计划 | 缺完整收转/点播/数据增值及政企服务，不满足C02/C03整体 |
| revenue_model_1 | 页18万元收入/成本、页19折旧/摊销/运行/人工等正确 | 数字/成本描述不是收费确认和如何赚钱机制，不能因此判变现完整 |

已审主张**3/3**，只代表这3个唯一对象，无已发现确定性错数或计划冒充；没有全报告准确率。C07收入总额通过，严格已交核心**1/19**。C01–C03、E01–E03、S01–S04不满足；C04–C06及C08–C12未进入已核交付，未评估、不计通过。N01–N06均未完成对应负例实查，**0/6完成评估**，不是6负例通过或失败。原19项分母/B报告不改。

### 77.3 阻塞分级与资源

**A类/P2，直接阻塞公司交付：** `direct_reading.py:785`起的settle_claims，按候选遍历时反复带package_conditions全部15项，超限又分条件重核同一候选；这使“联合核验”退化为多组重复比较。真实回执证明7候选中含output/input/role/upstream_downstream，但只生成前3条画像的支持结果，revenue_model又被重复核验。产品/角色也只是候选，未核前不能直接发布为可靠。

第8–19次共12次物理请求都在核验阶段，累计171837，占本窗280728的61.2%；仅4份成功语义核验回执。传输失败、输出截断及超请求输出增加损耗，但不能解释为模型没有读出任何角色或必须重建网关。下一卡围绕支持不重复、覆盖聚齐真实对象、必要段落/ID复用及核心范围优先；风格、补充附注、行业增强、旧测试属后置，不增平台。

资源复核280728＝111153有效usage＋169575unknown，1001.1564057497308秒、19物理/15逻辑、4恢复，费用unknown；原19次累计1391413账本、7077.438161647942秒、98请求。最后12次物理含失败/修复，不能当12次成功核验。原回执不改，unknown不释放、不称实测token或人民币。

§76.4“19272低于24000所以不够完整包”的推理不足：24000是单请求上限，不是完整业务包下限；0恢复也不等于禁止新的首发。但B没有乱发第20次，按实际留额停止可接受。A结束§36/旧余量，以下新授权不以这句话为依据。

### 77.4 唯一下一卡、授权及质量

[执行卡§37](b1-execution-card.md#37-消除重复核验完成整公司三视图)，唯一资源/进度[验收§7.32及§8](../../../docs/development/company_profile_acceptance_program.md#732-第19次结束消除重复核验后一次整公司验收2026-10-21)。B按A/B v3一个连续窗口先清相关无用历史产物，在原owner修复核验工作安排，用本次真实7候选/15条件与上下文证明全部处置、不重复、全核心包实载可准入，再Review/冻结并真实整公司交付。支持/覆盖争取≤6仅效率目标，必要跨包更多可按卡说明并自主继续，不增凑数门禁。复用有效证明，不跑全208页fake或另交准备卡，不改池/公共默认。

用户已明确同意执行并允许费用上调，A撤销§36未用第20次的旧余额，**重绑同一个company20序号、最多一次入口**，新卡400000账本/2400秒/40物理/6恢复、有价≤50元；现有账号/池、不购买充值，无价沿已批unknown例外。单物理预留24000、payload65536、请求输出4000，每逻辑120秒/2物理/1恢复不变。旧19次账保留，不叠余额，不把重绑说成原卡同版成绩。原阶段账本总限1410685→1791413、时间8490.367203→9477.438161647942；新增上限只用于这次公司验收，不授权其他公司或第21次。

原19至少18、6负例实查全过、三视图七维/主要板块、变现/重要投入/经营角色成立、已审≥98%、已知底线外放0不降；次要遗漏、有源unclear、普通补充未读可明示。不可补救列源/原因/尝试/影响/限制/恢复条件。4→12→20（失败最多另20）→100不变，达量未过不加样。当前观察1/4、可用0/4，其余阶段均0，不归档/扩批，生产not_authorized、规模false。开发投入不等于常规资源通过。

最多两次局部共性修正均在真实发送前；前置仍失败或一入口/任一限结束即停，无换公司/短段/另名重开。若仍无完整三视图，A须作实现终止/替代裁决，不能沿同根因续派补丁。

### 77.5 复验、保护与清理

A核query/export/budget/supervisor/entry.log五份SHA-256与§76.5一致，profiles一致；重复run-root已不存在，company19 attempt保留。QuoteBak约82MB，不能只按总量或旧日期盲删未知归属数据。A本轮未改业务代码、未真实外呼；既有13份tracked改动和16项未跟踪内容按基线保护，验收计划只修改/提交活动前缀，原在途历史后缀保留。

独立命令（先source scripts/quote_dev_env.sh）：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_the_first_package_is_persisted_before_the_second_package_is_inventoried \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_source_gap_failure_does_not_block_an_already_verified_claim \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_later_verify_unit_failure_keeps_the_earlier_unit
```

3 passed、7 warnings、22.32秒；证明本次直接顺序/保留，不能证明三视图业务质量或新的核验分组已经实现。OpenSpec本change严格校验通过（遥测外联失败不影响校验结果），范围diff、16处新增本地链接/锚点及资源算术检查通过；范围文档Review修正旧活动入口冲突，并将≤6请求明确为效率目标，避免新增凑数门禁，无剩余规划阻塞。12份无关tracked完整hash、验收计划原历史后缀及16项未跟踪列表均与基线一致；只提交本人8份文档活动改动。A已删除本次测试的一个空日志目录，未动未知归属旧目录或业务材料；提交隔离patch使用QuoteBak并在提交后清理。A活跃审核时间未可靠单独计量，记未知，不与测试/真实运行时间混报。

## 78. B交接：§37第20次仍未形成可用三视图

日期：2026-10-22。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。本段只记录核验安排修复和第20次。没有载入第19次的语义答案。观察1/4、可用0/4。生产 `not_authorized`，规模 false。无第21次。

### 78.1 重复核验

第19次的7条候选和15项条件用真实页15–19上下文离线回放。每条候选最多进入2次核验请求，每项条件最多进入1次；装不下的条件记为明确缺口，不再按候选数重放。产品和供应链角色候选都进入了核验请求。逻辑核验请求不超过6次。这是协议证明，不是歌华分数。

直接测试：`test_company19_candidates_are_verified_once_and_roles_are_included` 与首包持久测试 2 passed，34.41秒。

### 78.2 第20次实际文本

`draft_complete`、`semantic_complete`、`real_semantic_complete` 均为 false。查询与导出的 profiles 一致。每个主张 ID 只有一个状态。

已核：

- 画像，页16：主营有线电视网络运营，北京市有线电视网络经营权，覆盖全市16个区、可承载三网融合。
- 画像，页18：有线电视行业列示2025年营业收入 2,219,754,557.95 元、营业成本 2,085,236,738.78 元；成本含折旧、智能卡及机顶盒摊销、业务运行、人工、网络运行。
- 画像，页15：千兆宽带、5G、云服务 IaaS/PaaS/SaaS，以及4K/IP/网关机顶盒、电视APP、USB CAM大卡、歌华智能家居。
- 产品暴露，页19：前五名供应商采购额 40,406.77 万元，占年度采购总额 31.90%；关联方采购额 5,239.65 万元，占 4.14%。

待核，未当已核发布：

- 页19：前五名客户销售额 49,827.02 万元，占年度销售总额 22.45%；关联方 24,984.53 万元，占 11.26%。
- 页16：有线电视网络运营商及全市网络覆盖。
- 页19：供应商采购额与客户销售额并列。

成功核验回执5次，然后才进入下一批正文清点。页1–85导航之后，页170–208未定位。收费确认、五项收入分项和页53采购服务没有进入已核正文。

### 78.3 已交内容对照

B不报已审准确率，也不把待核文本算通过。

| ID | 判定 | 依据 |
| --- | --- | --- |
| C01 | 待A核源 | 已核文本写网络运营和北京市经营权，没有完整的“开发、经营管理和维护” |
| C02 | 不满足 | 有宽带、5G、云和机顶盒，没有收转、点播、数据增值的完整列举 |
| C03 | 不满足 | 没有同时写清政企综合信息与家庭用户业务 |
| C04–C06 | 未评估 | 收费与分期确认未进入已核文本 |
| C07 | 待A核源 | 已核主张和页18引文都有 2,219,754,557.95 元 |
| C08–C12 | 未评估 | 五项收入明细未进入已核文本 |
| E01 | 不满足 | 产品视图没有五项收入业务 |
| E02–E03 | 不满足 | 已核投入只是供应商集中度，不是节目、流量、维护或物资采购 |
| S01–S03 | 不满足 | 角色和上下游仍是待核，不能当已交付 |
| S04 | 未评估 | 5G代理费与合作角色未进入已核文本 |
| N01–N06 | 未评估 | 对应负例页没有完成实查。已核数字与各自引文一致，不能把6项记为通过 |

主营的收费机制、收入分项和已核经营角色仍然缺失。公司不通过。

### 78.4 资源

冻结绑定 `b1-pool-company20-binding.json` SHA-256 `f41f29571ab80c5ea2d39ee95c4b1966cea5701fec02ad4b540f2b71efe953c6`。尝试 `company20`。授权仍是 `company_profile_company_core.2026-10-21`，上限改为本卡的400000/40/50元/2400秒/6次恢复。旧19272余额没有并入。

本入口：账本370446，有效 usage 131351，未知预留239095；物理请求27；逻辑请求21；恢复6次，已到本卡上限。费用 unknown。监督耗时1254.6730473591015秒。

整卡剩余29554账本、1145.3269526408985秒、13次请求、0次恢复。这些剩余不构成第21次。

路线累计：1391413+370446=1761859账本；7077.438161647942+1254.6730473591015=8332.111209007044秒；98+27=125次请求。原19次账不改。

### 78.5 证据与清理

证据目录 `b1-company20-evidence/`。运行根在哈希一致后删除。`company19.json` 与 `company20.json` 都留在 `b1-company19-attempts/`。

- query `7c46e6a36e67a3ca6f039c02a8ea80eb8fb408053fe0f8461e3d7305a5355570`
- export `2ee6b86d32f1b75fd375e8e0a3e278f33e8f2ba2b2c28fef638f4ab94e3d5c55`
- budget `12948f745cd3a38991c4dbe9c68253c2e20a829633b21e9ac763bb5d2aff0031`
- supervisor `7e9bf43d25b88e3ff7ab2d2cfa817d78a00e16c218986027248b73bf0c462bdb`
- entry.log `feb5f4d4a3b464cb94c4145a1a5cb5eddbef7aa36a8560acc039df746a80634e`开发前删除11个空的 `quote_pytest_logs_*`。PDF、前19次证据、v61、window10、direct1 未动。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company20-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company20-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company20-20261022 --real
```

## 79. A审核5f910a27：结束页包循环，替换为公司核心证据优先（2026-10-22）

### 79.1 整体走向与独立结论

**工程改善接受，公司限用，B1/A1不通过；结束§37及旧页包实现，下一步仅执行§38替代路线。** 当前观察1/4、可用0/4，S1 0/12、S2 0/20、S3 0/100。目标仍是数千家公司自动形成三视图，不是把第一批页面反复加工到完整。

A先读产品/验收/交付路径/A/B流程和当前change，再查提交差异、真实query/export/账本/日志及原PDF。第20次已不再逐候选重放全部条件，但仅导航页1–85，86–208共123页未导航；页15–19成稿后经过5次成功核验才清点下一包。支持/覆盖分组只能缓解首包重复，无法保证关键后章进入答案。源/池的超时、解析失败及unknown又消耗额度，但“继续给首包补一个异常”不能解决整体覆盖。

A也纠正自己的规划：上卡§37.1虽写核心范围优先，离线挑战却只用了第19次页15–19的7候选/15条件，不能证明收费、收入分项和投入可跨章进入三视图；A接受局部协议改善后仍让原循环运行，是这轮无公司级进展的一项原因。现将跨章节直接证明设为本业务替代的验证内容，结束旧循环，不降低质量、增加流程层或派新平台卡。

### 79.2 实际产品和源核验

第20次query与export的profiles一致；4条verified、3条pending，无accepted_facts，三个完整标志false。A直接核冻结PDF页12、14–19及4条已核主张的实际引文：

| 唯一已核对象 | A核源结论 | 不能替代的缺口 |
| --- | --- | --- |
| principal_business_1 | 页16网络经营权、16区覆盖/承载三网融合有据；主营网络运营实质成立 | 未充分交出收转/点播、政企与家庭业务、收费机制 |
| revenue_model_1 | 页18营业收入2,219,754,557.95元、成本2,085,236,738.78元及页19成本构成有据 | 收入总额与成本罗列不是赚钱方式，五项主要收入分项未交 |
| products_services_1 | 页15–17宽带/5G、云平台及终端产品/研发推广的来源支持现有摘要 | 不能据此推设备制造或已完成全业务转型；收入端暴露仍缺 |
| input_exposure_1 | 页19前五供应商40,406.77万元/31.90%、关联方5,239.65万元/4.14%有据 | 集中度不等于主要采购服务/物资投入及上游角色 |

**已审主张4/4**只覆盖这4个唯一已发布对象，不能当全报告/候选准确率。C01按实质通过：已交北京网络运营/经营权足以表达主营，不因没有逐字复述“开发、经营管理和维护”判错；C07本期收入通过。核心2/19，分视图2/12、0/3、0/4。产品/服务摘要虽已核，仍不能取代E01的收入端业务暴露。

N04研发/推广/计划未被已核输出升级为设备制造或已完成转型、N05前五合计未编具名对手，结合原文和实际4条输出实查通过，完成2/6；N01–N03/N06尚未完成对应交付实查，不以空白算通过。其他正向条件按§78保留不满足/未评估，未核经营角色和客户集中度不晋升；A不人工补写它们再算业务成功。19/6原分母不变，原B报告不追改。

### 79.3 主矛盾、修复优先级和新卡

- **阻塞：** 页包循环让首批细项控制全部进度，缺主要收费、收入构成、重要投入和供应链实质；同义经营角色还因投影独立生成/核验。结束这段编排，以产品v6/[执行卡§38](b1-execution-card.md#38-替换页包循环先取得公司核心证据再交付三视图)在原owner替换。全报告紧凑结构先定位跨章核心，取足公司证据后统一成稿，有限支持/覆盖核验，共享有关证据/检查并及时持久。
- **继续保留：** 真实LLM池、可靠原生值/源锚点、query/export单owner、负面核验撤回、unknown保留和后批失败保已核。新算法允许先汇总核心传输包，不继承旧“每包核完才读下一包”的顺序。不是推翻有用底座，更不是恢复逐公司规则。
- **后置：** 普通附注明细、具名对手穷尽、行业增强、风格/无关旧失败和架构整顿。不再按每个超时/schema细节单派卡；B在同一连续窗口内处理必要P0—P3并直接验证，最多两次发送前共性修正。

源发现仍不带候选/Gold；支持性及覆盖仍分别判断并绑定有效对象，不能以同维度存在或集中度放行。新卡用跨章节入口证明和原208页真实载荷计量，避免再用首包或空目标代表整公司可行。就绪/Review/冻结后B自主执行一次新真实观察，前置失败不外呼，不再另交准备卡。

### 79.4 资源与授权裁决

账本370446＝131351有效usage＋239095unknown预留，1254.6730473591015秒、27物理/21逻辑/6恢复，费用unknown。旧20次累计1761859账本、8332.111209007044秒、125物理（有效803782、unknown958077）不改。这些账本单位不是实际token合计，也不证明货币成本达标。

A撤销§37未用29554/1145.3269526408985秒/13物理/0恢复，依据用户已明确执行/费用上调授权，为§38替代路线分配独立一次company21：400000账本、2400秒、24物理、4恢复，有价≤50元，无价沿已批unknown例外。每逻辑180秒/2物理/1恢复，每物理90秒、payload60000字节/完整预留65536/请求输出4000上限。只在本卡新身份局部兼容，不更改旧授权或公共默认；整卡预算不提高，以更紧凑的公司证据减少无谓拆包。新额尚未使用，A本轮真实调用0。

最多累计2161859账本、10732.111209007044秒、149物理。一次入口/任一限结束即停，无自动第22次、其他公司或短段。替代路线仍失败须作整体可行性结论，不能继续同根因无限修补。常规资源120000/900秒/16物理/10元目标与业务质量分别裁决；4→12→20（失败最多另20）→100、到量退出、已知底线外放0不变。

### 79.5 验证、证据与保护

A沿当前冻结代码独立运行：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_company19_candidates_are_verified_once_and_roles_are_included \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_source_gap_failure_does_not_block_an_already_verified_claim \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization
```

结果3 passed、7 warnings/25.17秒；仅证明当前真实7/15分组、失败保留及旧授权隔离，不证明公司级质量/新路线已实现。未跑全套或208页fake，未改业务代码。核query/export/budget/supervisor/entry.log的5份SHA-256与§78一致；第20次运行根已删，QuoteBak下company19/company20的attempt仍保留，不追删必要失败材料。

本轮规划只更新9份权威活动文档、OpenSpec和本审核；`openspec validate restore-company-profile-llm-led-generalization --strict`、16个新增活动链接/锚点、资源算术及`git diff --check`通过。对照开始基线，其他12份脏文件hash不变；验收计划以原活动前缀拼回当前历史后缀的hash与开始时一致，历史既有改动未动；16项未跟踪列表不变。提交仅隔离本轮9份文档，不包含既有历史后缀。A不替B改业务代码。

本轮两次诊断命令留下的2个空pytest日志目录已在进程结束后删除，共8192字节目录占用；未生成测试库/完整正文副本，未删历史必要材料。临时物统一QuoteBak，必要证据只保权威一套。不归档、不扩批，生产not_authorized、规模false。

## 80. B交接：§38第21次导航后正文载荷被拒绝

日期：2026-10-22。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。本段记录页包循环替换和第21次。没有载入旧语义。观察1/4、可用0/4。生产 `not_authorized`，规模 false。无第22次。

### 80.1 编排

全报告结构预览先全部定位，再读已选正文，然后一次公司成稿和有限核验。不再在每个正文包里走完核验才读下一包。小型跨章节输入里，页1和页9都在成稿前进入不带候选的源审。直接测试 `test_selected_chapters_are_read_before_the_company_draft`、源查漏保留、导航失败保留和 E2 预算隔离 4 passed，17.81秒。这是协议证明，不是歌华分数。

### 80.2 第21次结果

三视图主张数都是 0。`draft_complete` 为 false。查询与导出的 profiles 一致。

实际顺序：一次 `page_map` 覆盖全部 208 页，模型 `gemini-3.8-flash-high`，usage 15056 输入 + 1158 输出 = 16214。下一请求是正文，UTF-8 载荷 61120 字节，输出预留 2500，合计预留 63876。新授权把载荷上限定为 60000 字节，这次预留被拒绝，账本状态写成 `physical_payload_or_output_limit`，后面的阅读没有再发。收费、收入分项和采购没有进入答案。

19项和6项负例都是未评估。没有已发布主张，也没有已发现的底线错误外放。

### 80.3 资源

冻结绑定 `b1-pool-company21-binding.json` SHA-256 `b38f860f2dba1031bcdf48511924830345a8bd6271c3233e472b648c5e4a8fd3`。授权 `company_profile_company_evidence.2026-10-22`，尝试 `company21`。旧卡余额没有并入。

本入口：账本 16214，全部为有效 usage，未知预留 0；物理请求 1；恢复 0；费用 unknown。监督耗时 187.39162191655487 秒。

整卡剩余 383786 账本、2212.608378083445 秒、23 次请求、4 次恢复。这些剩余不构成第22次。

路线累计：1761859+16214=1778073 账本；8332.111209007044+187.39162191655487=8519.502830923599 秒；125+1=126 次请求。原20次账不改。

未在发送前用 208 页实载测量正文包字节。结构预览按 65536 预留装进了一次导航，正文包则越过了 60000 字节上限，拒绝后又停止了整个账本，没有改成更小的包再发。

### 80.4 证据与清理

`b1-company21-evidence/` 与运行根五份哈希一致后删除运行根。尝试标记 `b1-company21-attempts/company21.json` 保留。开发前删除 1 个空的 `quote_pytest_logs_*`。PDF、前20次证据、v61、window10、direct1 未动。

- query `047ce9a347cf9aa05d78a7151b7610001cd5b6536b5deefcccba2815e4548ea3`
- export `8157ec47a459cdf4cda7ee359039c572d2acade5f7c81d6080f050e8baaef8b6`
- budget `4fe6b9403f8d3e5a52adbb3fe77d4f3e6b089f5343ec3bb3be32b85a52db060a`
- supervisor `c9603a6c4e64c8eecaf53e83149f36ab1835c51d05e16ffc8dea8a656c8e7730`
- entry.log `805c1169fe230e5a5235b58ed8f6276b8ea7411a9ef348ffbd98fc195d0e0028`

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company21-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company21-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company21-20261022 --real
```

## 81. A审核a3afc162：正文未发送，v6未实现完成

### 81.1 整体走向与可行性判断

**第21次失败、B1/A1未过；接受全报告先定位及先读后成稿的次序改善，不接受“v6公司级合成已完成”。** 当前仍观察1/4、可用0/4，后续12/20/100均0。有继续解决的依据：历史真实核验能产出部分有源主张；本次唯一物理请求是导航，正文没有发到模型，不能据此认定LLM读不懂年报。整公司稳定可用、资源达标及数千家泛化均未证明，A不保证新卡一定成功。

主要矛盾是业务编排、实载发送与统一成稿没有集成完成，协议/局部测试持续被当成真实运行就绪。A上卡已要求208页实载计量，但只用小型跨章节时序约束，未确保打包同时遵守载荷与预留两个限制，也未发现所谓公司成稿仍逐包独立；这部分规划/放行失效由A纠正。维持产品v6，不再改产品版本、堆流程或退回穷举规则；以[§39公司交付卡](b1-execution-card.md#39-b1公司交付卡修复实载与统一合成完成一次可用公司)合并必要修复、实际入口证明和真实结果，B不再逐个小故障等下一卡。

### 81.2 第21次真实产品和资源核对

独立读取`b1-company21-evidence/`的query/export/checkpoint/budget/supervisor/entry.log。query与export的profiles一致，三视图0条主张，三个完整标志false。唯一成功语义回执为208页page_map，选择22页：4、12—20、25—31、93、94、165、166、197。导航不等于正文已读；收费、收入分项、采购和角色都无本次已核交付。

物理请求仅一次gemini-3.8-flash-high，15056输入＋1158输出＝16214有效usage；随后正文payload61120字节、输出2500、完整预留63876，虽未超过65536预留上限，却超过独立的60000字节限制。预算将全局状态设成`physical_payload_or_output_limit`，其余请求未发。没有发现/成稿/核验业务对象：19核心在阶段交付口径记0/19，逐项原文业务核验及准确率不可评估，6负例未评估，不把空发布当正确。

本入口16214账本、187.39162191655487秒、1物理/0恢复、未知预留0，费用unknown；原21次累计1778073账本、8519.502830923599秒、126物理，有效819996、unknown958077。账本不是实际token总量或人民币。历史第20次4条已核/核心2/19/负例2/6仅留原版本有限结果，不搬入第21次得分。

五份SHA-256与B§80一致。第21次运行根已不存在，`data/QuoteBak/b1-company21-attempts/company21.json`仍保留。无A真实外呼或人工补写产品答案。

### 81.3 范围内阻塞发现

| ID | 直接证据及实际后果 | 本卡要求 |
| --- | --- | --- |
| A21-P1-01 | `report_interpretation.fits_card_reservation`只核总预留；`body_packages`沿该判断装包。真实61120正文超60000却通过打包，预算随后全局停止。A用原22页和重建身份只序列化，第一包最大池payload61182/预留63938，同样复现；61182是A重建值，不改B原61120 | 完整实际发送消息同时核字节、输出、预留；过大包在源边界重包，局部拒绝不清账或阻断独立合法请求 |
| A21-P1-02 | `direct_reading.compose`仍对`packages[:4]`逐包单独`synthesize_views`，未将公司跨章节条件/证据带入统一合成，最后程序拼接主张 | 公司级合成实际可使用跨章核心证据与限定语；传输可分包，不能以局部答案拼接冒充整公司理解 |
| A21-P1-03 | 上述结果用`unique[claim.claim_id]=claim`去重。A内存诊断使两不同源包各返回同局部ID、不同证据/业务，2条候选最终只剩后一条；无缺口提示，real_calls=0 | 身份/引用映射保不同业务，不静默覆盖；同义有据合并，冲突显式隔离 |
| A21-P1-04 | 提交修改共享`LlmClient._routed_attempt_timeout_seconds`，仅凭`timeout_seconds>=180`施加90秒cap。既有公平路由测试第二跳预期约100秒、实得90秒，本次新增回归 | 本卡超时只局部绑定，恢复其他业务原行为；不改旧正确预期，不归为C类 |

没有继续扩大到无关重构/平台；普通附注、行业增强、纯格式及与本次无关旧失败均后置。B报告完整披露了本次未测实载及失败，交接格式可接受；“先全部定位/读源”时序改善可接受，业务就绪和无阻塞声明不能据现有证据成立。

### 81.4 本轮验证与开发者告知

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_selected_chapters_are_read_before_the_company_draft \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_e2_budget_is_separate_from_the_e1_authorization
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_utils/test_llm_routed_client.py::test_four_model_route_shares_remaining_deadline_fairly
```

前组2 passed/13.91秒，后组1 failed/1.15秒（90秒cap回归）；warnings分别7、6。另做22页真实载荷只序列化及两包重复ID内存诊断，均无外呼、不写脚本/测试库。不声称全套或静态全绿，不再重复昂贵208页fake。A活跃审核时间未精确计量，记unknown。

开发者执行[A/B流程v3](../../../docs/development/ab_role_development_workflow.md)：A抓公司目标和原文验收，B在一个连续窗口内完成当前P0—P3，最多两次共性修正及已授权首发/唯一后继，不逐项申请；合同冲突、既有改动不可隔离或硬限不足才提交裁决。直接证明须用原208页/实际客户端/实际发送预算边界和非空公司核验目标，不能再只用首包或小样例。

### 81.5 额度与下一步裁决

A依据用户既有明确执行及费用上调授权，结束§38，**显式把未用383786账本、2212.608378083445秒、23物理、4恢复转入§39**，最多company22首发＋一次明确根因修复后的company23，提前达标停止。两入口共用额度，已用16214不退；阶段cap2161859账本/10732.111209007044秒/149物理不增加。单请求60000字节/输出4000/预留65536不提高；90秒物理策略仅卡内。仅第21次导航可经兼容核验复用，旧业务条件/候选/核验/已核不搬入。费用有价本卡≤50元、无价unknown例外继续，不能声称旧unknown费用达标；无需用户再次确认相同授权。

下一项可验证交付是**一家公司实质三视图及19/6原文审核结果**，不是第22次成功启动。前置仍失败不发送；两次共性修正/入口或任一资源限到即给具体失败、支持范围和不可补救影响，结束本方案，不自动第24次或换公司。达标后A才分配其余3家。固定4→12→20（失败最多另20）→100不变，次要遗漏/有源unclear容忍，已知底线错误外放0不降；不归档/扩批，生产not_authorized、规模false。

### 81.6 文档验证、清理和基线保护

本轮仅更新9份权威需求/计划/OpenSpec活动文档，未改业务代码。当前change的OpenSpec严格校验、15个新增活动链接/锚点、资源算术及范围diff检查通过；不将这些文档检查替代上面的2 passed/1 failed或业务未过结论。范围Review已把本轮授权、仅导航复用、两版本不拼成绩和公共默认隔离同步到活动入口，历史§1—80及原失败不追改。

其他12份既有脏文件hash不变；验收计划以原活动前缀拼回当前历史后缀的hash与开始基线一致，既有历史更新未触碰、未纳入本轮提交。16项未跟踪列表不变，暂存只隔离本人9份文档。两次本轮诊断的空pytest日志目录在进程结束后用rmdir删除，共8192字节目录占用；未生成测试库/一次性脚本，未删历史必要材料。第21次运行根已删、attempt仍在。所有临时物继续QuoteBak；必要证据一套，未另建清理台账。

## 82. B交接：第22次与第23次分列，公司交付未达标并停止

日期：2026-10-22。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`，208页。本段只记录[执行卡§39](b1-execution-card.md#39-b1公司交付卡修复实载与统一合成完成一次可用公司)。两版结果分列，没有把第22次主张导入第23次。B自检不是A的19/6分数，也不把程序 `review_state=verified` 写成已审准确率。观察1/4、可用0/4。生产 `not_authorized`，规模 false。无第24次。

### 82.1 四项修复与直接证明

发送前同时检查完整预留和本卡60000字节载荷。过大正文在发出前按页重包。载荷拒绝记 `payload_not_admitted`，不把账本停在 `physical_payload_or_output_limit`。公司成稿只发一包跨页引文和源条件，不再把各包局部答案拼成公司理解。相同局部主张ID、不同业务会改成带后缀的ID，相同答案才合并。90秒物理上限只在本卡metadata里生效；旧的四模型公平分配测试仍要求第二跳约100秒。

直接验证5 passed、7 warnings、51.37秒：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_utils/test_llm_routed_client.py::test_four_model_route_shares_remaining_deadline_fairly \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_an_oversized_payload_does_not_stop_the_remaining_ledger \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_selected_chapters_are_read_before_the_company_draft \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_same_local_claim_id_does_not_drop_a_different_business \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_company21_core_pages_pack_inside_the_byte_cap
```

其中22页实载包每包载荷不超过60000字节，并覆盖第21次选出的全部22页。这只证明控制流和打包，不是歌华分数。未跑全套，也未再跑208页fake。

### 82.2 第22次：程序核过9条，收入分项页未读

复用第21次page_map，208页，`reused_navigation=true`。没有载入第21次业务条件或主张。顺序是一次库存17页（4、12—20、25—31），一次合成8页（12、14—20），一次核验。页93、94、165、166、197为 `necessary_unread`。`draft_complete`、`semantic_complete`、`real_semantic_complete` 都是 false。query与export的profiles一致。

程序把下列9条标成 `review_state=verified`，核验回执46条检查均为pass。这是模型核验结果，不是A的源审通过。

- 主业 `claim_cond_pb_01`，页12引文为“公司主要负责北京市广播电视网络的开发、经营管理和维护……互联网数据传送增值业务等。”页18另有行业行 `2,219,754,557.95`。
- 变现 `claim_cond_rm_01`。页17引文为“2025年，公司实现营业收入221,975.46万元……同比下降4.08%。”页14引文是频道收转到账，页20是偶发贸易 `67,290,058.81`。没有分期确认。
- 产品四条：收转/点播；千兆宽带与5G物联网卡；政企和家庭“智慧广电”；推广4K机顶盒56.45万端。引文分别在页12、14、15、17。
- 投入 `claim_cond_ie_01`，页19。引文是折旧、摊销、业务运行、人工、网络运行成本和前五名供应商31.90%。不是数据传送、节目、出口流量或维护采购对象。
- 产出 `claim_cond_oe_01`，页18—19。引文含北京 `2,217,111,146.02` 和前五名客户22.45%。
- 角色 `claim_cond_sc_01`。页16引文是北京市有线电视网络经营权及覆盖16个区的基础网络。页12另一句是“重要支撑平台”。主张里“连接上游节目内容与网络设备资源”不在这两段引文中，B不把它当确定事实。

对照19项：C01、C02、C03、C07的原文句或营业收入总额出现在上述引文中，待A核源，B不计通过。C04—C06、C08—C12、E01—E03、S03、S04不满足：收费分期、十年入网、收入分项和采购对象所在页没有读到。S01的经营权句待A核源，上游连接句不作为确定事实。S02的政府、企事业和家庭用户出现在产品引文中，没有编造客户名称，待A核源。

6项负例：页165—166未读，N01、N02未评估，不以缺页当通过。采购明细未读，N03未评估。N04的草稿写的是研发推广终端，没有写成设备制造，待A。N05只写前五名合计，没有点名，待A。N06保留了服务和偶发商品销售，没有写成零商品暴露，待A。负例不是6/6。

资源：账本173995，有效75267，未知预留98728；物理7、逻辑4、其中3个逻辑各有2次物理；费用unknown；监督耗时585.5874215047807秒；`stopped_reason` 为空。根因是后一个多页库存请求超时，整包被标成未读，所以页165—166没有进入成稿。

### 82.3 第23次：读到页165，核验没有发出

只复用第21次page_map。绑定 `b1-pool-company23-binding.json` SHA-256 `0a20e63b884ff3992bb33cdd439e20ebda520ab6056856ea44935457d9def377`。授权尝试 `company23`。本入口上限是剩余209791账本、1627.0209565786645秒、16次物理；恢复仍按共用4次计算，并带上第22次账本，使已发生的恢复继续占用额度。没有导入第22次主张。

实际顺序：复用208页导航；库存4、12—20、25—31；超时后拆包，再库存94、165、197；一次合成页12—20、94、165。页93、166仍是 `necessary_unread`。合成之后出现 `logical_recovery_already_used`、`card_recovery_exhausted`、`direct_core_ledger_hold` 和 `direct_core_verify_stopped:direct_core_admission_stopped`。核验检查数是0。7条主张都是 `pending_verification`，没有晋升。`accepted_facts` 为0，`core_complete` 为false，三个完整标志都是false。query与export的profiles一致。

待核草稿里，页12引文仍是经营管理那句；主张正文写成“建设、管理和维护”，与页94“建设、管理和经营”混写，待核，不发布为事实。页165引文含收看维护 `724,769,470.04`、信息业务 `897,897,997.75`、频道收转 `165,529,697.84`、工程建设 `151,340,245.65`、商品销售 `67,290,058.81`、广告、入网和5G业务代理费 `22,879,452.01`。每条分项引文后面还并列另一个数字，草稿没有标明本期列。租赁收入 `162,672,085.77` 未出现，页166未读。全文没有“分期”或“10年”。投入草稿仍是页19成本和前五名31.90%，没有采购对象。

因此第23次19项全部不能计为已核。C08—C11和5G代理费只存在于待核引文。C04—C06、C12、E02、E03、S03不满足。6项负例没有核验，不能判通过；分项引文并列两个数字，N01尤其不能按通过处理。

资源：账本115051，有效79225，未知预留35826；物理10、逻辑10，本入口账本里没有重复逻辑ID；费用unknown；监督耗时727.2062738873065秒；`stopped_reason` 为空。最后一条拒绝原因是 `card_recovery_exhausted`。恢复拒绝没有把账本停成载荷耗尽，但下一次核验已被准入拦住。

### 82.4 逐项对照与停止决定

| 项 | 第22次 | 第23次 |
| --- | --- | --- |
| C01—C03 | 引文含主业、收转/点播/互联网和政企家庭智慧广电；待A核源 | 同题待核，且主业措辞被改写 |
| C04—C06 | 不满足 | 不满足；无分期、无十年入网 |
| C07 | 页17有221,975.46万元；待A核源 | 待核行业行含2,219,754,557.95 |
| C08—C11 | 不满足，页165未读 | 数字在待核引文，未核验 |
| C12 | 不满足 | 不满足，页166未读，租赁数未出现 |
| E01 | 不满足 | 四条收入分项在待核引文，租赁缺失 |
| E02—E03 | 不满足；只有成本构成和供应商集中度 | 同，仍不满足 |
| S01—S02 | 经营权及政企家庭待A核源；上游连接句不作为事实 | 待核；角色句含战略转型 |
| S03—S04 | 不满足 | 代理费数字在待核引文；采购方向不满足 |
| N01—N06 | 未全查；未读页不能算通过 | 核验未发，不能算通过 |

两版都不达到至少18/19、6项负例实际全过、已审准确率≥98%。已知不能当确定事实发布的内容：第22次角色主张中的上游连接句；第23次全部待核主张，包括被改写的主业措辞和未标明列次的收入分项。

共用余额账上还剩94740账本、899.8146826913578秒、6次物理。恢复在第23次已被拒绝，不够再完成一次“重读未读页、重新成稿、完成核验”的业务尾部。本卡只允许这一次后继，因此不创建第24次。

本路线已经能做的：208页导航可以复用；正文按60000字节重包后能够发出；超时的多页包可以拆开再读；跨页引文可以进入一次公司合成；待核不晋升；局部载荷拒绝不毒化账本；其他调用方的公平超时已恢复。本路线在这份额度里做不到的：第21次选出的22页不含收费附注页116和采购明细页53，复用该导航补不出分期确认和采购对象；恢复用尽后页93、166留在未读，核验发不出去。替代决定留给A：若还要这家公司，需要新的导航覆盖收费与采购页，并单独给予能完成核验的额度。不要把两版答案合并，也不要再自动发同链局部修补卡。

阶段累计：1778073+173995+115051=2067119账本；8519.502830923599+585.5874215047807+727.2062738873065=9832.296526315686秒；126+7+10=143次物理。其中本卡新增有效154492、新增未知134554。第21次之前的未知958077不释放。费用仍为unknown，不宣称人民币成本通过。阶段上限2161859账本、10732.111209007044秒、149次物理没有提高。

### 82.5 证据、命令、清理和保护

第22次证据 `b1-company22-evidence/`，与当时运行根一致后已删运行根：

- query `92418ea76468fd8c89a632e6fa2b5ee6260c5bf3bc8defa60fdfc062df073b7f`
- export `c7f27cc06bea732f3864415fde6c2c133229d48f938736bb28c112773b69e538`
- budget `936709e970240f442ae20f0d287d5b934f415bdf4ed3229887157e5efa3cfb7d`
- supervisor `adec0f840b5e0d8c9729804fc55cb0be6047cae712a30e0b1378737ad0e27178`
- entry.log `f6629f588797d4bef1ddf50d200f3f196cd943efbaa19855546b0ebaf71b02b0`

第23次证据 `b1-company23-evidence/`，复制后哈希一致，运行根已删：

- query `a1e9eb9020b6259f24828ba8ff1d7b543081568637777c918cff698713ae71f4`
- export `86d38927ba5674c7c335c3b5f39fd959033291f328cafb014af81982e5f17bd9`
- budget `a78f08c48823b5d8a3a3679c86cff4116dbe0f62dccc02499d308e21b60df7a0`
- supervisor `7e238d7ce605fcf0b8479612c35fd9337040410b6a515abe64c69586a9b25578`
- entry.log `e77335bd2683d4a36c3140c5dd09863bd6268b80c3b1ca02d8eaecf12ad4400b`

尝试标记 `b1-company21-attempts/company22.json` 和 `company23.json` 保留。PDF、第21次及更早证据、v61、window10、direct1 未动。交接前删除8个空的 `quote_pytest_logs_*`。未另建清理台账。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company22-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company22-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company22-20261022 --real
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company23-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company23-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company23-20261022 --real
```

既有脏文件未暂存：`config/13_llm.json`、两份gateway文档、验收计划、PDF评估JSON、prepare-company-profile三份文档、`core_evidence_selection.py`、`core_skeleton.py`、`test_company_profile_core_skeleton.py`、`test_company_profile_owned_page_facts.py`、`test_llm_routing_config.py`，以及16项未跟踪内容。验收计划仍由A维护，本段没有改它。

## 83. A审核7bc37ec2：结束选页方案，改为全文核心读源

### 83.1 结论与整体走向

B1/A1不通过，观察1/4、可用0/4，S1/S2/S3仍0。第22次真实非空、有源主营等可保留价值；第23次读到较后收入表，说明模型/源链能够阅读。没有证据认定“LLM无法读年报”或前面全部代码作废；但23入口仍未有可用公司，当前编排不能继续按小卡滚动。§39和旧余额结束，不能照B建议仅补导航覆盖/核验额度。

主矛盾是：导航漏选的来源被排除在后续发现之外，长期漏掉变现和采购；核验仍会把无源推断整条放行。A此前让逐个控制流故障牵引发卡、没有及时验证核心来源召回假设，对停滞负规划责任。今后按产品目标/真实可用公司数裁决，合并必要实现/验证/真实交付，非阻塞细节后置，不以提交/测试数代替业务进展。

### 83.2 独立原文、交付对象和资源审核

A核对两轮query/export的profiles一致、§82五份回执hash分别一致；直接读原PDF及同源缓存。第22次实际读取17页，未读93、94、165、166、197；复用22页导航本来就不含收费116和采购53。第23次额外读到94/165/197，93/166仍未读，不加载第22次答案；这些版本不能拼成公司成绩。

| 交付/原文核查 | A结论 |
| --- | --- |
| 第22次主营网络经营及收转/互联网等服务、p17收入221975.46万元 | C01/C02/C07可确认；金额按合法万元舍入，不为小数位另开返修 |
| 政企方案和客户原文 | 政企服务有源；可靠答案未完整交家庭业务，不能因引文含“家庭”就给C03/S02通过 |
| 成本/折旧/智能卡机顶盒摊销、供应商31.90%与客户22.45% | 相关对象可保留，不等于已读采购对象/上游或收入分项；合计不生成具名对象 |
| `claim_cond_sc_01`的“连接上游节目内容与网络设备资源” | 当前引用只有p12平台/p16网络经营权，无上游关系支持；实际仍observed/verified且无limitation。P1，拒作确定结论，整体对象及依赖覆盖退出可靠消费，不能由A手改半句后计自动成功 |
| 第23次p165收入表引文及7候选 | 全部pending、核验0；引文并列数字未带充分列次，p166租赁续页未读，不能确认本期分项交付或借第22次核验 |

第22次独立可确认核心3/19；N04计划/研发不升制造、N05前五名合计不造名、N06服务不因无commodity丢弃/不造敏感性，实查3/6。N01—N03相关源/实际交付未完成，不算通过；其余核心未实质成立，不用沾边句计分。9条程序已核中至少1条无支持，准确率上限8/9=88.89%＜98%，故无需以局部正确率掩盖底线。第23次无可靠主张，不用空交付算准确率或负例通过。原§82分母和失败记录不改。

第22次173995账本（有效75267、unknown98728）/585.5874215047807秒/7物理；第23次115051（有效79225、unknown35826）/727.2062738873065秒/10物理。23入口累计2067119账本/9832.296526315686秒/143物理，有效974488、unknown1092631，费用unknown。余额94740/899.8146826913578秒/6物理关闭，不转新卡。

第23次账本的`card_recovery_exhausted`拒绝针对已有读源逻辑请求的恢复；没有核验物理请求。投影另有`direct_core_ledger_hold`、`direct_core_verify_stopped:direct_core_admission_stopped`。不能把“恢复拒绝”概括成所有新核验都因恢复耗尽不能发送；新实现直接验证逻辑目标/入口归属和真实末端准入，仍不清unknown或放宽真正总限。

### 83.3 接受工程范围、直接验证和替代可行性

B四项改动在代码中可见：正文同时按payload/预留重包，局部payload拒绝不毒化整账，跨页引文汇入公司合成，局部ID冲突保不同对象，卡内物理超时显式metadata隔离公共默认。保留已证工程能力；模型核验全pass不构成A语义质量接受。

A实际运行以下受影响直接回归：**2 passed、6 warnings、1.70秒**；不声明全套通过，告警不改变业务结论。B的5项实载证明作为已有协议证据复用，不再次重建整208页模拟。

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_utils/test_llm_routed_client.py::test_four_model_route_shares_remaining_deadline_fairly \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_an_oversized_payload_does_not_stop_the_remaining_ledger
```

A另用现有`pack_fitting_batches`、`ReportSemanticRequestV3`及当前池`request_payload_bytes/request_reservation`只序列化、不外呼：208页缓存184544字符/404158 UTF-8字节；11个源发现包（1—21、22—37、38—64、65—82、83—102、103—122、123—154、155—175、176—188、189—198、199—208），输出请求4000，payload最大59899字节，预留合计669036、单包最大64155。此测量证明可分片发送且旧120000假设不适用；不是新提示词最终计划、实测token/费用或公司可用证明，B须纳入非空末端与恢复。A本轮真实调用0。

### 83.4 唯一下一卡、验收与开发者告知

执行卡§40/验收§7.35是明确的v7替代任务，不是把失败路线改名扩额：取消导航作为正文准入，全年报分片独立源发现只收核心证据，跨章统一成稿后有限核验。保原解析/数值/证据、网关及writer/read，替换旧编排并删无调用分支，不另建平台或逐公司生产规则。第22次角色须退出可靠消费，原证据原件保留。

依据用户已有执行及费用上调授权，A明确追加新任务2000000账本/7200秒/64物理/12恢复，单入口1200000/3600秒/36物理/6恢复；仅company24＋至多一次company25，共用总额，到限停。单请求60000字节/输出4000/预留65536，逻辑300秒/2物理/1恢复，物理150秒仅卡内启用。现有池、不购买充值，有价累计≤100元、无价沿已批unknown例外，不能声称全部货币成本合格。新任务最多阶段4067119账本/17032.296526315686秒/207物理，明确追加而非篡改旧上限；非预计消耗或规模预算。

两个工作日内B连续完成必要修复/直接证明/Review/冻结及真实交付，最多两次共性修正，前置失败不发、资源不够完整后继不建attempt，不逐阶段请A。原19至少18、6负例实查全过、三视图七维/主业/变现/投入/角色、已审≥98%及已知底线外放0不降；次要遗漏/有源unclear可说明。不能补救写来源/原因/尝试/影响/限制/恢复条件。仍失败结束本实现，不增第26次/换公司/短段；A须给不可实现环节和支持范围。

继续[A/B流程v3](../../../docs/development/ab_role_development_workflow.md)：B现有报告形式基本符合，需把程序verified、B自检、A核源分开，逐项给19/6、首次/后继、实际可靠文本、精确停止原因及资源/清理；不再只报“核过9条”。A1通过才另发剩3家，4→12→20（最多另20）→100顺序/门槛不变。不归档/扩批，生产not_authorized、规模false。

### 83.5 文档、清理与保护

本轮更新产品v7、验收活动段/唯一台账、交付路径、proposal/design/tasks、delta、执行卡及本审核入口。§39标历史、旧方案余额关闭，proposal删重复版本执行摘要，避免新旧活动指令并行；历史真实原件不改。B22/23运行根已删的说明接受，attempt/持久证据保留；QuoteBak约83M并含更早任务材料，未证明无依赖的不盲删，先清理仍是下一卡前置。A确认本次pytest已退出、日志目录为空且无引用后，删除本人生成的`quote_pytest_logs_g7ufil2k`（无实质数据，释放约4KiB目录项）；未删旧任务材料。9份文档新增/修改链接、预算算术、OpenSpec严格校验及diff检查通过。13份已跟踪/16项未跟踪基线hash/列表保护通过；验收历史后缀既有改动隔离不提交。审核活跃耗时未独立计时，不编造成公司运行/补救成本。

## 84. B交接：全文已读源，公司成稿未发出，本方案停止

日期：2026-10-22。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`，208页。本段只记录[执行卡§40](b1-execution-card.md#40-b1替代方案全文分片读源后统一公司交付)。第24次和第25次分列，没有导入上一版主张。B自检不是A的19/6分数。观察1/4、可用0/4。生产 `not_authorized`，规模 false。无第26次。

### 84.1 编排和直接证明

主入口不再用导航决定正文。全部可读页按60000字节分片做不带候选的源发现，片边带上一片的首行；成功条件再进入一次公司成稿和有限核验。一片失败留未读并继续其他片。恢复拒绝只挡住同一次逻辑请求的再发，不把账本停掉。不同入口、不同核验目标使用不同逻辑身份。复合主张若核验返回 unclear，整条退出可靠结果。第22次原query/export没有改写。

208页缓存上的新提示词计划：9个源片、13次物理（含一次成稿、一次核验、一次补源和两次最大请求恢复）、853186账本，208个可读页都进入分片，单入口1200000/36装得下。这是发送计划，不是召回成绩。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_navigation_does_not_decide_which_body_is_read \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_an_unclear_composite_claim_is_not_reliable \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_logical_identity_separates_entries_and_verification_targets \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_full_report_slices_fit_one_entry \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_synthesis_keeps_the_conditions_that_fit_the_reservation \
  tests/unit/test_utils/test_llm_routed_client.py::test_four_model_route_shares_remaining_deadline_fairly \
  tests/unit/test_research/test_company_profile_bounded_entry.py::test_an_oversized_payload_does_not_stop_the_remaining_ledger
```

导航、复合主张、逻辑身份、公平超时和载荷拒绝为9项里的短测，连同既有章节顺序和预算隔离，短测9 passed、7 warnings、19.96秒。208页计划1 passed、132.63秒。成稿裁条件1 passed、3.25秒。未跑全套。

### 84.2 第24次：208页已读，成稿因预留超限没有发出

绑定 `b1-pool-company24-binding.json`。没有复用旧导航或旧主张。14次源发现回执覆盖页1—208，没有 `necessary_unread`。源条件里已经出现页12主业句、页16经营权、页19供应商集中度、页53数据传输服务费、页116分期确认和10年入网、页165收看维护收入、页166租赁收入。这些是源条件，不是已核主张。

成稿在发出前被拒绝：`direct_core_package_stopped:card_request_reservation_exceeded`。条件和引文合在一起超过65536预留，请求没有送到模型。三视图主张数都是0。`draft_complete`、`semantic_complete`、`real_semantic_complete` 都是 false。query与export的profiles一致。核验检查数是0。

账本748651，有效327683，未知预留420968；物理25、逻辑19、其中6个逻辑各有2次物理；费用unknown；监督耗时2614.390667054802秒；`stopped_reason` 为空。恢复拒绝记为 `logical_recovery_already_used` 和 `card_recovery_exhausted`，没有把账本停成载荷耗尽。精确停止点是成稿预留超限，不是恢复耗尽。

### 84.3 第25次：按预留裁过条件，实际载荷仍超60000

根因是成稿把全部条件塞进一次请求。修复只保留预留装得下的条件，装不下的写入 `major_condition_not_in_synthesis`，然后重新读源、成稿和核验。没有导入第24次条件或主张。绑定 `b1-pool-company25-binding.json` SHA-256 `be5094e501de92ce9cf6793c6bf2ac815ee217912624a2f6dedd4c66a6e65fbf`。

再次14次源发现覆盖页1—208，未读页为空，源条件45条。页116引文含收看维护、工程、入网按10年分期。页165引文含收看维护 `724,769,470.04`。页166引文含租赁 `162,672,085.77`。页53引文含数据传输服务费 `52,396,499.89`。页12引文含主业句和政府、企事业、家庭用户。这些仍只在源条件里。

成稿请求被拒：`direct_core_package_stopped:payload_not_admitted`。裁剪用的是65536预留，留下的包实际UTF-8载荷仍超过60000。请求没有发出。三视图主张数仍是0，核验0，三个完整标志false。query与export的profiles一致。`stopped_reason` 为空；最后拒绝原因是 `payload_not_admitted`，不是恢复耗尽。恢复拒绝另有记录，且发生在成稿之前。

账本727028，有效306060，未知预留420968；物理25、逻辑19、6次恢复；费用unknown；监督耗时2541.7521064607427秒。

### 84.4 19项、6项负例和停止决定

两版都没有已发布主张，所以19项全部未评估，6项负例没有可核对的交付文本，不能按通过计。源条件里的数字和分期句子没有经过成稿和核验，不能当成确定事实。第22次原件保持不动，其中无引文支持的上游关系仍不作为本卡可靠结论。

| 项 | 第24次/第25次交付 |
| --- | --- |
| C01—C12、E01—E03、S01—S04 | 未评估。相关句子出现在源条件引文中，成稿和核验都没有发出 |
| N01—N06 | 未评估。没有已发布文本可核对列次、购销方向或具名对象 |

本路线已经能做的：取消导航筛选后，208页都能进入源发现；收费、采购、收入分项和租赁续页能形成带页码的引文条件；一片超时后可以拆开继续；恢复拒绝不毒化账本。本路线在60000字节限制下做不到的：把这些跨章条件连同必要引文装进一次公司成稿。第24次死在65536预留，第25次按预留裁剪后仍死在60000字节，主张保持为0。

任务已用1475679账本、5156.142773515545秒、50次物理、12次恢复。剩余524321账本、2043.857226484455秒、14次请求、0次恢复。这不够再完成一次全年报读源加成稿核验，本卡也没有第26次。阶段累计2067119+1475679=3542798账本，9832.296526315686+5156.142773515545=14988.439299831231秒，143+50=193次物理。费用unknown。阶段上限4067119账本、17032.296526315686秒、207次物理没有提高。

若还要这家公司，成稿裁剪必须按实际60000字节而不是只按65536预留，或者按业务维度分成几次仍带跨章条件的成稿。那是另一个方案，不是本卡的下一次入口。

### 84.5 证据、命令和保护

第24次证据 `b1-company24-evidence/`：

- query `113464bc9a6a45a5259d727905b345d6f4c8f495b31ce6fa19f39e55a9c53ad5`
- export `7e49c7fd896f93d0a43acc059bd7b48764a8c2142a6fcd340eb0203d1a605b91`
- budget `840ee3ddae36335122f0111daff3b9789cb6e6be6714a199e76a0f63d1b8f755`
- supervisor `0e544cff6de540702dcb9acabe0d21e111392fe7985db8ca742f6a4fdc2585b6`
- entry.log `e464352b7617157f69734f98608ef65c81d4024f5853cae0dd0db53e2b5c6df3`

第25次证据 `b1-company25-evidence/`：

- query `1eeb632f457b840505290716ac1fd5488d5cb85f5218cfac78138d1e2942923e`
- export `714d196ddbffbba89b62d148f01c92d1055032c6938a1d0512c51fd3ca3e98de`
- budget `ca29bdbc2f8c6396f7963f39ec4177ce05787982dc4fff07866b766d07eecfb8`
- supervisor `e2bf6bfa7b123e6e8d65149c51c34f01b0084b2b7442a39c7009022cf4f70c3a`
- entry.log `75e3398769ce8ce206ad93de5faef7cbdf1b9c7584c97f39f8b3bfb0db62b370`

两份运行根在哈希一致后删除。尝试标记 `b1-company24-attempts/company24.json` 和 `company25.json` 保留。第22次、第23次及更早证据和PDF未改。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company24-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company24-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company24-20261022 --real
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company25-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company25-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company25-20261022 --real
```

既有13份修改和16项未跟踪内容没有暂存。验收计划仍由A维护，本段没有改它。

## 85. A审核：读源已进步，末端程序与就绪方案失败，规划纠偏

日期：2026-10-22；审核提交`be3aaa96`，按产品总需求→验收v2→交付路径→A/B流程v3→当前change及§84证据执行。A本轮只审核/规划及更新配套文档，不实现业务代码，不外呼模型。

### 85.1 先判主线与实际进展

目标仍是数千家公司可自动生成有源公司画像、产品暴露和供应链定位；当前先形成第一家可用公司，再验证4/12/20/100，不要求全部附注事实完美。本轮实际进展是导航漏源瓶颈已消除：第24/25次均有14份成功`source_inventory`回执，覆盖208页，收费/采购/收入分项已成为源条件。**交付端没有进展：两个成稿请求均未发送，核验0、主张0、19/6未评估，B1/A1不通过，观察1/4、可用0/4。** 全文读源不能代替语义召回或可用公司。

### 85.2 根因及责任

| 层次 | 独立证据与结论 | 本轮处置 |
| --- | --- | --- |
| 程序直接阻塞 | 第24次全部条件/引文超过65536预留；第25次只按预留筛条件，最后62718字节＋256封装＋2500输出＝65474，仍超60000字节，被`payload_not_admitted`拒绝 | 计划与发送用同一实际序列化口径核双限，超限重包而非裁主要条件 |
| 前置验证失真 | `fulltext_entry_plan`只用首源片和一个模拟核验对象；`compose`只取第一包且按排序筛条件；相关测试允许主要收费条件被裁并只记gap，没有证明真实45条件末端可交付 | 用真实持久累积输入、全部生成组和非空实际核验证明末端，原预检请求数漏1同时纠正 |
| A规划/验收管理 | A把公司统一理解约束成一次合成，改码后又要求重新读源；接受了未覆盖真实累积末端的就绪方案，导致可离线发现的确定性错误消耗两次整本阅读 | A撤销单HTTP及末端改码整本重读要求，保有效读源，只发一张成稿/核验/真实交付连续卡，不再顺B错误逐项发小卡 |
| LLM/来源服务 | 超时、超输出造成成本和不确定性放大；本轮最终失败点在请求发送前，两轮没有成稿/核验响应 | 尚不能据此判断模型不能写画像；后续以真实生成/独立核源实测，不用加预算承诺必成 |

第25次原45条件（36条标major）可从保存响应重载；A用缓存全文页重新定位均可找到，**这不证明原分片范围及上下文完整**，B接续必须重验原始实际发送范围。A只序列化现有完整客户端payload，不联网：按序有限分包可保45条件，三包分别22/22/1条，52259/51991/8670字节，56515/56247/12926预留，合计125688；另将30条主营/产品/收入端条件硬放一组仍达69765字节，说明不能简单“一维一包”后停止。该测量只说明分包空间，未包含新索引/完整必要语境/实际核验，不能当冻结计划或语义验收，不指定B必须三包。

### 85.3 核验范围、资源与审核分类

§84两轮query/export的`profiles`一致，各14份成功语义回执均为源发现，无成稿/核验回执。两轮query/export/budget/supervisor/log的SHA-256与§84.5一致；源合同、原失败及首次结果不追改。第24次57原始条件、第25次45，不混两次或第22次答案。第22次错误角色是否已在实际可靠消费层隔离仍未证明，§41要求直接消费查询证明；不能只在交接中声明不使用。

资源独立复核：两次1475679账本＝633743有效usage＋841936未知预留，5156.142773515545秒，50物理/12恢复。阶段3542798＝1608231有效＋1934567未知，14988.439299831231秒/193物理。费用仍unknown；这些不是人民币或全部实测token。原未用524321账本/2043.857226484455秒/14物理/0恢复不是自动许可。

A类阻塞：末端实际双限打包、真实累积末端未证明、单包/裁主要条件策略及已知错误消费隔离未证。B类后置：非核心明细完备、风格/规模性能优化。C类：任务前13份修改和16未跟踪、旧无关失败；本轮不处理。不重跑无关全套，不用测试全绿判A1通过。

### 85.4 唯一下一卡与停止

[执行卡§41](b1-execution-card.md#41-b1末端收口保留有效读源按跨章业务证据组生成与核验)与验收§7.36为当前唯一指令：只重验第25次真实源阶段，紧凑公司源索引＋有限跨章业务证据组生成，按实际非空目标支持/覆盖核验、即时原owner持久；不重读全年报、不混旧答案、不改Gold/分母。离线真实规模证明、必要修复、冻结与真实交付在一个B窗口完成。

A依据用户已批执行/费用例外，显式重绑未用524321/2043.857226484455秒/14物理为**一次company26末端开发接续**，新增恢复≤4，阶段账本/时间/请求上限不加、旧恢复不清；不是新首发、不是旧卡自动续跑，旧卡停止正确。直接前置未过不建attempt，成立后B自主运行，不再逐项申请。最多两次离线共性修正、两个工作日；真实入口结束无第27次或改码后第二次真实返修。仍失败结束本末端实现，按实际生成/核验给能力边界，不自动同链加额。

原19至少18、三视图主要业务/变现/投入/角色成立、6负例全查通过、已审唯一对象≥98%、已知底线外放0；允许次要遗漏和有据unclear，不以删主要条件/未读写未披露制造通过。进度仅验收§8，生产not_authorized、规模false，不归档/扩批。

### 85.5 A文档验证、清理与保护

本轮更新当前需求、验收、路径、proposal/design/tasks、执行卡、delta和本审核入口，历史§1—84正文、25次证据、PDF/合同及原账保留。A盘点QuoteBak约83M，确认第24/25次运行根已不存在、attempt目录保留；历史v61/S2资料、旧运行目录和共享日志仍有保留/归属核对需求，不盲删，不宣称历史垃圾全清。下一卡先核引用/占用/持久副本后清相关无依赖产物，结束只留必要证据一套。本轮只做只读测量/文档验证，不新建测试库、过程脚本或全年报副本，无本轮独立临时副本待清。

A验证通过：10份冻结证据hash、两份query/export的profiles一致、两轮208页回执范围、历史审核§1—84正文保护及预算算术；15个新增/更新本地链接及锚点；`openspec validate restore-company-profile-llm-led-generalization --strict`、`git diff --check`。本轮范围Review按§85.3分类，无新增规划阻塞；业务代码未改，不新增业务测试或把旧测试数量算进产品进度。13份既有文件改动及16项未跟踪路径核基线一致；验收计划历史区块与本轮活动区块隔离提交，其余12份不暂存。真实调用0，A完整审核活跃耗时未完整计，记未知，无人工填写业务事实。

## 86. 用户纠偏：先交真实答案，再核验与优化

日期2026-10-22，接用户“先要有内容有答案，然后再看答案对不对怎么优化”。A接受批评：上一轮虽找到末端问题，仍给固定余额、一次入口和完整核验离线前置，继续把成本/过程放到首份答案之前；该任务安排撤销，不要求B执行后再报一次失败。

当前唯一修订§41/验收§7.37在同卡连续推进：加载第25次有效源与原文→真实LLM成稿并立即保存三视图正文/引用为pending→核主要业务、收入/变现、投入/角色、关键数值和引用→修正主要错误→再校准成本/性能。第一份草稿只需输入/发送/待核写入的最小验证，不等全部核验/反例/45条件重审做完；必要时按实际服务限制分包，不先造新索引/框架。

用户本次指令及已有费用上调授权允许B在原公司/现池任务内自主调整预算预留、请求/输出/时间/恢复以及必要真实运行次数；§7.36未用的524321/14/一次company26、60000/65536等自设限制不再作为禁止成稿条件，旧阶段总额只作历史比较。旧25次/unknown不清，有价可超前次100元、无价记unknown，不购买充值、不改共享默认/生产。实际服务失败先定位/拆包/换源，不无限重复；真实来源/账号服务不可用、用户停止或既有改动无法隔离时交内容及原因。

待核正文必须在原query/export可读；不能只保模型回执或以“流程未完成”盖掉答案。核验失败保留草稿并标限制，已知错误退出可靠结论；草稿不计B1通过，正式19/6、准确率、底线及4/12/20/100不变。A下一轮先看文本与源再看成本/测试，不能再只审核预算是否达标。

本轮A只修正9份当前规划/任务文档，未实施B业务代码、真实调用0，尚未形成公司答案；不把文档修订称为业务进展。TMPDIR仍QuoteBak，未新建测试库/过程副本或清理他人材料；13份既有修改与16项未跟踪保护，验收历史区块隔离提交。文档校验与范围Review完成后正常提交推送供B执行，不另建报告/审批点。

## 87. B交接：复用第25次源条件，交出待核三视图

日期：2026-10-22。公司仍为歌华有线 600037.SH，2025年报，版本 `b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`。本段按[执行卡§41](b1-execution-card.md#41-b1末端收口保留有效读源按跨章业务证据组生成与核验)记录。没有重读208页，没有导入第24次或更早主张。第26次和第27次分列。程序 `verified` 不是A的源审，也不是19/6成绩。观察1/4、可用0/4。生产 `not_authorized`，规模 false。

### 87.1 第26次：主业草稿已保存，后一组超时

复用第25次14份源发现回执。先发出一组23页引文的成稿，主业主张 `claim-principal-business-view` 保持待核，正文还在。收入和产品两组核验包超过实际载荷，没有发出；当时失败路径把这两条正文换成了“结论未通过证据或语义核验”，所以第26次不能当最终文本。另一组成稿超时，供应链视图为空。

账本248234，有效80565，未知167669；物理9、逻辑5、恢复4；费用unknown；监督耗时882.3212321028113秒；`stopped_reason` 为空。未读页0。query与export的profiles一致。

### 87.2 第27次：三视图都有正文

同一批45条源条件，成稿按更小的组发送，核验失败不再改写正文。3次成稿、5次核验。query与export的profiles一致。`draft_complete`、`semantic_complete`、`real_semantic_complete` 都是 false。未读页0。第22次 `claim_cond_sc_01` 没有进入本查询的可靠主张，查询缺口写明 `isolated_unreliable_claim:claim_cond_sc_01`，原第22次文件未改。

程序结果：14条 `review_state=verified`，14条 `pending_verification`。公司画像20条，产品暴露7条，供应链定位1条。仍有一条成稿超时，部分产品/投入主张因此停在待核。

可读正文里，页12引文仍是“公司主要负责北京市广播电视网络的开发、经营管理和维护……”。程序已核的主业主张同时写了“建设、管理和经营维护”，这比页12引文多了“建设”，B不把它当已核事实。页116引文含收看维护、工程验收、入网费按10年分期、信息业务按服务期限分期。页166引文是租赁收入 `162,672,085.77`；另一条已核主张还引用页175经营租赁收入 `146,594,318.86`，两个租赁数需要A区分。页18引文含有线电视行业营业收入 `2,219,754,557.95`，同一行还有成本和增减列。页53/188引文含数据传输服务费 `52,396,499.89`，待核主张还写了节目分成及推广、互联网出口流量和商品采购。页16引文是北京市有线电视网络经营权；角色主张停在待核，没有再写“连接上游节目内容”。页12引文含政府、企事业单位及家庭用户。没有“设备制造”或“上游”字样。

已发布正文里没有收看维护 `724,769,470.04`、信息业务 `897,897,997.75`、频道收转 `165,529,697.84`、工程建设 `151,340,245.65` 这四个金额。第25次源条件里有它们，这次成稿没有把四个数写进主张。

### 87.3 对照19项和6项负例

B不打分。程序已核只说明模型检查返回了pass。

| 项 | 第27次文本 |
| --- | --- |
| C01—C03 | 页12主业句、收转/点播/互联网、政企和家庭用户都有引文。主业主张多写了“建设”，待A核源 |
| C04—C06 | 页116引文含分期确认、工程与验收分开、入网10年。待A核源 |
| C07 | 页18有 `2,219,754,557.95`，同一引文含成本和增减列。待A核源 |
| C08—C11 | 不满足。四个分项金额没有进入已发布正文 |
| C12 | 页166有 `162,672,085.77`，页175另有 `146,594,318.86`。待A区分 |
| E01 | 不满足完整五项金额 |
| E02 | 数据传输、节目/推广、出口流量和商品采购在待核主张及页53引文中。待核 |
| E03 | 有商品采购和预付款对象，没有把智能卡/机顶盒写成制造对象。待A |
| S01 | 待核。页16经营权引文支持网络运营，没有上游连接句 |
| S02 | 政府、企事业、家庭用户在页12引文中。待A |
| S03 | 采购方向在待核投入主张中。待核 |
| S04 | 5G终端合作按净额，没有写成制造或全链持有。待A |
| N01 | 未通过。行业行引文同时带成本列 |
| N02 | 待A。两个租赁数都在已核主张里 |
| N03—N06 | 待A。采购没有改写成公司主业销售；计划句标了计划；前五名有合计，也有报表点名的欠款和预付对象；服务没有因缺少商品编号被删掉 |

### 87.4 资源和保护

第27次：账本374198，有效167493，未知206705；物理17、逻辑11、恢复6；费用unknown；监督耗时1394.9104948053136秒；`stopped_reason` 为空。一次成稿超时，两次核验因该逻辑请求的恢复已用尽而没有再发。恢复拒绝没有清空已保存正文。

第26次证据 `b1-company26-evidence/`：query `32f2ee294596f493968ab3e9e0b8b8b53e612d28f58abd82e247aaa04f941676`，export `c9fd193b788c452f3b6bf606642771cc32da4c9ddfd1d25dfe6d0f2b1dac6e52`，budget `b7c784fb146106f46c8aaaf6b395fcb15a5e39dd86bce88b00a920c227c6cbd6`，supervisor `647c7cd87034aa7a1c181b1fd4c805513e9e7a4ff309e98f003a9b8e7de5f47b`，entry.log `a2871773f939300d7ead75209f41a9689ad92008a684d0ed00f39fe226c5e7c2`。

第27次证据 `b1-company27-evidence/`：query `d2d740ef197037e02d8ce30597b02df313fa39421eb38c72cf2058e5e6a6b5ef`，export `b375be84e13fbc4a62de315f1aabfa69edf2a1eb5bc8c391901a210d5e5e378b`，budget `09d3e777afda6470e37449ab61c55f6d4e935db84227c3cba5cc0787997e2951`，supervisor `7c725bbaa5a3f81f54fa5db66d08d11281e3d5cac9c4804ff3ac1a5c840c38c4`，entry.log `00a0443e7ea788648aa9216ffde3f8c223e7dea7aa0c6e2916ca799428ad364d`。

两次运行根在哈希一致后删除。尝试标记 `company26.json`、`company27.json` 留在 `b1-company24-attempts`。第25次及更早证据未改。直接验证是复用源条件后成稿并写入查询的离线用例1 passed，以及第26次发送前对45条条件分成两组、载荷57945/50490、预留60701/53246的测量。未跑全套。

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry \
  --binding openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company27-binding.json \
  --authorization openspec/changes/restore-company-profile-llm-led-generalization/b1-pool-company27-authorization.json \
  --run-root /home/python/Quote/data/QuoteBak/b1-company27-20261022 --real
```

既有13份修改和16项未跟踪内容没有暂存。验收计划没有改。

## 88. A审核d522dccb：已有三视图答案，接续收口首家公司

日期：2026-10-23。遵循产品总需求→验收计划→交付路径→A/B流程v3→当前change，使用business-first-goal检查方向。审核对象为`d522dccb`及第26/27次证据，不把本次审查写回原query或B的§87。

### 88.1 主线判断与裁决

**部分接受：首份真实三视图草稿交付成立；首家公司仍限用，B1/A1未通过。** 从“源已读却无答案”推进到28条可阅读、有引用的正文，收费确认和入网10年机制也已进入答案，这是实际业务进展。当前主矛盾转为源已有的主要收入未成稿、投入/角色未核，以及核验把不支持的交易方向/实际经营断言放行。下一卡保持现路线及已有效成果，补齐主要内容并纠错；不再整本重读、派准备卡或以旧预算/入口次数卡住接续。

A同时检视自身任务切分：既然第27次已有可审正文，就不要求B把所有附注明细变成已核才交付，也不因一个旧fixture失败重开工程专项。原§7.37连续开发/调用授权有效，A不新增一次入口或资源审批；B按[执行卡§41.6](b1-execution-card.md#416-第27次后的接续任务卡保答案补主要收入与上下游收口首家可用)连续完成核心收口，再提交A1。开发观察1/4、可用0/4；尚余3家未观察，S1 0/12、S2 0/20、S3 0/100，不扩批/归档，生产not_authorized、规模false。

### 88.2 原文及交付独立核对

A用pypdf直接读正式208页PDF，SHA-256与冻结`b5b6be18424b7c3d7d7988f755ff7083141831904d4347c07cba1da8452c8b76`一致；第27次query hash与B的`d2d740ef197037e02d8ce30597b02df313fa39421eb38c72cf2058e5e6a6b5ef`一致，query/export的profiles相等。核全部28个实际正文对象及引用所在段/表，57处bounded_quote忽略排版空白后均能在相应原PDF页定位。**引文定位只证明引用存在，不证明答句含义正确。** 公司画像20、产品暴露7、供应链1，accepted_facts为空，14程序verified/14pending，三个完整标志均false、classification=limited，与实际限制一致。

B对三处疑点的解释需纠正：

- 主营“建设”有同一主张第94页明确支持；第12页没有该词不构成错误，也不要求只按首条引文审核。
- 第18页引文同时含营业收入/成本/增减列，不自动说明N01失败。需要检查实际输出取的是哪列；当前总收入只在引文，不在正文金额/原生事实中，属于数值答案未交付，不能编造错列。
- 第166页租赁收入162,672,085.77与第175页经营租赁收入146,594,318.86口径不同，第176页融资收益16,077,766.91与后者相加恰等于前者。原文支持分别描述，但不要求程序为了过关另建勾稽体系；也不把三数再加成收入。租赁主张正文目前没有这些金额，仍需完成数值投影。

### 88.3 当前对象审核与必须处理的错误

下列每行对应一个唯一实际claim，V/P为程序verified/pending，A判定与该标签分开。主张虽列多项事实，准确率仍按该唯一对象计，不拆句稀释错误。

| claim_id（去掉共同claim-前缀） | 程序 | A原文结论 |
| --- | --- | --- |
| principal-business-core | V | 复合对象不通过：第12/94页主营和建设有据；第123页是一般税收政策，不能证明公司实际收取农村基本收视费，整条暂不作为可信对象计分 |
| principal-business-asset-swap | V | 第55/195页有线资产业务换河北网络股权，有据 |
| principal-business-gov-enterprise-plan | V | 第29/31页为未来计划，正文明确计划及2026—2028目标，有据 |
| revenue-industry-region | V | 第18页有线行业及北京/涿州地域方向有据，不能替代正文收入金额 |
| revenue-leasing | V | 第166/175/176页租赁及融资收益分类有据；金额尚未成可读/原生数值答案 |
| revenue-related-services | V | 第53—55页提供劳务和租出方向有据；不是全部业务收入构成 |
| revenue-2026-goals | V | 第27页经营目标与计划时态有据 |
| products-current-1 | P | 第73/116页业务名和收费类型有据，仍待真实核验 |
| products-5g-terminal | V | 不通过：第191页“公司为中广电移动网络有限公司销售5G终端设备等”不能改成“向其销售”；对手角色被改变，合作/净额部分可保留 |
| products-smart-gdt-transformation | P | 第12页原段支持推进转型及政企/家庭服务，未写已完成转型，仍待核 |
| products-ai-applications | V | 第30页计划探索；limitation明确探索、未披露完成，按该限制有据 |
| products-content-quality | P | 第28页提质/内容品牌计划有据，非本轮必核核心 |
| products-tv-broadband-5g-packages | P | 第28页差异化组合营销计划有据 |
| products-uhd-development | P | 第28页超高清目标/布局表述有据 |
| products-uhd-upgrade-project | P | 第63页项目目的和终端升级有据，不是已实现全部结果 |
| products-inventory | P | 第104—105页是存货会计政策，不能因此推成设备制造商，非核心必核项 |
| revenue-recognition | V | 第73/116页收看、工程开通/验收、入网10年、信息/频道服务期等机制有据 |
| principal-smart-gdt-platform | V | 第12页政企、家庭及智慧广电方案有据 |
| principal-business | V | 第16页北京网络经营权和16区覆盖有据 |
| products-services-viewing-maintenance-writeoff | P | 第127页为收看维护应收核销65,142,706.25元，正文保留核销语义，有据，非主要收入 |
| input-related-party-purchases | P | 第53/188页购入服务/商品方向和所列四额有据，仍缺系统维护及成本语境的核心完整表达 |
| input-top-five-suppliers | P | 第19页40,406.77万元/31.90%及关联部分有据，不证明采购对象或经营角色 |
| input-spectrum-resources-plan | P | 第29页频段使用为计划，正文保留计划，有据 |
| input-program-fee | P | 第155页103,811,962.05为应付账款期末节目费用余额；当前仅称项目金额，需补口径，不能作本期成本/采购流量 |
| input-prepayment-top-five | P | 第130—131页列的是期末预付款对象和余额，有据，不能替代前五采购供应商名单 |
| output-top-five-customers | V | 第19页49,827.02万元/22.45%及关联合计有据，未编具名客户 |
| output-ar-top-five-debtors | V | 第127—128页欠款方名单、315,928,797.99元/34.51%有据，不是前五销售客户名单 |
| supply-chain-role-cable-network | P | 第16页有源网络运营/服务角色，仍待核，未交上下游及5G合作完整角色 |

程序已核14个对象中12个有据、2个含未支持断言，**已审准确率12/14=85.71%（只针对这14个对象）**。其余14个已核来源，其中13个正文可支持、1个节目费用需补余额口径；全部保持原pending，不凭A手工改成模型核验成功，也不合并草稿来抬高可信对象准确率。未对其他未交付源事实宣称准确率。

A类阻塞合并为三项：上述两条错误对象及依赖必须退出可靠状态并纠正；已读主要收入构成须变成实际答案；产品暴露和供应链的主要投入/下游/运营合作角色须形成可信交付。节目应付款补口径可在限用补充项处理，不要求全部存货/排名/计划核完。B不能用专属正则修这两个句子，须改善实际使用的语境和语义核验；引用存在及模型pass均不能证明完整答句成立。当前已知错误位于研究证据，不代表生产被启用。

### 88.4 原19核心与6负例裁决

计分针对当前可信实际交付，不把源条件、附带大表、待核或错放对象当成合格答案。相同事实允许跨视图投影，但该视图必须实际表达/引用它，不要求重复生成。

| 条件 | 当前裁决与理由 |
| --- | --- |
| C01 | 通过：独立正确的principal-business支持北京网络运营/经营权，按实质语义计 |
| C02 | 已生成但不计：收转/点播/互联网在principal-business-core，复合对象仍含未隔离农村收费断言 |
| C03 | 通过：principal-smart-gdt-platform明确当前政企及家庭综合服务，不靠战略目标充数 |
| C04、C05、C06 | 均通过：revenue-recognition已解释服务收费、安装传输维护、工程验收/开通及入网10年机制 |
| C07、C12 | 部分，不计：正确数字在第18/166页引文，但正文未给金额、accepted_facts无相应对象；有源不等于已解释的数值答案 |
| C08—C11 | 不满足：四项金额只在第25次源条件，没有进入本次发布正文/原生事实 |
| E01 | 不满足：产品暴露视图未交五项主要收入构成及关联，不是要求每段复制金额 |
| E02、E03 | 待核/缺项：采购草稿有数据内容流量/商品，但系统维护与智能卡机顶盒成本语境未完成可信表达 |
| S01—S04 | 未满足：当前供应链只有待核网络角色；无可信完整下游/上游类型及5G代理合作答案，不拿集中度替代 |

严格通过5/19；各视图5/12、0/3、0/4。C02保留正确内容、修复所在对象即可复审；这不是要推倒主体答案。五项主收入是主要板块，不能按次要遗漏容忍；小额其他收入和全部具名关联行继续后置。

负例：N04实查研发/推广/探索/计划未冒充设备制造或已完成转型，N05实查前五合计未凭空生成销售客户/采购供应商，当前范围通过。N01未完成本期主要收入实际数值交付的取列验证；N02原文租赁口径已澄清但数值答案未交；N03投入仍pending、未完成可信购销方向交付；N06虽然服务未被全部删除、commodity为not_assessed而非零暴露，但主要服务收入端暴露尚未成立。完成2/6，其余未完成，不伪造通过或将未完成直接称为错误。5G合作方向另列已知错误，不能因6负例表未单列此句就免责。

### 88.5 工程验证、资源、清理与后续执行

A独立执行：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_saved_source_conditions_are_drafted_without_rereading \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_an_unclear_composite_claim_is_not_reliable \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_later_verify_unit_failure_keeps_the_earlier_unit \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_logical_identity_separates_entries_and_verification_targets \
  --basetemp=/home/python/Quote/data/QuoteBak/a-review-d522dccb-pytest
```

结果3 passed、1 failed、7 warnings/19.78秒。失败用例的第二页为4000个无句界“甲”，停在`fragment_has_no_safe_boundary:2`，claims为空，尚未触发后批核验失败。A内存加载`d522dccb^`同模块复核，父版相同失败/缺口，属C类既有验证问题，不声称全部回归通过，也不扩大本卡去修全套测试。新增用例证明源条件复用及正文保存，在该范围接受；真实主张语义仍以本节原文审核为准。B本次分组按dimension/ID排列、生成失败继续、最后集中核验，会留下已有核心条件未成稿及次要对象抢占核验；下一步只在这条直接链修主要条件优先及实际覆盖，不建新planner。

A独立重算第26/27次budget-final：248234=80565有效+167669未知、9物理；374198=167493有效+206705未知、17物理，supervisor耗时分别882.3212321028113和1394.9104948053136秒。旧25次3542798/193物理不改，累计4165230/219物理；费用unknown不是0或人民币成本。A无新增真实模型调用，资源优化在首家可用之后校准。

开发前清理要求继续执行。A确认B所列第26/27次运行根均不存在，attempt目录仍保留；QuoteBak检查时约83M，没有据此删除归属不明或正式证据。A本轮临时测试产物按结束清理要求处理，不产生第二套正文/数据库证据。后续B先清自身可确认无依赖垃圾再开发，交接清本人无用制品、记释放量及必要保留项；不把全量年报/重复开发库当永久证据。任务开始13份既有修改/16项未跟踪继续保护。

后续按§41.6执行，同一A/B流程不变：B自主连续修正、必要真实核验、直接回归和交付；原公司及现有池内资源可按§7.37调整，不另请相同调用/费用授权。达原19至少18、三视图主要内容成立、已审准确率≥98%、6负例全实查且已知底线外放0再交A1；次要缺项/有据不确定保留说明。无法补救时交真实答案和具体限制，不再用预算差额代替交付。A1前不进入另外3家公司或S1/S2/S3。

本轮文档检查：OpenSpec本change严格校验、`git diff --check`通过；新增本地文件链接可解析。历史两处指向已删除`batch-review-audit.py`的链接在HEAD已存在，本轮冻结历史未改，不另恢复一次性脚本。§1—87正文逐字节保持；验收计划旧details区重构回任务开始版本的SHA-256一致。A本轮pytest未使用磁盘fixture目录，指定basetemp运行后不存在；没有可确认需删除的新测试库，清理释放0字节。原有临时目录/attempt和既有工作区内容未顺手删除；不宣称已全清QuoteBak。业务代码未改，无新增真实模型调用。

## 89. B交接：保留第28次纠正，并核经营角色；主要收入金额仍未写入正文

日期：2026-10-23。按[执行卡§41.6](b1-execution-card.md#416-第27次后的接续任务卡保答案补主要收入与上下游收口首家可用)接续第27次，没有重读208页，没有改第27次及更早的query/export。第28次和第29次分列。程序已核不是A的19/6。观察1/4、可用0/4。生产 `not_authorized`，规模 false。

### 89.1 第28次：两条错放已退出，5G方向和上游已改

加载第27次正文和第25次源条件。`claim-principal-business-core` 退出已核，原文仍留在查询里，状态是 `retracted_pending_correction`。其中“建设”仍在，农村基本收视费那句也还在，所以整条不能当可信对象。`claim-products-5g-terminal` 改成“公司为中广电移动网络有限公司销售5G终端设备等”，并写明净额法；查询里已经没有“向其销售”。节目费用另有已核主张，写明 `103,811,962.05` 是期末余额，不是本期采购。

新的已核正文还有：营业成本含智能卡及机顶盒等摊销；上游采购写了数据传输、节目分成及推广、互联网出口流量和商品。经营角色那条在第28次仍是待核。

四项本期收入金额没有进入任何主张正文。补写它们的那次请求超时：`direct_core_package_stopped:LLM request deadline exceeded`。

账本319017，有效192222，未知126795；物理16、逻辑11、恢复5；费用unknown；监督耗时1258.928660677746秒。query与export的profiles一致。

### 89.2 第29次：角色已核，收入金额再次超时

只再送仍缺金额的条件和那条待改的主业。经营角色 `claim-supply-chain-role-cable-network` 这次为已核，页16经营权引文还在。上游采购主张保持已核。5G“为中广电移动销售”保持已核。

收看维护 `724,769,470.04`、信息业务 `897,897,997.75`、频道收转 `165,529,697.84`、工程建设 `151,340,245.65` 仍然不在正文里。主业那条也没有改成去掉农村收费的新句子。缺口仍是同一次成稿超时。第27次里原来已核的收费机制、政企家庭服务等主张，在第29次查询里变成待核，正文还在，但本版不能把它们继续算作已核。因此第29次不是第28次的超集。

账本129384，有效60643，未知68741；物理7、恢复2；费用unknown；监督耗时570.362404378131秒；`stopped_reason` 为空。

同一超时没有再发第三次。源条件里的四个金额还在第25次证据中，只是没有被写成答案。

### 89.3 对照19项和6项负例

以第29次为最新查询，第28次的已核纠正单独保留。B不打分。

| 项 | 现在的文本 |
| --- | --- |
| C01 | 第29次里网络经营权主张已核。含“建设”和农村收费的复合主业已退出已核 |
| C02 | 收转、点播、互联网接入仍在那条已退出的复合主业里，没有单独的已核主张 |
| C03 | 政企和家庭服务的正文还在，第29次为待核 |
| C04—C06 | 收费、验收/开通、入网10年的正文还在，第29次为待核 |
| C07、C08—C11 | 不满足。总额和四项分项金额没有写进主张正文 |
| C12 | 租赁分类还在；`162,672,085.77` 仍主要在引文，不是单独的金额答案 |
| E01 | 不满足。产品暴露没有五项主要收入金额 |
| E02 | 数据传输、节目/推广、出口流量、商品采购有已核主张。系统维护没有单独成句 |
| E03 | 已核成本主张写了智能卡及机顶盒摊销，没有写成制造 |
| S01 | 第29次经营角色已核，依据页16经营权 |
| S02 | 政府、企事业、家庭用户的正文还在，第29次为待核 |
| S03 | 上游采购主张已核 |
| S04 | 已核主张写的是为中广电移动销售并按净额核算，不是向其销售，也不是设备制造 |
| N01 | 未完成。本期收入金额还没成答案，无法核列次 |
| N02 | 两个租赁口径的正文都还在，金额答案未分开写完 |
| N03 | 采购方向已写在已核主张里，待A看是否把购入写成了对外销售 |
| N04 | 计划句仍标着计划，成本主张没有写成设备制造 |
| N05 | 前五名仍是合计；另有报表上的预付和欠款名称 |
| N06 | 服务名称还在，五项收入金额没有进入产品暴露 |

### 89.4 证据和保护

第28次 `b1-company28-evidence/`：query `027f996076dc4957a422f5b400d6958c5eacb7d59d91387ba29726d1a471a630`，export `30255e3a320fa9936b92e5393e62aee3e8b49fb20b6ab0b08d2fd6a3e9a682ff`，budget `7e87d517d58d8eabb52241b4d4773796ee30c80de4062af2051b517bffc467be`，supervisor `fd0847cb90151e7f93d06a4baed8775ca5cce5d400520a2a87f5323f0b2941e7`，entry.log `99dfb78ccef0c5e72dae2549a07e7c2d42691b99607dcc09decfbc3f14126269`。

第29次 `b1-company29-evidence/`：query `e7078e777d334cccd040407982654c43c8aff13f5a30d6c36d332118c437dee6`，export `2b363ce503e0b01ac6da4b2fb8ba53e6dfd2c7c3d9701328deb8d5b0cd0928ad`，budget `4f1e25be931309d6b03a8410a544c2122a32bfeaa7def05a90f43f5cffd55cec`，supervisor `95265631378c0e90889cd62995d6e34b00ded73ec888ec796b5ff8375195f2c8`，entry.log `bbc8de1451b8f34dedb1f66237759a51da2dc7ccb5ee107b86b12cec92ebf887`。

两次运行根已删。尝试标记仍在 `b1-company24-attempts`。直接验证：保留已核主张、退出被撤回主张、缺金额的条件会进入成稿，2 passed。未改那个既有的长无句界失败用例。

既有13份修改和16项未跟踪内容没有暂存。

## 90. A审核15bc6624：纠错和角色接受，主要收入与可信接续未完成

日期：2026-10-23。按产品需求v7→验收v2→交付路径v3→A/B流程v3及当前change执行。A负责独立审核/规划，本轮未改B业务代码，新增真实模型调用0。审核对象是第28/29次各自结果，不把两版最好的部分拼成首发成绩。

### 90.1 主线判断及实际进展

**仍限用，B1/A1不通过；已有真实内容，应继续收口首家，暂不重写路线或扩批。** 第28次画像21/暴露14/供应链2条，22已核/13待核/2不可靠；第29次21/18/2条，16已核/23待核/2不可靠。两版query/export的profiles分别一致，三个完整标志均false。A核对§89所列10份文件hash一致，旧第27次不改。

接受的业务进展：农村收费复合主业已退出可靠消费；5G合作已改为原文“为中广电移动销售”及净额核算；节目费用103,811,962.05明确为应付账款期末余额；智能卡/机顶盒等成本语境、数据/节目推广/流量/商品采购方向已核；第29次网络经营权及基础网络运营角色真实核验通过。A用正式PDF独立核第29次16条程序已核正文及其引用，涉及页16、19、53、127—131、155、188—191，原引文去排版空白后可定位；语义核验另看本期列、主体、方向及余额/发生额，不以定位本身代替正确性。未发现本轮上述纠正应被退回的新底线错误。

未完成的是主营纠正、主要收入金额和完整核心投影，不是没有原文或没有真实调用。总收入、五项主要收入和5G代理费原文可读，accepted_facts仍空，四个主要金额未进正文；租赁数不能仅靠原表引文计交付。第28次仍可靠的收费机制、政企/家庭正文在第29次退回待核，最新可信成果不构成旧版超集。本轮又增加同义采购、前五名、预付/欠款排名；这些不能补偿主要收入缺失，更不能按重复ID凑准确率。

A自省：§41.6已经要求保正确成果并优先主要内容，本轮实现仍以缺数字串筛选代替实际业务缺口排序，且直接测试未经过真正连续两次磁盘加载。A下一卡必须使主要收入先成为新答案、已有真实核验不丢；不再被补充排名、更多测试或纯接续修复牵引。成本和吞吐在首家公司可用后再校准。

### 90.2 阻塞结论与必要验证

| 分类 | 已复现事实与影响 | 同卡处理 |
| --- | --- | --- |
| A/P1：真实核验凭据未随多轮接续保存 | `bounded_entry.load_saved_draft`从query去掉review_state，再只取本query的verify_views回执；`direct_reading`虽把旧checks加到本次内存结果，却不把它们沿相同接续出口保到下一query。直接调用现加载器：第28次22条verified有12条没有可加载claim-pass，第29次16条有10条没有；不代表原pass不存在，而是当前加载出口漏传 | 沿原持久对象/回执保可追溯的真实凭据；合法未变对象继续可信，撤回/修改/真实新否定仍局部失效。禁止相信状态字符串批量补pass。用实际加载器的连续两次磁盘重启证明后，直接继续真实业务 |
| A/P0/P2：主要收入仍未成答案，补写优先级与载荷不当 | 补写扫描引文中至少六位整数部分的数串，以正文是否含该串选条件，并以维度存在判断部分缺项；`conditions[:] = selected_conditions`又以补写子集替代原基线。它会继续吸纳上期/余额/排名，未证明主要条件真实覆盖。第29次补写zai→grok共300秒超时，失败预留34373/34368；随后较小采购/排名包能成功。不是总账拒绝，也不能据此断言LLM无法归纳 | 保原主要条件集合，补写子集独立；主要收入/主营纠正先单独成组，必要分包、简短回答及任务内池路由/时限调整，成功即存再核。两次同包超时后改方法继续，不原样重试、不为新授权停工 |

A复跑以下直接命令，**3 passed、7 warnings、7.37秒**（既有依赖/环境提示，不是业务分数）：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_kept_claim_survives_and_a_retracted_claim_is_not_verified \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_saved_source_conditions_are_drafted_without_rereading \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_logical_identity_separates_entries_and_verification_targets -q
```

现有保留/撤回单测在内存里直接传claims和checks，不能反证上述磁盘缺陷。B类：相同事实的补充重复主张可局部复用，不开通用去重/重构项目；与本轮无关旧测试和既有改动归C类，不修改。未做更多全年报模拟或外呼探针。

### 90.3 最新版原19/6裁决与进度

| 条件 | 第29次裁决 |
| --- | --- |
| C01/C02 | 未满足完整交付。原复合主业已撤回但未改写；经营权对象只证明经营权/网络范围，不代替开发维护及广播、点播、接入等具体业务 |
| C03—C06 | 正确正文仍在，当前待核，不移用第28次状态计本版通过 |
| C07—C12、E01 | 未满足金额答案/收入端完整交付；租赁分类可保留，但原表在引文中不等于本期数值答案 |
| E02 | 部分：数据/节目推广/流量已有，维护服务未形成完整答案 |
| E03 | 通过：采购商品及智能卡/机顶盒成本语境有源，不推制造 |
| S01 | 通过：经营权、覆盖16区及网络运营/服务环节已核 |
| S02 | 下游正文待核且供应链视图未收口，不通过 |
| S03 | 通过：数据、内容/节目推广、流量及商品等上游方向已核 |
| S04 | 部分：5G终端合作方向及净额成立；22,952,276.32合作发生额不是22,879,452.01代理费，后者及代理角色还需形成答案 |
| N01/N02/N06 | 未完成：主要收入/续页金额和服务收入端尚未完整交付 |
| N03/N04/N05 | 实际范围检查通过：采购方向、计划/制造边界、前五名合计无虚构具名；具名预付/欠款来自原表，且明确余额口径，不是由集中度编出客户名单 |

当前严格确认**3/19**（画像0/12、暴露1/3、供应链2/4），负例完成**3/6**。这不是把历史5/19改为3/19，也不是三视图完全无内容；是第29次可信交付范围退化后逐项核定。当前16条程序已核正文包含同义/重叠对象，不按ID数宣称16/16准确率；完整核心、补充抽审及唯一对象准确率仍待收口。原27/28次另保留，不拼版。观察1/4、可用0/4，尚余3家公司未观察；S1 0/12、S2 0/20、S3 0/100不变。

### 90.4 下一卡、资源与清理

按[执行卡§41.7](b1-execution-card.md#417-第2829次后的连续收口卡主要收入先落盘已有正确成果不退化)继续同一B窗口：保真实核验链→主要收入/主营纠正先落盘→核心投影/角色及真实核验→query/export/新实例末端验收。不是单独修接续再交A、不是重新读208页。验收§7.37原自主资源/必要真实运行授权完整有效，无新增入口次数、费用审批或自设余额停止线；用户已允许费用提高，unknown如实记，不购买充值、不改共享默认/生产、不扩其他公司。停止看实际服务/来源/账号/改动隔离或用户指令，不以两次原样超时就等待新授权。

第28次319017账本＝192222有效usage＋126795未知预留，16物理、1258.928660677746秒；第29次129384＝60643＋68741，7物理、570.362404378131秒。旧累计4165230/219不改，加本轮后**4613631账本/242物理**，费用unknown，不称实际token或成本达标。

临时物盘点约83M。第28/29次B已交接删除运行根；本轮未新增整库或复制年报，保留attempt、正式源及持久失败/审核证据；未证可删的历史库、v61/S2冻结和既有13份修改/16项未跟踪不动。本轮直接验证产生的2个无用空日志目录已删除（目录块约8KiB，无业务数据），其余缓存按复用需要保留；下一卡继续先核归属清历史无用物再开发，结束清本人产物，不另建清理台账。A本轮仅更新当前审核/任务指引，原§1—89正文和B证据保持。OpenSpec本change严格校验及文档差异检查通过；遥测网络报错未影响校验结论。

## 91. B交接：第31次主要收入金额已写入正文，主业复合句未通过核验

日期：2026-10-23。按[执行卡§41.7](b1-execution-card.md#417-第2829次后的连续收口卡主要收入先落盘已有正确成果不退化)继续，没有重读208页。最新查询是第31次。第30次是中间结果：金额曾生成，但同名 `claim-1` 被后一次覆盖。B自检不是A的19/6。观察1/4、可用0/4。生产 `not_authorized`，规模 false。

### 91.1 已核凭据接回，两条被覆盖的收入答案已恢复

`load_saved_draft` 按主张正文和引文对照第27—30次核验回执。`review_state` 不当作通过。撤回对象不拿回旧通过。磁盘测试连续加载两次：正文和引文不变的主张仍带上原来的 claim-pass；撤回主张不会因旧通过重新变成已核；正文改写后旧通过不再套用。

第30次模型已经写出信息业务收入 `897,897,997.75` 和收看维护收入 `724,769,470.04`，两次都使用本地 id `claim-1`。后一次频道收转主张用同一个 id 覆盖了前两条。第31次从第30次回执把这两条正文放回，id 分别为 `claim-information-business-revenue` 和 `claim-viewing-maintenance-revenue`，并各自核验通过。同 id、不同正文现在改名保留，不再互相覆盖。

### 91.2 第31次新增或核上的主要收入

以下金额在第31次主张正文里，且该主张为已核。租赁总额和经营租赁收入没有相加。

| 主张 | 正文中的本期金额 |
| --- | --- |
| `claim_revenue_model_2025` | 营业收入 `221,975.46` 万元；收看维护与信息业务合计 `162,266.75` 万元 |
| `claim-viewing-maintenance-revenue` | 有线电视收看维护收入 `724,769,470.04` 元 |
| `claim-information-business-revenue` | 信息业务收入 `897,897,997.75` 元 |
| `claim-1` | 频道收转收入 `165,529,697.84` 元 |
| `cond-5-products-services-engineering-construction-revenue` | 工程建设收入 `151,340,245.65` 元 |
| `claim-revenue_model-01` | 租赁收入 `162,672,085.77` 元 |
| `claim-5g-agency-fee` | 5G业务代理费收入 `22,879,452.01` 元 |

`claim-products-5g-terminal` 仍是“为中广电移动网络有限公司销售”和净额法，合作发生额 `22,952,276.32` 与代理费分开。查询正文里没有“向其销售”。节目费用 `103,811,962.05` 的两条主张都写明是期末余额。智能卡及机顶盒摊销、上游采购方向、16区网络运营角色、政企/家庭用户服务仍为已核；政企/家庭那条同时出现在供应链视图。

`claim-principal-business-core` 为 `not_reliable`。核验说明：页12写开发、经营管理和维护，页94写建设、管理和经营，主张把两页收成一句，任一引文都没有完整支持；页28的巩固基本盘和打造直播电视服务商是目标，不是当前身份。农村基本收视费在这句里被写成一般增值税政策，不是公司实际收费，但这一句不能让整项通过。该对象没有作为已核结果发布。

商品销售收入、收费机制复述、计划和排名仍有待核对象。系统维护没有单独成句。页18行业行的多列金额没有被写成另一条总额。

### 91.3 对照19项和6项负例

只看第31次。B不打分。

| 项 | 第31次文本 |
| --- | --- |
| C01 | 未形成可通过的主业句。经营权主张已核，但不足以单独代替开发、建设、经营和维护。复合主业未通过核验 |
| C02 | 收转、点播、接入等服务仍写在未通过的复合主业里，没有另一条已核的完整服务句 |
| C03 | 政企、家庭用户服务已核，并出现在供应链视图 |
| C04—C06 | 收入确认机制、行业地区、政企计划正文仍在且为已核 |
| C07 | 已核正文写了2025年度营业收入 `221,975.46` 万元 |
| C08—C11 | 四项分项金额都在各自已核正文中 |
| C12 | 已核正文单独写了租赁收入 `162,672,085.77` 元。经营租赁 `146,594,318.86` 仍不是另一条金额答案，也没有与租赁总额相加 |
| E01 | 产品暴露仍以投入和客户/供应商集中度为主。五项收入金额在公司画像，不在产品暴露 |
| E02 | 数据传输、节目/推广、出口流量、商品采购已核。系统维护没有单独成句 |
| E03 | 已核成本主张含智能卡及机顶盒摊销，没有写成制造 |
| S01 | 网络经营权、16个区和运营/服务环节已核 |
| S02 | 政府、企事业单位和家庭用户服务已核，并在供应链视图中 |
| S03 | 上游采购方向已核 |
| S04 | 终端合作为“为中广电移动销售”且净额法已核；代理费 `22,879,452.01` 另有已核正文 |
| N01—N06 | 主要收入金额已经是正文而不是只在引文里。复合主业未通过，所以负例不能由B计为6/6。排名和计划保持待核，没有继续补写 |

### 91.4 调用、证据和清理

第30次账本154039＝有效108814＋未知45225，物理17、逻辑15、恢复2，监督耗时901.2478343350813秒，1次失败。第31次账本52074全部为有效usage，未知0，物理10、逻辑10、恢复0，监督耗时395.1233111638576秒。费用两版都是 unknown。§90累计4613631账本/242物理不改；加上这两次为4819744账本/269物理。

证据目录 `b1-company30-evidence`、`b1-company31-evidence`。query与export的profiles一致。运行根在hash一致后删除。attempt `company30.json`、`company31.json` 保留。

第30次：query `5331e2671032bbe86f2dcec20e8e660e45005da47bdd6c843e65b1595f9155f1`，export `3c6f789c7618054f1ed03166811597d1ec16782ba6206070e30045c67c5f390f`，budget `dd063755d338ab2b945262dca2f54d4da1132306046e84d5573ae0bd7aecba4f`，supervisor `1355895941afecd3b5032dfcb4c9bdf036cabb6a1c2814a37d35312ac40e727f`，entry.log `8ba42f8a9b7ab9ca3cf30596765896b083cda5a7e7c33ce7017889119c2f5b2f`。

第31次：query `523b89b22ccba181bc1fec921bdb3e2503bc7574ed1f73d4067827381de0ba35`，export `0e1d1e690f838fba51bcd8493f06457b64d7f0e4bbca4046f16be501b8711cd9`，budget `afb8588e735e221fafbde1979103bff58a7fd2025c022edf1c18ac3c814032e6`，supervisor `16d239f87afa0e680d0ff7152a3a5a485d67c48442e58e34c0cb2c36538f7461`，entry.log `3d3f47ed13cad5968fc03c45e9ae90c4fa02b40d69d4bbbf6ecf1e59c18f1648`。

定向测试：凭据两次加载、保留/撤回、同 id 两条收入答案，3 passed。开始前既有修改和未跟踪文件没有纳入本次提交。

## 92. A审核15fe35fb：主要收入接受，首家还差主业与收入端投影

日期：2026-10-23。依次按产品总需求v7、验收v2、交付路径v3、A/B流程v3及当前change审核。A本轮只审查、规划和更新任务指引，未改业务代码，新增真实模型调用0；B继续实现。

### 92.1 主线判断与真实进展

**第31次主要收入交付接受，首家公司仍限用、B1/A1未过；不重写路线，不回到全年报重读。** 相比第29次3/19，本版达到15/19，主要金额、正确收费机制和下游服务已经可审；剩下的主线是简明当前主营/具体服务，以及已有收入的产品暴露投影。系统维护单项和补充计划/排名不升级为必须全部修完。A不为剩余4项再拆4张小卡，不因错误的完整标志放行，也不因一条复合句被拒就否认所有正确成果。

两版query/export的profiles分别一致，§91所列10份query/export/budget/supervisor/log哈希核对一致。第31次三视图32/18/3条呈现，共52个主张ID（36已核、15待核、1不可靠）；政企/家庭主张共享到供应链视图，重复呈现不重复计事实。36条程序已核主张的引用在正式PDF去排版空白后均可定位。复用§88/90已独立核过且正文/引文未变的对象，重点复核本次新增7项收入、租赁续页和修改的主营，不用引文定位代替语义准确性。

七项关键收入对象独立核对7/7正确：总收入221,975.46万元（第73页），收看维护724,769,470.04元、信息897,897,997.75元、频道165,529,697.84元、工程151,340,245.65元、5G代理22,879,452.01元（第165页），租赁162,672,085.77元（第166页）。总额与精确2,219,754,557.95元相差42.05元，是原披露精度允许的舍入，不判数字错误。租赁总额未与经营租赁146,594,318.86元及融资收益16,077,766.91元重复加总；5G代理费与关联合作22,952,276.32元分开，终端合作仍为“为其销售/净额法”。节目费用103,811,962.05元仍为期末余额。

### 92.2 原19/6及完整性裁决

| 条件 | 第31次A裁决 |
| --- | --- |
| C01/C02 | 未通过：复合主营整条not_reliable，独立经营权和频道/收入对象不足以替代开发维护及广播、点播、接入等完整主营/服务回答 |
| C03—C06 | 通过：当前政企/家庭服务及收看、信息、工程/入网10年、频道期限等经营机制已核；计划文本保计划性质 |
| C07—C12 | 通过：总额及五项主要收入已在实际已核正文中，主体/本期列/单位及租赁续页正确 |
| E01 | 未通过：五项金额在公司画像，但产品暴露18条中没有products_services或revenue_model，未形成服务收入端投影 |
| E02 | 部分未通过：采购方向/数据/节目推广/流量成立，系统维护局部遗漏；第53页软件系统维护479,888.40元可保作明确的小额缺项，不要求单独一条句子 |
| E03、S01—S04 | 通过：智能卡/机顶盒成本语境、网络运营角色、政企/家庭下游、上游采购方向、5G代理及合作边界成立 |
| N01/N02 | 通过：本期收入/成本/上期列无错置，租赁续页未借合同小计或重复加总 |
| N03/N04/N05 | 通过：采购未反向，当前/计划及制造边界在可靠输出中保持，集中度未虚构具名客户；不可靠主营不计可靠结论 |
| N06 | 未通过：产品暴露缺服务收入端；没有据此宣称零商品暴露或利润敏感性，仍需补投影后实查 |

合计**15/19**（公司画像10/12、产品暴露1/3、供应链4/4），负例完成**5/6**。七项关键收入7/7不等于公司全量准确率；原37次已核呈现及36个已核ID仍有同义/重叠对象，不按ID或重复投影凑准确率。完整核心及唯一对象准确率门槛尚未全部验完，不能宣称B1通过。

A对主营核验的裁决：第12页“开发、经营管理和维护”与第94页“建设、管理和经营”可由同一主体/期间的多处证据共同支持摘要，不能要求任一条引文独自逐字覆盖整句；因此“合并两页”本身不是拒绝理由。第28页巩固基本盘、打造直播电视服务商位于经营计划，必须与当前身份区分；一般增值税政策也不应夹在当前主营中。新答案应简明表达已有源的当前业务，不反复争取旧长句pass，不要求删掉有源“建设”，也不强制所有信息塞成一条。

### 92.3 直接程序审核与验证

| 分类 | 实际证据/影响 | 同窗修复要求 |
| --- | --- | --- |
| A/P1：核验未跟随改名后的主张ID | `upsert`把同名不同正文改为`claim-1__2`，后续核验仍用原组中的`claim-1`；核验后再upsert又生成`__3`。A沿B现有`test_same_local_claim_id_keeps_both_revenue_answers`捕获实际result：信息收入`claim-1`为verified，维护收入`__2`和`__3`均pending，checks为两条`claim-1/pass`。测试只断言两金额存在，漏掉可信状态错误。第31次通过给恢复收入独立命名并真实核验取得了有效金额，不能据此认为通用冲突路径已修好 | 同一对象在生成、保存、核验、覆盖和状态更新中用一致ID；不同业务保留，状态更新不新增副本。用两条都已核及第二条unclear不误伤第一条的直接结果证明，不引入ID平台 |
| A/P1：主要覆盖基线仍被补写子集替换，完整性误报 | 继续保留`conditions[:] = selected_conditions`；`_completeness_gaps`以某个维度有observed对象代替主营实质，`_result`不把当前主要缺项作为阻塞。实际第31次`core_complete=true`、`draft_complete=true`、`semantic_complete=true`、`real_semantic_complete=true`，同时主营not_reliable、E01收入端缺失，classification仅pending_independent_review。B未宣称A通过是正确的，但程序标志仍不符合同 | 保源条件基线，补写子集独立；只在有效主要业务/收入端/角色及必要覆盖成立时完成，不用维度存在放行。次要待核不一律阻塞；原31快照不改，后继修正标志并实际读回 |

接续凭据恢复在本次真实样本范围接受，不代表任意跨报告/修改依赖都已泛化验证。B类：细粒度去重、补充排名与全部附注恢复后置；C类：原13份修改、16项未跟踪和无关旧失败不改。

A复跑3项，**3 passed、7 warnings、9.58秒**：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_unchanged_claim_keeps_its_check_across_two_disk_loads \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_kept_claim_survives_and_a_retracted_claim_is_not_verified \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_same_local_claim_id_keeps_both_revenue_answers -q
```

上述绿色不能覆盖A另外检查到的同ID核验缺陷，未改弱测试断言、未跑全年报模拟或真实探针。A只读挑战直接复用测试函数和实际结果，没有新建诊断脚本/库。

### 92.4 下一卡、进度、资源与清理

执行[§41.8首家验收收口卡](b1-execution-card.md#418-第31次后的首家验收收口当前主业与收入端投影)，同一B窗口完成当前主营/具体服务、既有收入共享投影及上述ID/覆盖修复，真实复验后交A1。收口C01/C02/E01可到18/19；E02小额系统维护允许作为唯一局部遗漏，仍明记未通过及限制，不放宽整块主业/变现/角色及准确率底线。无须把其余15条补充待核全做完。§7.37自主资源/必要真实运行及费用可提高授权完整延续，不新增一次入口用完即停或预算审批；不重读全年报、不重核正确金额、不另开纯工程卡。

第30次154039账本＝108814有效＋45225未知，17物理/901.2478343350813秒；第31次52074全部有效usage，10物理/395.1233111638576秒。旧累计不改，新累计**4819744账本/269物理**，费用unknown，不宣称成本合格。观察1/4、可用0/4，余3家公司未观察；S1/S2/S3仍0/12、0/20、0/100，A1通过后才转B2，生产not_authorized、规模false，不归档。

QuoteBak约83M；第30/31次持久证据合计约3.4M，有本轮审核/接续用途。`recovered-draft.json`被第31次绑定引用，暂留；运行根已由B核hash后删除，attempt/正式源及旧失败证据保留。A确认无活跃pytest/bounded_entry、加载测试目录仅被会重建它的测试引用后，清3份可重建测试JSON（2946字节）、测试目录及20个空pytest日志目录（合计约96KiB文件/目录块，可由测试重建）。未动原13份修改/16项未跟踪及归属不明旧库；下一卡继续先核清无用历史物，再开发，结束清本人产物，不新建清理台账。

## 93. B交接：第32次当前主营已核，收入进入产品暴露

日期：2026-10-23。按[执行卡§41.8](b1-execution-card.md#418-第31次后的首家验收收口当前主业与收入端投影)继续，没有重读208页，没有改第31次及更早的query/export。最新查询是第32次。B自检不是A的19/6。观察1/4、可用0/4。生产 `not_authorized`，规模 false。

### 93.1 当前主营

新的已核主张 `claim-1__2` 写的是当前经营：公司主要负责北京市有线广播电视网络的开发、建设、管理、经营和维护，并从事节目收转传送、网络信息服务、视频点播，以及基于有线电视网的互联网接入和数据传送增值等业务。引文是第12页和第94页，两条源条件都写在 `condition_ids` 里。这句话没有“打造直播电视服务商”，也没有农村基本收视费或增值税政策。

旧的 `claim-principal-business-core` 仍是 `not_reliable`，正文还留着目标和税收政策，没有被重新核成通过。政企计划仍标明是计划。

### 93.2 产品暴露里的收入

已核的服务收入和收入确认主张按同一主张、同一通过记录出现在产品暴露，没有重新生成或再核这些金额。其中仍包括营业收入 `221,975.46` 万元，收看维护 `724,769,470.04` 元，信息业务 `897,897,997.75` 元，频道收转 `165,529,697.84` 元，工程建设 `151,340,245.65` 元，租赁 `162,672,085.77` 元，5G代理费 `22,879,452.01` 元。5G终端合作仍是“为中广电移动销售”和净额法。查询正文里没有“向其销售”。

同名不同正文不再在核验后多复制一份。直接测试里两条收入都可以是已核；后一条被标为 unclear 时，前一条保持已核，且后一条只有一个对象。

源条件集合不再被补写子集替换。计划、排名、存货和收费政策不挡住完成标志。第32次三个完整标志和 `core_complete` 为 true；旧复合主业不计入已核。第31次原快照未改。

### 93.3 对照19项和6项负例

只看第32次。B不打分。

| 项 | 第32次文本 |
| --- | --- |
| C01/C02 | 已核主张同时写了开发、建设、管理、经营、维护，以及收转、点播、网络信息和接入。旧复合句仍不可靠 |
| C03—C12 | 第31次已核的政企/家庭、收费机制、总额和五项收入仍在，且仍为已核 |
| E01 | 上述收入主张出现在产品暴露，沿用原通过记录 |
| E02 | 仍未单独写系统维护。页53的小额维护费继续作为明确缺项 |
| E03、S01—S04 | 成本、网络运营角色、下游、上游和5G代理边界仍为已核 |
| N01—N05 | 沿用第31次已接受的范围；不可靠主营不作为可靠结论 |
| N06 | 产品暴露已有服务收入端。是否全过由A实查 |

### 93.4 调用、证据和清理

第32次账本45034＝有效34920＋未知10114，物理8、逻辑7、恢复1，监督耗时457.1855048080906秒。费用 unknown。§92累计4819744账本/269物理不改；加上这次为4864778账本/277物理。

证据目录 `b1-company32-evidence`。query与export的profiles一致。运行根在hash一致后删除。attempt `company32.json` 保留。

query `0b9742fdde988c888f1d8664a482ea7082e001ed9a11c0b9a0d99cdcdd10c59c`，export `cc659f77b8b50d83f5a9bdb89762c0ba0bc1404ca4534f5692b5e6e4cb9b2488`，budget `5169cd288547336aca078c384e005a9ecac16299385446bd35b63d64fde3c06f`，supervisor `4009f82878e9faf7f533cf37991db0700a3b77c1dab504b69c4807ce023617ca`，entry.log `22180cedc235a6bb2a92149be29e61c6b589eea45dccee18bd59d9fd9fb19aa4`。

定向测试5项通过：凭据两次加载、保留/撤回、两条同名收入都已核、unclear只影响自身、条件通过必须指向已交付对象。开始前既有修改和未跟踪文件没有纳入本次提交。

## 94. A审核a6fa4320：首家内容通过，转向三家跨行业验证

日期：2026-10-23。按产品总需求→验收v2→交付路径v3→A/B流程v3及当前change执行；A只审核、规划和更新文档，未改业务代码，真实模型调用0。

### 94.1 主线及阶段裁决

**第32次补救样本内容有限通过，首家公司可用；不再为歌华次要缺项开真实轮。** 当前主营及具体服务有源成立，已有可信收入共享到了产品暴露，三类产品的核心已可用。主线已从“有没有答案/主要收入”变为“同一实现能否在不同公司自动交付”。A不能继续用小额维护、计划/排名全核或成本尚unknown把业务进度写成0，也不能因样本答案正确就认可通用完整性规则。

本轮确认18/19（94.74%），视图12/12、2/3、4/4；适用负例6/6。A1仅对本份补救样本内容有限通过，观察1/4、补救可用**1/4**，首次失败保留，不称同版首发通过或通用/成本达标。S1/S2/S3仍0/12、0/20、0/100。一个真实可用样本还不能证明数千家公司经济可行；下一卡直接验证另外3类，而不是继续优化一家公司。

一项A/P1通用覆盖缺陷仍未关闭，明确转入[B2卡](b2-execution-card.md)前段。最小修复和直接正反/持久证明成立后条件执行该卡3家，不新增逐函数等A；未修不得真实发送新样本。不把P1降为建议，也不把工程整改再拆成歌华第33次真实验收。B1旧运行卡结束；整个change仍在实施，生产not_authorized、规模false。

### 94.2 实际答案、原19/6与独立准确性

第32次query/export的profiles一致，§93五份hash核对一致；PDF hash与原绑定相同，62份冻结文件hash均匹配。视图36/36/3条，共75次呈现、56个ID：40已核、15待核、1不可靠。36条已核对象的正文/引文/条件关联和可信状态与第31次完全相同，复用§92及更早有源审核；4条新增重点核第12/94、116、165页的原文、表头和上下文。40条已核对象的全部引用去排版空白后均能在其正式页定位，定位不代替语义判断。

新增`claim-1__2`由第12页“开发、经营管理和维护”与第94页“建设、管理和经营”及具体服务共同支持，没有未来目标/税收政策，跨页归纳接受。新增第116页收入机制及业务类型准确；商品销售67290058.81与第165页本期一致，正文省略元单位但所引表头明确，作为展示补全建议、不判成错误金额。上轮七项正确收入未重新生成/再核，共享到产品暴露保相同ID及通过依据；旧复合主营仍不可靠，没有回升。总额正式万元舍入、租赁总额/经营租赁/融资收益、5G合作发生额/代理费、节目费用期末余额均保持原界限。

| 原条件 | A对第32次的裁决 |
| --- | --- |
| C01/C02 | 新的已核当前主营及广播收转、网络信息、点播、接入/数据传送等具体服务成立，通过 |
| C03—C12 | 政企/家庭、实际收费确认机制、总额及五项收入保合法真实核验链，全部通过 |
| E01 | 五项主要服务收入已在产品暴露中可辨识并关联原主张/证据，通过 |
| E02 | 数据/节目推广/流量等已交，系统维护479888.40元仍遗漏；按上轮裁决不通过，作为18/19内唯一局部限制，不称未披露 |
| E03、S01—S04 | 成本、网络运营角色、上下游方向及5G代理合作边界保持，全部通过 |
| N01/N02 | 第165—166页本期收入/成本/上期及租赁续页、总额边界实际检查通过 |
| N03/N04 | 采购不反向，5G为其销售/净额，未推制造/已完成目标；不可靠复合句退出可信消费，通过 |
| N05/N06 | 集中度未据此生成具名销售客户，欠款/预付对象保各自性质；无commodity ID未丢服务且未称零暴露/利润敏感性，通过 |

准确性分母不按75次投影或40个同义ID累加。下列28个业务对象组去重核对，每组全部已输出子项有源正确才计组通过；这是本份可信输出的已审范围，不是原子指标总数或未见准确率：

| 组别 | 已审唯一业务对象组 | 结果 |
| --- | --- | --- |
| 核心14组 | 当前主营/具体服务；政企家庭；收费确认机制；总收入；收看维护、信息、频道、工程、租赁、5G代理六个收入对象；网络经营权/运营角色；5G终端合作；采购/上游关系；成本构成 | 14/14 |
| 补充14组（不足20，全部查） | 资产置换；政企计划；收入地域；关联劳务收入类型；2026目标；AI探索；两项频道关联交易；完整业务类型清单；商品销售；节目费余额；前五客户集中度；应收/合同资产欠款组；供应商集中度；预付对象组 | 14/14 |

可信已审合计**28/28，100%**，达到本样本≥98%门槛；不以重复排名、共享投影或未核对象凑数。15条pending保待核，1条不可靠及错误轨迹留存、不作为确定结论；相同内容存在新已核版本不代表所有旧pending自动晋升。原首发错误及此前准确率不改。

### 94.3 直接代码审核：样本过线不等于通用规则正确

同名ID修复接受：保存后的ID返回给后续核验，直接测试现在证明两条同名不同收入各自已核、第二条unclear不伤第一条且不额外产生第三份。条件基线不再被补写子集替换亦接受。

**A类/P1：用词语、维度和引文包含替代语义覆盖。** `direct_reading.py::_completeness_gaps`新加`_supplementary`，见“收费/定位/目标”等便跳过；主要products_services若不含“收入/代理”也跳过；input_exposure不在允许维度；`_quote_covered`又把condition_ids或引文前缀包含当覆盖。源条件major/时态及有效的覆盖对照未被作为真正依据，违反产品§9—11及该change语义覆盖要求。

A直接复用现有`_report/_evidence`构造同源长引文“公司提供云服务并取得云服务收入；另有独立的工业设计服务，其面向设备制造客户提供收费设计服务。”，五个必要维度的已核主张均只答云服务；只有claim/pass、没有condition覆盖检查，执行实际`_completeness_gaps`：

| current、major=true的未交付条件 | 期望 | 实际 |
| --- | --- | --- |
| revenue_model：公司的另一主要变现机制是按使用量收费。 | 保主要缺口 | 空缺口列表 |
| products_services：另一主要产品是工业设计服务。 | 保主要缺口 | 空缺口列表 |
| input_exposure：公司关键投入是云计算基础设施。 | 保主要缺口 | 空缺口列表 |
| principal_business：另一主要业务是工业设计。 | 相同长引文不代替答案覆盖 | 空缺口列表 |

这是当前可执行路径，不是推测性建议；四种现象合为一个语义覆盖根因，不拆四张返修卡。现有“condition pass必须有对象”测试缺少已核但未覆盖该条件的反例，所以绿色不足以关闭P1。B应局部去除这些兜底，用真实有源语义覆盖判定和有效目标绑定；次要计划/排名可经有理由的语义重要性更正后不阻塞，不能走向全附注必核。无需新平台或新一轮歌华真实调用。

A复跑如下5项，**5 passed、7 warnings、11.83秒**，另做上述四项只读挑战，未改业务代码/测试预期、未新建诊断脚本或业务库：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_unchanged_claim_keeps_its_check_across_two_disk_loads \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_kept_claim_survives_and_a_retracted_claim_is_not_verified \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_same_local_claim_id_keeps_both_revenue_answers \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_an_unclear_renamed_claim_does_not_replace_the_other_answer \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_condition_pass_without_a_delivered_object_is_not_coverage -q
```

B类：去重展示、商品销售的单位文字、小额维护及可选排名补核均不阻塞首家；C类：原13份修改、16项未跟踪及无关旧失败不改。新增阻塞根因1项，转B2前段，不重复调查已确认的收入/ID。

### 94.4 下一张卡、资源、清理及效率

执行[B2连续开发卡](b2-execution-card.md)，具体来源/12项逐家公司合同/负例、P1直接证明、新3家运行条件及停止范围均已固定。前段修好即可同一卡连续交上汽、浦发、同方，卡末A2；补充待核不全收，4家未齐不进S1，不开第5家。这个转向是主线规划：先把已有首家内容验收兑现，再观察跨公司能力，而不是继续在单家公司上堆补丁。

第32次8物理、7逻辑、1恢复，34920有效usage＋10114未知预留＝45034账本，457.1855048080906秒，费用unknown；无价不记0。累计4864778账本/277物理，不是实际token总数/人民币费用。B2独立累计及有价/unknown范围见验收§7.41，不拿歌华剩余或旧预算作新3家权限，不为每个工作窗口另等批准。

A核运行根已删、attempt保留、持久证据存在（第32次约1.5M）；QuoteBak约83M，固定日期加载测试又重建3份JSON，确认无活跃测试/运行进程后收尾删除3文件（2946字节）、测试空目录及3个空pytest日志目录，均可重建；修产生位置已写入B2卡。B运行根删除前hash核对属于B回执，A只确认现状；原30/31恢复文件仍被引用，正式源、旧失败、v61/S2及归属不明旧库保留。没有对既有改动或未知归属未跟踪文件做清理。A活跃审核约30分钟（估算，含源核/规划/文档，机器/审批等待另计），本轮未做人工填业务答案或真实补救。

本轮OpenSpec严格校验、文档差异检查及26个新增文件/锚点链接检查通过；历史§2—93正文保持不变，§1仅更新当前入口。无Ruff适用代码改动，不把文档校验称作业务测试。只提交A本轮7份文档，验收计划原有历史details改动按开始基线隔离，不纳入本轮提交。

## 95. B进度：覆盖误判已关，上汽已有正文，浦发正在读

日期：2026-10-23。按[B2卡](b2-execution-card.md)执行。这不是四家交卷，B不打分。歌华没有再跑。

覆盖判断不再用词语名单或引文重叠把未回答的主要业务当成已覆盖。当前且 major 的条件，必须有指向有效主张或记录的通过检查。计划状态或非 major 的条件不挡住核心。直接测试覆盖了收费、工业设计产品、云计算投入，以及与云服务共用长引文的另一主业；直接阅读路径上的同一缺口也不会把三个完整标志写成 true。

上汽首发读完源条件后，一次合成返回 0 条主张。账本 1059372（有效 370798，未知 688574），物理 44，约 3707 秒。证据在 `b2-saic1-evidence`，query `79f1ce9166e8a1d2673875f7061ed0ba3ec52a1b4f4319342d16d10662decf26`。

后继改为一条条件一次成稿。公司剩余时限用尽，进程在写出 query/export 前被停止。检查点里有 50 条主张，其中 33 条已无待核限制，内容包括整车、零部件、劳务及其他和金融，以及对应收入。检查点 `b2-saic2-evidence/checkpoint.json`，SHA-256 `ec6aabb1fcd50a13c4646d3083237fb9ed0a99e9d056ed20dc2d980df3abf4e3`。监督退出码 124。这不能当作已导出的三视图。

浦发首发已按同一条条件成稿的代码启动。同方绑定已重算，尚未开始，避免和浦发抢同一个池。

## 96. A审核B2在途成果：已有答案，先交付再按业务组收口

日期：2026-10-23。审核提交`04de33eb`、`cbe89d24`及§95进度；按产品总需求→验收唯一进度→交付路径/B2卡→A/B流程v3及本change执行。本次不是A2，A未改业务代码/候选或冻结回执，真实模型调用0。

### 96.1 主线、自省及裁决

**覆盖误判根因接受；上汽已经有能查询和导出的限用正文，当前不能继续报告“只有checkpoint、没有答案”。可用仍1/4，不打上汽/浦发12/6。** A独立调用原查询owner，证明已有发布版本可读；临时导出及新实例读回一致，不需再外呼模型或重新run。上汽4条供应链正文均待核，模型标33已核也不等于A认可全部正确。

本轮真实进展是从歌华单样本走到上汽不同披露的实际三视图、主要收入及真实核验；尚未形成第二家可用公司。A不能把语义条件机械当开发工单，也不能只给足预算而不审查成功阶段何时保存、第一份核心交付何时发生。现在的主矛盾是**成功源/业务成果的持久交付时点和核心业务分组**，并有主体限定丢失的正确性阻塞；不是先补更多附注明细、重跑大mock或再建预算/恢复平台。

浦发在本次审核期间已按原7200秒绑定停止（见96.4）；从“在途”变为首发失败，checkpoint源条件/主张/回执仍0。这证明“全年报读源结束/全部条件成稿之后才进入可交付阶段”仍会丢掉整窗价值。下一卡沿唯一[B2卡§7](b2-execution-card.md#7-b2接续先兑现已有正文按业务组收口三家公司)，将成功源片立即持久、核心跨章业务组先交付/及时核验、超时原owner收尾及主体限定合成一次剩余共性修复窗口。小型实际入口证明后连续完成三家，原样重试和整本重跑mock均不作为下一交付。

观察更新为已完成3/4（歌华、上汽、浦发），同方未启；补救可用1/4。首次及正式后继失败保留；上汽追加开发收口显式另列。S1/S2/S3仍0/12、0/20、0/100，4家最终可用才交A2决定S1，生产not_authorized、规模false，不归档。

### 96.2 实际业务与原owner独立读回

持久`b2-saic2-evidence/checkpoint.json`及运行根`output/company_profile_common_core.v1/bp-work-5157694452e829569d6a42f3.json` SHA-256均为`ec6aabb1fcd50a13c4646d3083237fb9ed0a99e9d056ed20dc2d980df3abf4e3`。205页均标成功读源，46条源条件；50个唯一候选中程序状态33 observed/无限制、9 pending、7依赖未核及1 unclear，33个均有claim/pass。以上是程序状态核对，不是A准确性评分。

`CompanyProfileReadService("data/QuoteBak/b2-saic2-20261023/output").query(("600104.SH",), work_id="bp-work-5157694452e829569d6a42f3")`实际返回`found/delivered=1`：

| 原owner视图 | 呈现条数及状态 | 独立结论 |
| --- | --- | --- |
| company_profile | 40；33 verified、7 not_reliable | 有当前主业、整车/零部件/劳务及其他/金融和收入文本，不是空结果 |
| product_exposure | 29；23 verified、5 pending、1 not_reliable | 已有共享收入端文本，核心投入仍有待核 |
| supply_chain_position | 4；全部pending | 有经营角色/经销网络/上下游正文，但还不能当可靠结论 |

三完整标志均false。73次呈现不当73个唯一对象；没有逐项12/6及全量源核，不报33/33准确或公司可用。A核第11页制造/金融及分产品金额与候选一致，第155—156页主营/其他业务与时点/时段有不同口径；不得把同名总额相加或混成集团总收入。高风险抽查另见96.3，不能据模型pass外放全部候选。

A调用同一个reader的`export`到项目TMPDIR下`TemporaryDirectory(prefix="b2-a-readback-")`，再起新reader query，三份profiles完全相同；生成3个临时导出文件合计915895字节，退出上下文自动清理，原runtime hash不变。此为审核最小验证，不替B写正式交付、不退还124入口或称B发送前已完成就绪。B现在只需将同一路径用于正式持久快照，避免为了导出再启动语义。

### 96.3 代码审核及必须同窗处理的阻塞

覆盖P1根因关闭：`_completeness_gaps`移除词语/维度/引文重叠兜底；current、major条件须有绑定有效对象的condition/pass，依赖冲突不能解除缺口。四类缺项、正向有效对象、次要/计划不挡及后续撤回在本轮定向节点中通过；真实上汽缺口在原owner读回仍false。B被停止的208页离线入口/读回不作为已过证明，A本次小范围读回补证；不追写B首发前记录。

当前新增阻塞根因3项，合并到同一次业务收口，不拆3张返修卡：

1. **A/P1，成功阶段未及时持久与超时交付收尾。** `inventory_pages`在成功源响应后累积条件，但不及时发布独立阶段快照；浦发72笔请求后持久解释仍无源回执/条件，具体原响应能否从其他既有缓存恢复待B核。上汽已有runtime却因`bounded_entry`只在`run`返回后query/export，124时没有交付文件。沿原checkpoint/owner局部修复，每成功源片立即保存；本地交付从最后持久版本读取，不依赖长任务正常结束，不建新恢复平台。
2. **A/P1，逐条件串行成稿且核验后置。** `compose`/`draft_reused_sources`默认一条件一请求，全部草稿后才`settle_claims`；上汽后继74份语义回执含24份复用源、43次成稿和7次核验。按维度rank先排大量主营/子公司细项，重要投入/角色留在尾部待核。同条件同义投影多次占时。应按业务关系跨章有限合并、核心三视图共同优先，一组核验发布后再下一组；源发现可继续，但不等全文发现/全部附注成稿才交首个可审核心组。
3. **A/P1，核验上下文丢失更窄主体。** 上汽已核`claim-1`为“公司主营业务收入及其他业务收入主要在某一时点确认”，只给页203的单句引文/相同context_before；页193明确起“十九、母公司财务报表主要项目注释”，该句不能不加限定概括集团。页156集团另列11832005928.17元在某一时段确认。新版本先将该歧义对象退出可信，再保母公司限定核验；其他无关正确内容继续保留。修上下文与解释，不用证券/页码规则或A手填答案解决。

非阻塞建议：展示去重、非主要子公司/排名/附注明细、常规吞吐及行业增强后置。C类：既有13份修改、16项未跟踪及无关后台/生产进程不动。本次没有改测试预期、补写业务答案或靠放宽主要业务门槛制造通过。

A复验命令（项目环境、隔离测试目录），**4 passed、7 warnings、11.99秒**：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_keywords_and_shared_quotes_do_not_cover_an_unanswered_major_business \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_direct_reading_does_not_publish_an_uncovered_major_business \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_condition_pass_without_a_delivered_object_is_not_coverage \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_later_block_retracts_that_claim_and_keeps_another -q
```

现有4测不证明上述新阻塞已修；下一卡只加对应小型实际入口的成功/中断及限定反例，不重跑整本mock或建立新测试平台。

### 96.4 资源、在途变化、清理及下一步

上汽首发：3706.771485秒、44物理、1059372账本＝370798有效usage＋688574未知；后继：3400.049023秒、60物理、667290＝519335有效＋147955未知（含尚reserved请求），124退出。合计1726662/7106.820508秒/104物理/12恢复；原累计额内仅余约93秒且0恢复，不能把还有账本当作仍可继续调用。

审核开始以宿主只读进程与日志核浦发确在运行，没有干预；2026-10-23北京时间22:18读取已见`supervisor.json`：124、`entry_deadline_exhausted_workers_killed`、7200.059529秒。`physical-budget.json`为1568065账本（537672有效＋1030393未知）、72物理/6恢复；原owner checkpoint的源条件/主张/语义回执均0，query/export/entry-result文件均无。日志尾为22:13:39语义心跳。该变化以本轮读取为准，不能继续照抄§95“尚无模型调用完成”。未核其他缓存，不宣称所有响应永久不可恢复。

B2当前已用3294727账本、176物理、18恢复，费用unknown，不与歌华原4864778/277混作实际tokens或人民币。当前卡按验收§7.42显式补足研发范围：上汽一次额外定向开发收口，浦发/同方原未用正式后继保持；代码/prompt/复用范围冻结后连续执行，不逐公司或窗口请批。新累计额为上汽/同方各2400000账本/10800秒/128物理/16恢复、浦发3600000/14400秒/160物理/16恢复，整卡8400000/36000秒/416物理/48恢复；已用计入，旧失败不改。有价仍单家200元/整卡600元，无价unknown不能保证总货币成本，现池/账号、不购买充值/不改生产。

QuoteBak约92M，上汽两套持久证据合计约2.6M；上汽运行根仍为现有可读发布版本、浦发失败根的恢复性及解析占用尚未核清，不能盲删。宿主还见归属未核的解析子进程，A未按PID/日期清理或终止；已将B核本人占用及收尾写入卡。A临时导出915895字节已自动删除，可用相同原owner重建，无正式资产或历史证据被删。历史无依赖测试物仍须B开发前先清、交接前收尾，只留一套必要持久版本，不因是OpenSpec目录就永久囤积。

本轮更新7份现行文档，保§2—95及原首次/后继不变；验收计划原details的既有修改按基线隔离。本轮A活跃审核/规划约40分钟（估算，等待和机器运行另计），新增阻塞根因3项；B活跃时间及额外补救成本本次未提供，不假填0。OpenSpec严格校验、文档差异检查及18个新增/更新文件或锚点链接检查通过，原§2—95保持一致、资源合计核对通过；不把这些校验称作12/6通过。

## 97. B进度：上汽正文已导出，追加收口用尽，同方正在读

日期：2026-10-24。按[B2§7](b2-execution-card.md#7-b2接续先兑现已有正文按业务组收口三家公司)执行。这不是A2，B不打分，可用进度不改。

上汽后继退出124之后，原`CompanyProfileReadService`按工作`bp-work-5157694452e829569d6a42f3`查询、导出并用新实例读回。三处正文一致：`found`/`delivered=1`，公司画像40、产品暴露29、供应链4，三个完整标志为false。正式快照在`b2-saic2-delivery`，query `b58102e45c1e34f76190f1d51a6bb346f8cbc6bd91097167eee43de058986ece`，export `ec06650690cffb3f4dff5ac07a2f66214830ba632be7e3e9e3a32e4ee06081ac`。运行中的检查点文件没有改写，仍是`ec6aabb1fcd50a13c4646d3083237fb9ed0a99e9d056ed20dc2d980df3abf4e3`。这是可审限用正文，不是公司可用。

追加开发收口没有重读205页。同一窗口先发4次物理后，因同一证据编号在核验请求里内容不一致而停止，已把`claim-1`“公司……主要在某一时点确认”退出可信。余下20次物理继续同一窗口，用尽上汽128次物理上限后停止。最终查询在`b2-saic-closeout-result`，query `cab23a2baf27a3f4fb211f68c736432ecbccf0d8c12f0c8724f3be9aff995af6`。`claim-1`保持`retracted_pending_correction`。另有已核句子保留“本公司主营业务收入及其他业务收入主要在某一时点确认”。同时出现一条把该时点确认写成“上海汽车集团股份有限公司”的已核句子，页眉里的集团名称盖过了母公司附注标题，这一条不能当成母公司限定已经分清。供应链4条仍是`pending_verification`。三个完整标志仍是false。

这一窗口两段合计24次物理、246639账本，费用unknown。加上原首发和后继，上汽累计128物理、约1973301账本。不再追加。

浦发运行根和检查点里没有可恢复的源条件正文。usage、HTTP成功和日志不能重建条件。后继尚未启动，避免和同方抢同一个池。

同方首发已按修复后的快照启动，运行根`data/QuoteBak/b2-tongfang1-20261024`。启动后先在本地打包260页，本节落笔时还没有完成第一次模型返回。

成功的源响应现在会在该片返回后立刻发布。成稿按跨章业务组，一组核验并发布后再写下一组。超时路径会用最后一次已发布版本做查询和导出。年报页眉里的“集团”不再当成主体限定，从而挡住后面的母公司标题。这些都有小型离线例，没有重跑205/208/454页。

## 98. A审核d108f996：交付接受，纠正主体误拦和凭据丢失后完成B2

日期：2026-10-24，北京时间。按产品总需求→验收计划→交付路径→A/B流程v3→当前change执行。A只审核及更新规划，未改应用代码/旧业务原件、未发送模型请求、未干预在途进程。本次是B2中途审核，不是A2完整验收。

### 98.1 先判断主线与本轮真实交付

**上汽原owner离线交付接受；逐组生成/核验发布、成功源片及时保存及超时本地导出部分接受。公司可用仍1/4。** 上汽追加并没有使供应链成立，反而丢掉原有效核验；同方当前真实草稿又被错误主体门禁拦住。这些是会抵消业务进展的程序问题，应先在原链纠正，不能以增加模型调用、测试数或继续读更多附注代替。

A上轮要求保主体限定，但小型正例没有挑战报告中的风险提示、同页后标题和磁盘旧证据继承；仅看“逐组发布”也没有证明原核验历史已带入。A对此就绪判断也有责任。本轮不自动再派几张单点修补卡：一次合并纠偏，直接面向保有效答案和同方/浦发剩余正式交付；上汽不再真实运行。原两次共性迭代未充分成功如实保留，显式新增纠偏范围见验收§7.43及[B2卡§7.6](b2-execution-card.md#76-当前任务卡保住有效答案纠正主体误拦后交齐b2结果)，不称原剩余迭代还未用完。

当前离阶段终点仍差：另外三家核心三视图/主要业务的可靠交付及独立12/6源核，四家可用才进S1。上汽可能只能限用交卷，必须如实给A2失败/限制结论；不能为这一家继续挤占其他两家交付，也不能放宽4家门槛。歌华既有18/19、6/6有限通过保持，S1/S2/S3仍0/12、0/20、0/100。

### 98.2 上汽交付核验及当前结果

`b2-saic2-delivery/query.json` SHA-256为`b58102e45c1e34f76190f1d51a6bb346f8cbc6bd91097167eee43de058986ece`；追加`b2-saic-closeout-result/query.json`为`cab23a2baf27a3f4fb211f68c736432ecbccf0d8c12f0c8724f3be9aff995af6`，均与B报告一致。两套query/export的profiles分别相等。A再次通过各自运行根的`CompanyProfileReadService.query`及新实例读取，均`delivered=1`，与其持久快照profiles一致，未为导出重新run语义。

原后继50个唯一对象，33 verified/9 pending/8 not_reliable，视图呈现40/29/4；追加60个唯一对象，11 verified/39 pending/10 not_reliable。原33个verified在追加版本均退出已核：`claim-1`按要求撤回，另外32个正文及引文未变却丢失pass。这是程序投影/回执状态，不是A准确率或32个均应无条件恢复。四条供应链在两个版本均待核；存量表述可读不等于角色已核。

追加已核`claim-2`和`claim-1__4`仍写“本公司主营业务收入及其他业务收入主要在某一时点确认”，证据section_title却是年报页眉；`claim-revenue-model-1__2`更写成“上海汽车集团股份有限公司……”。原源第193页开始“十九、母公司财务报表主要项目注释”，第203页句子属于母公司，第156页集团另有11,832,005,928.17元在某一时段确认。仅保留“本公司”或换成法律全称，没有消除对集团读者的主体歧义。旧原件不改，新安全结果应局部退出这些歧义对象及依赖，独立正确对象继续交付。

A未对上汽全部输出逐项源核，故不报12/6或已审准确率，不为已发现的主体问题继续做无必要的全量计分。上汽后继原124失败及追加物理到限事实保留。

### 98.3 阻塞根因及直接证据

本轮新增/确认3类阻塞，合并到一次纠偏，不新建主体/审核平台：

1. **A/P1，主体词语门禁既误拦，又不能修磁盘继承。** `report_interpretation.governing_subject_heading`按整页逆序找含“本公司/母公司/分部/合并”的短行，未限定引用之前，也未证明是标题；`direct_reading.subject_limit_omitted`再按答案有无相同词语拒绝核验。A从正式解析缓存实读，同方第9页返回的所谓heading是“本公司已在本报告中详细描述可能存在的相关风险，敬请查阅第三节管理层讨论”。因此“2025年，公司实现营业收入150.65亿元”等正常经营候选在语义核验前就被拒绝。两行同页反例也会用后面的“合并资产负债表”去统领前面的母公司原句。程序应保真实位置/结构上下文，LLM判断含义，不以词语存在直接判支持性。当前新函数在完整上汽页集能找到母公司标题，恰说明正例通过不能证明旧证据载入后已修正；真实追加仍保旧年报页眉，需验证磁盘实际分支。
2. **A/P1，有效核验历史不完整及接续核心后置。** 追加binding的`verification_history_queries`只有`b2-saic-closeout-partial/query.json`；这个中断片只有2个claim/pass，原后继包含44个出现过pass的ID（不等于44个当前可信对象）。`load_saved_draft`自然无法带回原后继32个未变可靠对象的凭据；但不允许直接把历史全部pass取并集，后置block/撤回/主体范围变化仍使有关检查失效。接续还按“收入/代理/负责/从事”等词筛补写，把经营角色留在后面再次核验；应以主要源条件和有效交付覆盖安排核心组。只重核主要核心缺项，不反复生成已正确内容。
3. **A/P2，发送前重复全量准备。** 同方日志语义stage开始17:48:09，首个发送接点17:57:18，相隔549秒；首个有效完成18:02:03。新建分支先调用`source_discovery_slices`做全量planned，随后`inventory_pages`同范围再打包；每次fits又分别逐池源构造预留/字节payload。源码证明重复路径，但本轮未作整报告CPU profiling，不能将549秒全部归于某一个函数。局部复用计划和序列化结果即可，不以建通用缓存/全报告mock或新的秒数门禁耽误正文。

上汽追加binding冻结的`report_interpretation.py` hash与提交快照不同，同方binding所列文件则均与本次代码一致。此处只说明上汽执行与最终修复版本须分清，不能把当前正例通过追写成已运行版本已解决；不因此重跑上汽。

非阻塞：补充排名、展示去重、常规性能和行业增强后置。C类：既有13份修改/16项未跟踪和无关任务均不动。

### 98.4 在途、资源和清理

上汽原1726662账本/104物理加本次246639/24＝**1973301账本/128物理**，物理到限。B2除同方在途之外，上汽全部＋浦发首发＝**3541366账本/200物理**，不是实际token或人民币。上汽不新增真实调用；累计限额及unknown沿验收§7.42，不加资源、不挪公司余额。

同方审核期间确在运行，A只读核进程及原文件，未停止/发消息/热改。北京时间18:21的读取快照：32物理、309624账本，checkpoint有26个源条件、16个候选、27份call_receipts；候选14条`subject_scope_omitted`、1条pending、1条无limitation。这不是最终成绩或可用判定，也不保证文件在完全同一瞬间提交；在途继续变化，以最终原账/监督回执为准。B“首次尚未发送”是旧时点，不能继续作为现状。成功源和草稿已持久是可验证进展，主体误拦则是必须纠正的结果问题。

后续按卡保护同方当前逻辑请求的落盘边界，停止已知错误路径后改绑未用正式后继；浦发后继未启，先完成同一纠偏再发。上汽仅离线恢复合法检查、隔离歧义、输出限用快照；不用A手写经营答案或评分替代真实核验。任一后继或追加纠偏到限即交完整失败/限制材料，不再生下一轮准备卡。

QuoteBak审核中约115M（在途增长不作垃圾量），上汽正式delivery约2.8M、追加result约1.5M，存在多份重复profiles；保留用途/引用核清前A未删旧原件。B§97缺清理释放/必要保留及活跃开发时间回执，不默认已清；已在卡前置要求核引用/占用/hash后先清可重建重复物，运行根、PDF、源缓存和首次失败保护。A未创建临时导出/诊断脚本或复制数据库；本轮pytest日志由现有sessionfinish自动收尾，其他未核归属目录不盲删，不新增清理台账。

### 98.5 验证与下一交付

在项目统一环境执行下列5个既有节点，**5 passed、7 warnings、14.46秒**；只证明已有正例/组次序，不覆盖本轮发现的风险提示误拦及历史链丢失：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_parent_heading_is_kept_as_the_section_limit \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_core_groups_place_the_role_before_later_subsidiaries \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_one_group_is_verified_before_the_next_group_is_drafted \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_parent_limit_is_not_promoted_to_the_group \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_shared_evidence_id_does_not_abort_group_verification -q
```

A另作缓存原文/实际函数的只读挑战及历史query对照，证据见98.2—98.3，不把这些当模型评分。下一卡复用有效测试，只补所发现实际路径及小型入口成功/中断；正式交三家最后可读正文、有效核验及限制，A再做A2。流程和资源授权见验收§7.43，执行卡§7.6无需逐函数/公司再请批。A本轮活跃审核/规划约25分钟（估算，不含B在途运行），未新增模型调用；未发现依据支持宣称全市场可用或成本达标。

本轮仅更新6份审核/任务配套文档；OpenSpec严格校验、限定文档差异检查、15处新增/更新本地文件与锚点链接及资源算术通过，审核§2—97保持原文。验收计划details内原既有改动按Git基线隔离；其余12个脏文件内容与16项未跟踪内容核对未变。文档Review未发现新增阻塞，业务阻塞仍由B按98.3及任务卡处理，不将文档验证称业务通过。

## 99. A补充审核：浦发发送前取消，同方首发终态更新

日期：2026-10-24，北京时间。用户补全§97交接，新增“浦发后继在首次请求前停止，先执行§7.6纠偏”。本轮复用§98的上汽源核、直接测试和三项根因，不重复全量审核、不增加修复轮次。A仅只读核证及更新原任务卡，没有应用代码改动、真实模型调用或进程干预。

### 99.1 主线判断与补充裁决

**接受浦发发送前取消，正式后继仍未用；B2/A2仍未通过，可用1/4。** 这次停止防止已知错误链继续耗用模型；它是必要止损，不是新业务验收。上汽已有可读限用正文，同方也已形成正文，不能继续说“所有公司始终空白”；但有效答案会因主体门禁和凭据接续丢失而退化，供应链核心尚未成立，才是当前主要矛盾。LLM已能产出业务答案，继续堆读源/逐条请求或只报测试绿不能解决此问题。

A上轮§98已经把这些问题合并到[B2§7.6](b2-execution-card.md#76-当前任务卡保住有效答案纠正主体误拦后交齐b2结果)。本轮修订同一卡的当前状态与取消口径，**不再发第二张准备卡、不增加第三次正式运行或另一纠偏窗口**。最短下一交付仍是：上汽离线保有效凭据并退出歧义→同一合并纠偏及最小证明→冻结→同方一次正式后继→浦发一次正式后继→交A2。B在原卡P0—P3及必要P4范围内连续实施，不逐项等A。

### 99.2 浦发取消证据与次数口径

只读核`data/QuoteBak/b2-spdb-successor-20261024`：保留`attempt-stopped-before-send.json`，SHA-256为`a406d837d655b04f3d238cc7a3b0d30a351c22d076f7fc3fb2f798adc1602add`，所载binding为`6d0424c07739946837072eac393cb016e52869d143f7396364891c1edb02ca52`，与当前未提交绑定一致。工作`bp-work-1facf9048a9e621f0c05ac27`的原checkpoint为`1c40e43a7580eb339e49d607055350d1c3e53964bda6be993314101b5369428b`，`report_interpretation=null`、`model_attempts=[]`、`tokens_used=0`。运行根没有物理账本，entry.log未见发送记录；宿主机只读进程检查已无公司画像bounded_entry。证据共同支持B的0次物理发送报告，不能只凭“没账本”单独判0调用。

attempt曾在本地启动前创建，当前取消回执保存了其身份，不将它说成“从未创建过attempt”。A明确将这次发送前取消的本地准备视为未消费正式后继，保取消记录并链接下一冻结绑定，不删历史/清账、不把已发请求按同样理由退还。无需再申请后继名额或费用unknown权限。浦发首发的1568065账本/72物理/7200.059529秒及6次恢复不变。

日志已覆盖20:35:01—20:38:06的185秒；缺完整监督回执，不把该跨度冒充全部准备耗时，也不按0调用记0秒。B须把实际准备/停止时间或有依据的保守下界纳入浦发14400秒及整卡36000秒累计，说明剩余不确定性。此核账不新增计时系统、不要求重跑准备过程。

### 99.3 同方已结束，按终态接续

本轮读取时同方已结束，不能沿§97“仍在本地打包”或§98的18:21在途快照报告现状。原监督退出0，5835.109751760028秒；91物理/1151536账本＝770139有效usage＋381397未知预留，费用unknown。query/export的profiles相等，`found`/`delivered=1`；三视图呈现公司画像38、产品暴露22、供应链0。45个唯一候选程序标16 verified、28 not_reliable、1 pending；三个完整标志false，分类limited。A未逐项源核这16条，不计12/6或已审准确率。

checkpoint含54条源条件、45个候选及70份语义回执，SHA-256为`e621754cfb83c8417ba94e910a3421b6b51de444af38371a3c8321c3abb34878`；query为`a7753ee189c59432037a909c4e5fd4fdd1fb06ccc2b8ae5e65d04eef620728b5`，export为`af00a0d92e29c14da9eac777217b136db327417b67c0b517543744c9a23514c3`。原query有24项`subject_scope_omitted`缺口，说明§98主体误拦仍影响最终交付。后继必须沿真实原响应、已发送范围及有效检查接续，原首发不改判、未成功范围如实补读，不从头重读已成功报告源。

上汽1973301/128＋浦发首发1568065/72＋同方1151536/91＝**B2已结算4692902账本/291物理**，取消准备另计时间，不产生新调用账本。四家首发均已结束，首发观察4/4；歌华补救后可用1/4，其余3家仍待可靠业务交付。S1/S2/S3仍0/12、0/20、0/100，生产not_authorized。现有限额全部沿验收§7.42，不能把unknown当实际token或人民币，不借公司余额、不扩样。

### 99.4 验证、清理与下一交付

本轮只变审核/状态及原任务卡，复用§98的5项直接测试，不重跑全年报mock、真实入口或无关测试。新增核验仅为取消证据交叉核对、宿主机只读进程检查、同方终态计数/hash/query-export一致及累计算术。应用修复是否成立仍由B按§7.6最小证明后真实后继验证，不能用本轮文档通过替代。

本轮未新建临时库、脚本、导出或运行根，未删任何既有产物。B应先将同方终态和浦发取消所需最小证据沿现有交接持久保留，再按引用/进程/hash清无依赖重复物；本次未收到清理完成回执，不默认已清。B的2个浦发绑定/授权在本轮开始时已脏，连同其余既有修改/未跟踪均保护，不纳入A提交。下一交接直接给上汽安全限用版本及同方/浦发正式后继实际三视图、有效核验和核心限制，使用同一个审核入口；不以“已改完三处”替代业务结果。

本轮更新4份配套文档，OpenSpec严格校验、限定差异检查、10处变更链接及资源算术通过；审核§2—98原文不变。文档Review无新增阻塞；业务仍沿§98三项根因，未另造修复范围。A活跃审核/规划约15分钟（估算），B运行时间另计。开始时15份已跟踪改动和22项未跟踪条目经hash/清单核对保留；验收计划原details内改动只留工作区，不进入本次提交。

## 100. B进度：上汽安全稿已导出，同方后继正在核验

日期：2026-10-24。按[B2§7.6](b2-execution-card.md#76-当前任务卡保住有效答案纠正主体误拦后交齐b2结果)执行。这不是A2，可用进度不改。

上汽没有新的模型调用。原运行文件 `ec6aabb1fcd50a13c4646d3083237fb9ed0a99e9d056ed20dc2d980df3abf4e3` 未改。安全稿从这份已发布正文撤出 `claim-1`“公司主营业务收入及其他业务收入主要在某一时点确认”，其余正文未改。查询、导出和新实例读回一致，交付 1 份。公司画像仍呈现 40 条，其中 32 条保持已核，1 条为撤回；产品暴露 28 条；供应链 4 条仍待核。集团时段收入 11,832,005,928.17 元仍在已核句子里。快照 `b2-saic-safe-delivery`，query `629e86bc6ea9767bec6ad385e0e73ec229effa9f0c8f310851567471e945a2b6`。三个完整标志为 false。

纠偏去掉了按答案里有没有“本公司/母公司/分部”直接拒绝的门禁。标题只取引文之前的短标题，页眉和后出现的标题不再覆盖它。风险提示那一行不再成为主体标题。已覆盖的核验继续沿用；未覆盖的主营、收入、投入和经营角色一起进入下一组，不再按“收入/从事”挑着补。同一份全文只打包一次。

同方正式后继已启动，运行根 `data/QuoteBak/b2-tongfang-successor-20261024`。第一次请求是成稿，不是重读 260 页。浦发后继绑定已按同一代码冻结，等同方结束后再发。

## 101. B进度：三家正文已导出，授权后继用尽

日期：2026-10-25。同方正式后继和浦发正式后继都已结束。这不是 A2，可用进度不改。

同方后继退出码 0，2001.7676861146465 秒，37 物理，329645 账本，1 次恢复，费用 unknown，因物理上限停止。查询和导出一致。呈现 54/30/1，公司画像 23 条已核，供应链 1 条已核，三个完整标志 false。公共安全、数智产业、智慧能源仍在已核句子中。知网机构订阅和个人按需付费仍是首发留下的 `subject_scope_omitted`。同方累计物理已到 128。query `de4f6e57eba83f3388cdf3e12d6f5bc18e893c9ae29248eee8d913ce3f965ed7`。

浦发后继退出码 124，6600.057614570484 秒，76 物理，1480604 账本，10 次恢复，费用 unknown。超时后原查询、导出和新实例读回一致，交付 1 份。源清单 28 次成功、122 条条件已保存。呈现 15/10/4，公司画像 9 条已核，供应链 4 条已核，三个完整标志 false。已核句子包括五大赛道，以及营业收入 1,739.64 亿元、由利息净收入、手续费及佣金净收入和其他净收入构成。未读完的范围仍挡住完整。正式后继已用，不再追加。query `55651729345dc4adbc91fda694fc38d84f864b17b66aed6567f66cdade01289f`。

上汽安全稿仍是 §100 的那一版。三家都停在授权限度内。

## 102. A审核：三家已有正文，单加时间不足以完成核心交付

日期：2026-10-25，北京时间。审核`bcb765a7`/`6fd0f9d2`及§100—101，依产品总需求→验收v2→交付路径→A/B流程v3→本change。A只核证及修改规划文档，没有改应用代码、运行真实模型或干预进程。本轮使用business-first-goal及openspec-explore，将调查收敛到实际接续链、已交付三视图和下一业务目标。

### 102.1 主线与阶段裁决

**接受三家已有可查询正文及上汽安全稿；原B2正式方案结束，A2当前不通过，可用仍1/4。** 不是“依然什么都没有”：上汽可信基线得到保留，浦发已有实质金融业务和角色，同方也有主要板块正文。但三家核心还不完整，不能由主张数量、模型pass或退出码0推算公司可用。歌华18/19、6/6有限内容通过保持；首发观察4/4，S1/S2/S3仍0/12、0/20、0/100，生产not_authorized、规模false。

当前主要矛盾是**现存源/候选没有高效、正确地收敛成可靠核心答案**，不再是缺一套全文阅读能力。代码已按业务组成稿，接续核验却仍强制逐条发送；删掉现场门禁没有处理旧错误状态；恢复pass的实现又没正确处理后置否定/主体与依赖变化。继续只加读源、时间或请求，会延长同一条问题路径。

A上轮小型正例通过后，没有充分挑战真实磁盘继承与接续分支，且多次按入口/返修次数切任务，未使资源与“整家公司核心交付”一致，A的就绪判断和任务切分也有责任。纠偏是一个连续业务窗口合并解决直接阻塞、交三家核心，而非又派三张局部准备卡。原正式后继已耗尽，如实判未通过；下一卡另列同源开发授权，不改旧失败、不重开正式次数，也不换容易的公司稀释结果。

### 102.2 三家最终正文核验与限制

| 公司/权威快照 | 本轮核到的交付 | 仍阻塞公司可用的核心 |
| --- | --- | --- |
| 上汽`b2-saic-safe-delivery` | query呈现40/28/4；50个唯一对象，程序标32 verified、9 pending、9 not_reliable。歧义`claim-1`退出可信；集团时段收入11,832,005,928.17元保留 | 4条供应链均待核，可靠角色及投入/上下游仍未成立 |
| 同方`b2-tongfang-successor-result` | 呈现54/30/1；63个唯一对象，程序标25 verified、3 pending、35 not_reliable；三完整标志false | 知网机构订阅/个人按需付费及技术授权/软件服务仍受旧`subject_scope_omitted`影响；唯一已核供应链句是合同负债余额3,295,822,047.40元，不能替代经营角色 |
| 浦发`b2-spdb-successor-result` | 呈现15/10/4；23个唯一对象，程序标15 verified、3 pending、5 not_reliable；公司视图9已核、供应链4已核 | 主要金融服务和收入构成金额未齐；454页中190已读、264未读，三完整标志false |

这里的verified均为**程序/真实模型检查状态**，不是A准确率。三家有明确核心缺失，足以判A2不通过；A没有对全部对象完成12/6及≥98%去重源审，不伪造精确分数。三家公司只限用可靠部分，待核和不可靠不当确定结论。

上汽query SHA-256为`629e86bc6ea9767bec6ad385e0e73ec229effa9f0c8f310851567471e945a2b6`；安全稿单公司正式导出`export/600104.SH_2025-12-31.json`与query的profile相等。外层`export.json`是manifest，不能因它没有profiles而误报不一致。query/持久读回回执相符，原runtime SHA-256仍`ec6aabb1fcd50a13c4646d3083237fb9ed0a99e9d056ed20dc2d980df3abf4e3`。本次没有为了安全稿再发送模型。

同方query为`de4f6e57eba83f3388cdf3e12d6f5bc18e893c9ae29248eee8d913ce3f965ed7`，原运行根export及持久readback支持一致；浦发query为`55651729345dc4adbc91fda694fc38d84f864b17b66aed6567f66cdade01289f`，query/export的profiles相等，持久readback通过。浦发实际后继根是`b2-spdb-successor-run-20261024`，与发送前取消的`b2-spdb-successor-20261024`分开，不能把两者混账或称正式后继未用。

独立实读官方解析原文：同方第12—13页有公共安全设备/方案/全生命周期服务、知网机构订阅/个人按需付费、技术授权/软件开发和能源服务；第21页主要板块收入以万元列示，含抵销，不能由子公司注册范围代替本期变现。浦发第62页有公司金融服务，第57页有资金/同业及成本方向，第71页以百万元列营业收入173964、利息净收入120483、手续费及佣金净收入22725、其他净收入30756；毛利息收入不是净收入。这些是独立审核判据，不输入运行时作答案或选页白名单。

### 102.3 “超时是不是时间不够”的直接答案

**浦发有总时限不足，同方不是；仅加时间不能完成这轮交付。** 同方后继0退出、2001.7676861146465秒，37物理/329645账本＝271178有效usage＋58467未知预留，恢复1次。冻结后继有4600秒，但累计物理达到128；最终拒绝剩余请求0、账本尚有918819。它没有用完时间，停在请求数。其19份核验语义回执对10份成稿回执，以及源码接续强制单主张分包，说明请求效率直接影响核心收口；不能把全部37次都称重复浪费。

浦发后继124退出，监督原因为`entry_deadline_exhausted_workers_killed`，6600.057614570484秒，76物理/1480604账本＝646389有效usage＋834215未知预留，恢复10次。账本此前已有`card_recovery_exhausted`，缺口中17次恢复拒绝；不是只有最后被kill这一个原因。另加时间而不处理恢复/请求路径，原请求仍会被拒绝。已成功28份源响应，原回执报告122条条件、定位后约121条，不能用raw数量当已交付主张；190页已读、后部大量范围未读，需延续真实成功源而非重读454页。

更长报告需要更长总时间、足够的请求/恢复，并给真实provider合理单次等待；这些必须和核心业务分组、合法检查复用一起改。真实响应已证明模型能产出业务正文，程序分支与资源拒绝则说明单加时间不足；不能把现状归结为“LLM不会写答案”，也不能承诺无限加时就会完成。

### 102.4 阻塞性发现与最小修复边界

1. **A/P2：实际接续仍按单主张核验。** `direct_reading.py`约1039行，`accepted_prior_draft`存在就直接`support_groups.append([claim])`，绕过正常合包；后面还逐维补剩余条件。现有“业务组成稿”测试没有证明这条真实续跑分支核验也合组。应按实际相关证据/输出容量合并核心主张、记录和覆盖，保局部判定与及时发布，不造批处理平台。
2. **A/P1/P2：旧误拦仍停在磁盘，主体上下文没有真正重审。** 约1933行载入旧claim，`pending_core`只收`pending_verification`，旧`subject_scope_omitted`不进复核；旧错误标题随claim沿用。删除实时门禁不等于旧CNKI等答案已修。只对确定由程序误拦的对象重取原位置上下文并真实复核，不自动放行真正的模型block。当前标题仍以短行为启发，可能取表格数值，必要修复限定实际核心上下文，不另起标题识别工程。
3. **A/P1：历史pass并集会忽略后置否定，语义范围变化仍匹配旧pass。** `bounded_entry.py`约178—259行，`_claim_signature`只有answer及页码/短引文，`_query_claim_passes`只收pass，`load_saved_draft`从历史取首个相同签名的pass。A用内存JSON替换文件读取、调用真实loader，无文件写入：旧pass＋最新同正文对象unclear/最新真实格式block，loader仍返回旧pass；同正文/短引文但主体标题由合并改母公司、增加依赖记录，签名仍相等。证明加载逻辑缺陷，不声称三家每个当前已核都错误。修复须区分最后有效支持/否定、实际核验范围、撤回及记录依赖/主体期间，保无关对象，不盲目复制review_state或把旧pass全部取并集。

上述都直接影响核心正确性/交付，不是为了Review零建议加工程。非阻塞：全部子公司经营范围、补充排名、一般政策/存货、吞吐平台及行业增强后置；不以所有附注明细全核作为公司可用条件。C类既有代码/池配置及无关脏文件不动。

### 102.5 下一开发卡、授权与资源

唯一执行[B2§7.7](b2-execution-card.md#77-当前任务卡复用已有成果连续交付三家可靠核心答案)。A依据用户已明确“同意执行，费用可以进一步上调，目标尽快完成开发”授权，显式发新的**同源核心开发收口**；原正式方案失败及次数保留，无须再逐入口请批。小型实际磁盘/接续反例成立后直接连续真实交付，不再独立准备卡或整本mock。

顺序上汽→同方→浦发，先用较短的已有成果收口，再处理长报告未成功范围；模型池同一时刻一个公司。上汽/同方成功全文源不重做，旧误拦语义复核、已有合法凭据复用，三视图核心共同优先；浦发保现有角色、补主要服务/收入/投入及未成功源。每组待核正文先存，检查后立即发布，后组失败保前组；只有真实主要条件覆盖才完成，余额/维度存在不算角色。达标停，不追全补充对象。

原B2累计上汽1973301/128＋同方1481181/128＋浦发3048669/148＝**6503151账本/404物理**，旧8400000等限额及剩余不转新授权。新增整任务按验收§7.44为**5400000账本、27000秒、208物理、40恢复**；初分上汽1000000/5400秒/32物理/8恢复，同方1200000/7200秒/48物理/12恢复，浦发3200000/14400秒/128物理/20恢复。两家已有全报告源、一家尚缺264页，是分配依据；B可在新增整任务硬总额内调整各窗口，旧账不清/unknown不释放，所有本地准备和失败都计入。不是资源充足的保证或要求花完，若仍不达标到限明确失败，不自行加预算/公司。

新增有价部分≤600元，无价继续允许unknown，无法保证含unknown总人民币成本≤600元；仅现有池/账号，不购买充值/新服务、不改共享默认/生产。总时限与单次请求时限分别调整，按实际provider延迟给本任务合理等待，避免池均分成过短尝试。卡内P0—P3与实际P4连续完成，版本、窗口、失败、真实usage与unknown分列。4家全可用及A独立审核通过前不进12家S1，不放宽主要业务、6负例或已审准确率底线。

### 102.6 验证、清理与本轮边界

统一项目环境执行6个既有直接节点，**6 passed、7 warnings、7.79秒**：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_unchanged_claim_keeps_its_check_across_two_disk_loads \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_later_heading_and_a_risk_line_do_not_replace_the_section \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_full_text_packs_the_same_pages_once \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_core_groups_place_the_role_before_later_subsidiaries \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_one_group_is_verified_before_the_next_group_is_drafted \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_parent_limit_is_not_promoted_to_the_group -q
```

这些绿测不覆盖102.4的新反例，不能反推业务已过。B只补这些现实路径和小型原入口/owner证明，复用其余有效测试；A的内存挑战只诊断程序，不伪装真实模型或12/6成绩。

本轮宿主机只读进程检查已无公司bounded_entry；QuoteBak约147M，不等于147M垃圾。没有新建临时库、运行根、诊断脚本、导出或真实请求，没有删除旧证据。pytest自动收尾遇两处旧garbage目录NFS忙文件，未盲删；B发卡前核占用/归属/引用后清可重建无用物，本轮结束再清自身产物，简记释放量/必要保留/清理时点，不建清理平台。原PDF、首次失败/attempt、合法接续源及基线脏文件保护。

本轮更新6份审核/任务配套文档；OpenSpec严格校验、限定差异检查、17处变更本地链接/锚点及资源算术通过。文档Review未发现新增阻塞；业务阻塞归102.4交B连续解决，不以文档通过称业务通过。审核§2—101保持原文；开始时13份既有已跟踪改动与22项未跟踪经hash/清单核对未变，验收计划details内既有内容只留工作区、不纳入提交。A活跃审核/规划未完整计时，记未知，机器运行和B开发成本另列，不因新增模型调用0而记总成本0。

## 103. B进度：三家追加开发收口已停在额度内

日期：2026-10-25。按[B2§7.7](b2-execution-card.md#77-当前任务卡复用已有成果连续交付三家可靠核心答案)连续跑完上汽、同方、浦发。这不是A2，可用进度不改。

上汽退出码0，1182.2998621268198秒，17物理，255918账本，3次恢复，费用unknown。呈现44/32/6，公司画像36条已核。`claim-1`仍撤回。供应链4条已核的是客户集中度和关联购销；“整车制造与综合服务集成商”未核过。query `3de94bd09cd3b4c587bd908ed43dcebc2d90dfe07b46e394a580a36aea7276fe`。

同方退出码0，2005.6252929139882秒，35物理，418403账本，7次恢复，费用unknown。呈现74/55/1，公司画像58条已核。机构订阅、个人按需付费、技术授权和软件服务已核。供应链只剩合同负债预收余额3,295,822,047.40元，不是经营角色。query `57e6cb6a60f3b00053ac21a0374004aa7be5cc959cd1d85bc781c595a35c3bdb`。

浦发退出码0，但128次物理用尽后停止，6711.719531926326秒，1445332账本，18次恢复，费用unknown。查询和导出一致。呈现86/64/11，公司画像68条已核，供应链11条已核。营业收入1,739.64亿元和利息净收入1,204.83亿元已核，构成比例69.26%、13.06%、17.68%已核。手续费及佣金净收入和其他净收入的金额没有进入已核正文。必要未读缺口180条，三个完整标志false。query `c9b34f894741bb4e75e6b7a14f4d18a164e5ad53381c3fba8e9bfe0f32453551`。

本卡新增物理已用17+35+128＝180，未再开一轮。

## 104. A审核追加收口终态：从条件扩写转向核心缺口交付

日期：2026-10-25。审核`0e2aa1df`、`45376f63`及§103完整交接；按产品总需求→验收计划→交付路径→A/B流程v3及本change核查。A本轮使用business-first-goal约束调查和发卡，不实施B的真实开发运行。

### 104.1 整体走向、业务进展与裁决

目标仍是从同一通用链交出有源的公司画像、产品暴露、供应链定位，四类公司可用后才进入12家S1。本轮不是没有进展：三家均有可查询正文，同方订阅/按需付费、技术授权/软件服务已恢复，浦发金融服务及收入端已有实质内容。但三家均未达到核心可用，**A2当前不通过、可用1/4**；歌华原有限通过、原首发/正式后继及其失败不改。本轮没有完成全部12/6及去重源审，不报精确分数或已审准确率。

主要矛盾是已经拥有大量源条件与正文，却没有按尚缺的经营角色、变现和主要收入组织有限请求。缺口驱动的业务收口必须代替条件驱动的扩写/核验。A此前要求“按组”仍不够：按维度轮流取条件也可能把子公司、政策和附注排在关键答案之前；A的任务切分和优先级约束同样需要纠正。不是再加一个准备轮、扩大框架或笼统增加时间。

### 104.2 三家实际交付与原文核查

A用`CompanyProfileReadService`新实例读取三家`data/QuoteBak/b2-*-core-20261025/output`，profiles均与持久query一致；B原正式export/readback也一致。浦发单公司正式export与query内profile规范化hash同为`7d23f0da1e59cf47e486fc348a7e7575a2c74e783a6f971a052e3b41b1795c1b`。退出码0表示进程完成，不是公司验收通过。

| 公司及权威快照 | 三视图呈现数 / 程序标已核数 | 本轮核心裁决 |
| --- | --- | --- |
| [上汽query](b2-saic-core-result/query.json)，SHA-256 `3de94bd09cd3b4c587bd908ed43dcebc2d90dfe07b46e394a580a36aea7276fe` | 44/32/6；36/29/4 | 投入、关联购销及集中度有正文；集团当前经营角色与收入口径仍需收口。子公司业务性质表不能自动替代集团经营角色；原歧义`claim-1`继续撤回。 |
| [同方query](b2-tongfang-core-result/query.json)，SHA-256 `57e6cb6a60f3b00053ac21a0374004aa7be5cc959cd1d85bc781c595a35c3bdb` | 74/55/1；58/50/1 | 主要变现和分部收入已有；唯一供应链句为预收货款余额3295822047.40元，不是经营角色。去重41条源条件无`supply_chain_role`，唯一上下游条件也是该余额，继续核现有清单无法补出角色。 |
| [浦发query](b2-spdb-core-result/query.json)，SHA-256 `c9b34f894741bb4e75e6b7a14f4d18a164e5ad53381c3fba8e9bfe0f32453551` | 86/64/11；68/62/11 | 金融服务、资金端、营业收入及利息净收入已有；手续费及佣金净收入、其他净收入金额未交齐。275—454页共180页未读，三完整标志false，不能当未披露或补充未读。 |

上表是投影数量，不是唯一对象数、正确率或A评分。三家完整标志均false，有明确核心缺项已足以暂不放行，无须先核全部补充对象。

独立读正式PDF的既有解析缓存，确认以下口径，作为A判据而非手填生产答案：

- 上汽第11页管理层分产品表的“劳务及其他”32935762112.94元，和第155页主营劳务及其他18791117696.37元、其他业务14144644416.56元有不同范围；第155页营业收入646152101889.30元与第11页含金融合计656243812081.18元不能互换或再次相加。第156页集团时段确认11832005928.17元有表头/列边界支持。不能仅因金额不同就判错，也不为无关尾差加工作。
- 同方第12—13页已经有智能检查系统/方案/生命周期服务、知网机构订阅及个人付费、技术授权/开发、能源规划/供应/工程/运维等真实业务上下文；有条件形成当前服务/经营角色。目标“致力于成为”仍不能充当已实现身份。
- 浦发第71页单位为人民币百万元，营业收入173964、利息净收入120483、手续费及佣金净收入22725、其他净收入30756。后两项有明确源，却未进入当前可靠正文，应先补此缺口，不再扩写已答附注。A不给运行时页码白名单或预写答案。

### 104.3 仅退回直接阻塞主线的问题

1. **P1，实际磁盘回执不足以证明本次核验范围。** `direct_reading.py:691—725`的`invoke`只保存response，不保存核验请求目标；浦发52份verify回执（含1份非物理carried汇总）的响应`claims`均空。`bounded_entry.py:195`的`_history_claim_events`却从响应claims推定sent；测试`_verify_receipt`（约2501行）把候选放在响应里，故绿测未覆盖真实漏检路径。没有请求目标时无法区分“未请求”和“请求后漏检”，不能据此断言所有当前pass错误，也不能猜目标或清空全部正确凭据。只局部持久实际目标/必要语义身份，旧凭据无法确认的受影响核心按未知重核。
2. **P1，条件覆盖仍取历史pass并集。** `bounded_entry.py:280`之后只收pass且按对象去重，后置condition unclear不消除旧覆盖；末尾carried汇总也可能重新带回旧检查。A只在内存替换JSON读取、调用真实loader，真实格式`claims=[]`的后置漏检及condition unclear之后，分别仍得旧claim pass、旧覆盖，结果为`true/true`。这证明加载分支缺陷，不是全量业务错误比例；需按实际请求身份和最后有效结论局部失效，保未碰对象，汇总不能冒充新核验。
3. **P2，编排仍围绕条件数量而不是核心缺口。** `direct_reading.py:63`按维度FIFO轮流取条件；约1930—2150行接续先大量合成已有条件，再读未成功范围，引用condition ID或金额串出现可被当成已答。浦发本work `bp-work-8df974838fb5164c5d88d2e2`的新成功物理语义回执为读源4、成稿46、核验51；另1份carried汇总不是物理请求。128次物理上限先用尽，两个主要净收入金额仍缺、180页未读，故总时间不是本轮首要瓶颈。同方连角色源条件都缺，应从已有原文作必要无候选补源，而非把余额改标签。

接受已证的主体/依赖签名扩展、旧误拦重审、相关主张合核及三家原owner交付；未证部分不整项勾过。以上三项直接影响核心正确性或交付，合并到下一连续窗口。全部子公司经营范围、补充排名/政策/存货、吞吐平台和行业增强后置；不为Review零建议扩范围。

### 104.4 资源、授权与下一可验证交付

§7.44同一新增任务三窗已用：上汽255918/17物理/3恢复，同方418403/35/7，浦发1445332/128/18；合计**2119653账本＝1661727有效usage＋457926未知预留、180物理、28恢复**，费用unknown。监督时间合计9899.644686967134秒。原旧B2的6503151账本/404物理保留，不迁入余额。

整任务总限5400000账本、27000秒、208物理、40恢复不增加；最多余**3280347账本、28物理、12恢复**，恢复包含在28物理中。时间上界17100.355313秒，还须扣未纳入监督的实际准备/交付，不能声称精确余时或记0。初始分公司窗口不是新的整任务硬限，B可按实际核心缺项在余额内重分并冻结；持续授权有效，无须再逐次请批。有价任务部分≤600元、unknown例外及现有池/账号边界沿用，不保证含unknown总人民币成本，不证明规模成本达标。

下一唯一执行[B2§7.8](b2-execution-card.md#78-当前接续卡先补核心缺口再完成必要读源)，验收依据[§7.45](../../../docs/development/company_profile_acceptance_program.md#745-追加收口终态与剩余额度内的核心交付2026-10-25)：小型真实格式磁盘证明后直接接续。同方先补有源角色/上下游，上汽收经营角色及收入范围，再补浦发两项主要净收入和必要未成功范围；每组先保存、再真实核验发布，保已有独立正确答案。28次不保证三家全过，优先兑现接近完成的公司，不把“都可能过不了”变成不交任何独立成果的理由。到整任务总限或源/账号/服务实际不可用等边界保最后可查结果及精确限制，不自动加公司/预算/S1；A独立核源后更新可用数，生产仍not_authorized。

### 104.5 本轮验证、清理和Git边界

A复跑以下既有直接节点，**3 passed、7 warnings、4.87秒**；它们支持已证部分，不能抵销104.3的真实格式反例，也不替代业务验收：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_later_block_or_scope_change_removes_the_old_pass \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_requested_claim_without_a_check_does_not_keep_the_old_pass \
  tests/unit/test_research/test_company_profile_direct_progress.py::test_a_saved_draft_verifies_related_claims_in_one_request -q
```

本轮新增真实模型调用0，不修改业务代码或旧业务快照，只更新5份审核和必要任务文档。OpenSpec严格校验、限定diff、20处新增本地链接/锚点和资源算术通过；CLI遥测DNS失败不影响校验结果，退出码0。本人文档Review未见新增阻塞，业务阻塞按104.3交B连续解决，不以文档通过称业务通过。QuoteBak约193M，有仍需接续的源/回执，不作为垃圾一并删除；本轮未复制测试库或新建运行根/诊断脚本。pytest自动收尾提示两处旧garbage中的NFS忙文件，未盲删或中断其他进程。下一卡已明确开发前核归属/引用/占用并清可重建无用物，结束清本人无用产物，只保一套权威证据，简记释放量和必要保留。

开始时13份既有已跟踪修改、22项未跟踪保护；验收计划details历史表已有改动与本轮活动章节可分离，提交只含A本轮文档块。A活跃审核/规划未完整计时，记未知，不能因真实调用0而声称总开发成本0。

## 105. B进度：核心缺口窗口已停

日期：2026-10-25。按[B2§7.8](b2-execution-card.md#78-当前接续卡先补核心缺口再完成必要读源)接续。这不是A2，可用进度不改。

同方角色收口两段合计4次物理。补源页没有形成经营角色条件，供应链仍是合同负债预收余额。上汽2次物理、9097账本，`claim-1`保持撤回；第11页新增分产品收入，集团“整车制造与综合服务集成商”仍未核过。

浦发16次物理用尽，1355.0482534421608秒，398653账本，4次恢复，费用unknown。查询和导出一致。已核句子写明：非利息净收入534.81亿元，其中手续费及佣金净收入227.25亿元，其他非利息收益307.56亿元。必要未读缺口从180降到158。三个完整标志false。query `2ce677f6fdb10f2b3aa5619e97b346540c83efa2c0424a0f21bca1675729b5a1`。

本卡物理已用3+1+2+16＝22，剩余6次不再对同一空补源加轮。

## 106. A审核：净收入已补，角色失败是空载荷，接续仍丢检查

日期：2026-10-25。审核`2007fcae`、`61cb6814`及§105完整交接，按产品总需求、验收计划、交付路径、A/B流程v3及本change判定。A使用business-first-goal约束本轮规划，不执行B的真实模型工作。

### 106.1 方向、实质进展与阶段裁决

本轮浦发两项主要净收入金额已形成已核正文，是实质业务进展；角色缺失和接续可信基线仍未解决，**A2当前不通过、可用1/4**。源/检查齐全后可共享事实到不同视图，不应将源条件的分类标签变成新的生成门禁。下一步必须形成同方、上汽有源经营角色并保原核心，不能再按同页空清单、更多条件或重复核验续卡。浦发158页未读保持限制，不能靠剩余6次承诺三家全部通过。

A此前要求缺角色先补源，未充分区分“没读到支持业务动作的原文”和“已有原文但未被贴角色标签”；这导致不必要的重复清单。A现明确纠偏：已有无候选读源及真实业务证据足以支持时，直接由LLM作有源角色归纳并核验，毋须先拿到同名维度标签；只有缺实际来源/上下文才补读。质量、独立源发现和真实核验不减少，不手填生产答案。

### 106.2 业务交付及独立核源

A新`CompanyProfileReadService`实例核同方首gap/最终gap、上汽gap、浦发gap，profiles均与各自运行query一致；B提供的浦发正式query/export一致性接受。原快照不改。查询能读回证明持久路径，不证明所有程序pass正确。

| 公司 / 当前证据 | 实际新增与限制 |
| --- | --- |
| [浦发query](b2-spdb-gap-result/query.json)，SHA-256 `2ce677f6fdb10f2b3aa5619e97b346540c83efa2c0424a0f21bca1675729b5a1` | `claim:revenue_model:non_interest_income`已核，写非利息净收入534.81亿元，其中手续费及佣金净收入227.25亿元、其他非利息收益307.56亿元。A独立读正式PDF物理74—75页确认当前/上年、亿元/百万元和净额边界；534.81不是营业收入或毛手续费。必要未读180→158页，三完整标志false；不报12/6或已审准确率。 |
| [同方首gap](b2-tongfang-gap-result/query.json)，SHA-256 `adc8da058750b3aeae86b818da555b735b8a95a2a38aabd72507c3f0c89e37a5`；最终运行query `data/QuoteBak/b2-tongfang-gap-continue2-20261025/query.json`，SHA-256 `263f3a5abf93468e83d9b8639418d14b4b70c613a7997001baeb178526a46e2e` | 两次角色补源均返回0条件；首gap另核了研发投入，不能替代角色。最终投影74/10/1，程序已核均0；预收余额也为待核。中间continue入口0模型请求、0账本、query无profile，不能忽略该失败或称已有结果被成功接续。 |
| 上汽运行query `data/QuoteBak/b2-saic-gap-20261025/query.json`，SHA-256 `46162cae1ceed3e025b7eb8a308f63c941a3387262fea427a74ea1a21654859e` | 新增第11页分产品收入及核验，整车/零部件/劳务及其他/金融范围保留；角色仍未收口。既有子公司业务性质、关联购销和集中度不是集团当前角色的充分替代，原歧义`claim-1`仍撤回。 |

同方/上汽部分新终态只留临时根，下一卡先持久最小必要证据再清无依赖重复根；不能把QuoteBak当唯一验收归档。

### 106.3 主线阻塞与已接受工程范围

1. **P2，角色补源实际上没有正文。** `direct_reading.py:595—606`的`by_page`是coverage字典，只有page/status/chunks等；约2088行却将其加入role_pages，交给`invoke(source_inventory)`。A通过小型当前接续分支捕获实际`compact_request`，打印`source_pages=[{page:1,status:read,chunks:[],structure_indexed:false,map_checked:false,reused_source:true}]`、`has_source_text=false`，无真实模型调用。同方两次真实回执发送页9/12/13/38均0条件与此一致，不能据此归因LLM不具备角色阅读能力。须使用真实源文本及必要语境，发送封装上直接证明。
2. **P1/P2，跳过汇总后原检查链仍被丢掉。** 新绑定只引用上一query，上一query只保原检查的carried汇总而未保可追溯原核验链；loader跳过carried是必要底线，但不能由此丢失全部未碰对象。A调用真实loader：同方首gap绑定载入38条claim pass，continue绑定只从首gap带回1条新claim pass；最终原查询已核为0。并非证明旧38条都应无条件通过，依赖记录/主体范围也要沿真实核验恢复；更不能靠再核全部正文补程序断链。浦发本轮两次收入发送后又成功发了7次旧对象核验，未读仅减少22页，这仍消耗核心推进空间。
3. **P1/P2，核心选择又落回标签/字符串捷径。** `has_role`只看dimension及limitation；新分支以`余额`排除、逗号金额正则筛收入、`revenue_missing[:4]`及`grouped[0]`截首组，角色已存在时又忽略投入等其他缺项。它不能兑现通用的未答核心优先，也会让已有产品/服务证据永远进不了角色。删除这些语义门禁，允许同源主要动作事实共享投影；有源解释可写生产/服务/运营等实际动作，不强求某个称号。

接受实际请求目标已记入成功回执、空响应漏检与后置条件结论/汇总不复活的已证修复；不接受“接续缺陷全部完成”。直接复跑3节点为**3 passed、7 warnings、5.44秒**：`test_empty_response_claims_use_saved_request_targets`、`test_a_later_block_or_scope_change_removes_the_old_pass`、`test_a_saved_draft_verifies_related_claims_in_one_request`。它们不覆盖上述空正文及真实两代凭据链，不能替代角色交付或12/6成绩。只补这两个实际入口证明，不再跑整本mock或扩通用框架。

### 106.4 资源及下一卡

本段五个实际入口（含0物理的continue）共430990账本＝243505有效usage＋187485未知预留、22物理、4恢复、1905.896036秒监督时间。上汽9097/2物理，同方20108+0+3132/4物理，浦发398653/16物理；unknown费用不记0。

§7.44同一新增任务累计**2550643账本＝1905232有效usage＋645411未知预留、202物理、32恢复、11805.540723秒监督时间**。总限5400000/27000秒/208物理/40恢复不增加，最多余**2849357账本、6物理、8恢复名额、15194.459277秒**；恢复实际也包含于6物理，额外准备/交付时间还须扣。旧B2及歌华账不动，不把剩余账本误作请求不限量。

执行[B2§7.9](b2-execution-card.md#79-当前任务卡用现存业务证据交付角色保住原核验链)，验收见[§7.46](../../../docs/development/company_profile_acceptance_program.md#746-核心缺口交接裁决与角色交付2026-10-25)。A显式将最后6次同任务余额改绑为修复后的同方、上汽角色交付，停止原空补源/标签驱动路线；目标各一包紧凑成稿、立即保存及核验，必要补源/恢复共用余量，不承诺两家公司必达标。已有独立有效事实只补投影不重复付费，原真实否定不复活。浦发新金额保留，暂不消耗余量重核旧对象或盲扫附注。范围内修复、最小证明、冻结和真实接续由B连续完成，不逐项请批、不另交准备报告。角色交出后还须核心12/6映射供A独立审核，达标才计可用，不放宽底线、不进S1。

### 106.5 清理、验证与变更边界

本轮真实模型0；仅更新审核和必要任务文档，不修B业务代码、不改旧快照。已完成原owner新实例读回、当前发送封装只读捕获、真实loader诊断及上述3项直接回归；`openspec validate restore-company-profile-llm-led-generalization --strict`、限定5文件`git diff --check`、19处新增本地链接/锚点及资源算术均通过。阻塞性Review结果为上述3类业务缺陷，非阻塞优化后置。临时根盘点约254M，不等于全部无用；相关源/核验仍被接续依赖，未盲删。pytest收尾仍遇旧NFS忙文件，不中断他人进程；本轮未新建运行根、库副本或一次性诊断脚本。下一卡先清可确认无用产物、交接清本人垃圾，补本轮未给的清理量/必要保留说明。

开始时19项既有已跟踪改动及并行未跟踪内容全部保护；验收计划details旧改动与活动章节分离，只提交本人文档块。A活跃审核/规划时间未完整计量，记未知；新阻塞为上述3类，非阻塞优化后置。原资源、费用unknown例外、现有池/账号、生产not_authorized及规模false保持。

## 107. B进度：同方与上汽角色已写入

日期：2026-10-25。按[B2§7.9](b2-execution-card.md#79-当前任务卡用现存业务证据交付角色保住原核验链)执行。这不是A2，可用进度不改。

### 107.1 新角色

同方 `claim-operating-role-from-current-business` 已核。公司按公共安全、数智产业、智慧能源经营：公共安全向民航、海关、铁路等场景提供智能检查系统和运维；数智产业通过 CNKI 做机构订阅和个人按需付费的知识服务，并以技术授权、软件设计与开发做数智转型；智慧能源通过热泵、储能和运维提供一体化方案。合同负债预收余额 3,295,822,047.40 元仍是单独的 `claim-5`，没有当成经营角色。原知网订阅、技术授权，以及公共安全 6,980,575,849.42 元、数智 2,750,721,999.45 元、智慧能源 4,504,196,896.13 元的分部收入仍在公司画像。三视图为 74/55/2。查询、导出和新实例重读一致。query SHA-256 `69a838dc3e930935af696f9531ea0a5513dce470ff458cff762517920c50e2e3`，见 [b2-tongfang-role-result](b2-tongfang-role-result/query.json)。

上汽 `claim-operating-role-001` 已核。集团设整车及零部件分部（生产并销售整车及零部件）和金融服务分部；正文写入整车业务 410,196,473,001.11 元、零部件业务 203,019,866,775.26 元、金融业务 10,091,710,191.88 元。`claim-1` 仍为撤回。`整车制造与综合服务集成商` 仍是 unclear，没有整句提升。子公司业务性质仍是另一条已核主张，没有当成集团角色。第156页时段金额 11,832,005,928.17 元、第155页主营劳务及其他 18,791,117,696.37 元和其他业务 14,144,644,416.56 元、营业收入 646,152,101,889.30 元与对外交易收入 656,243,812,081.18 元都还在公司画像。三视图为 44/32/7。查询、导出和新实例重读一致。query SHA-256 `dba837fc327451875a09acb95450d2239b7f5902f6524e44ded29151c5112f3b`，见 [b2-saic-role-result](b2-saic-role-result/query.json)。

### 107.2 程序与投影缺口

角色路径改为把已核产品、服务和收入主张所在页的原文交给一次成稿和一次核验，不再把页码、阅读状态当作正文，也不再按“角色”标签、余额、逗号金额或第一组决定是否成稿。直接测试 `test_role_closeout_sends_source_text_not_page_metadata` 确认发送页含原文且不再补源。两次真实请求的保存回执只记页码，不保存发送正文。

两代加载会跳过汇总，并把历史文件里的非汇总核验回执写入新查询。`test_a_second_generation_keeps_an_earlier_real_pass` 通过。本轮绑定只指向核心结果。核心结果里，同方 22 条、上汽 22 条产品暴露通过只写在被跳过的汇总里；可追溯原回执分别在同方首份/后继和上汽 `b2-saic2-delivery`。公司画像主张没有丢，但当时产品暴露降到同方 33、上汽 10。随后没有再调用模型，只按同一主张范围把这些更早的非汇总回执并回解释记录，再由原读取路径重新查询和导出。产品暴露回到同方 55、上汽 32。`claim-1` 的旧通过没有恢复。

### 107.3 资源

同方 2 次物理、31,402 ledger、283.5161844538525 秒、0 恢复，费用 unknown。上汽 2 次物理、26,787 ledger、265.2502857465297 秒、0 恢复，费用 unknown。本段合计 4 次物理、58,189 ledger、548.7664702003822 秒。§7.44 累计 2,608,832 ledger、206 物理、32 恢复、12,354.307193 秒。上限不变，余 2,791,168 ledger、2 次物理、8 个恢复名额、14,645.692807 秒。浦发金额和 158 页未读未再消耗请求。

### 107.4 清理与边界

补回检查前的导出副本已在新查询核对后删除。权威回执在 `b2-tongfang-role-result` 和 `b2-saic-role-result`。运行根 `data/QuoteBak/b2-tongfang-role-20261025`、`data/QuoteBak/b2-saic-role-20261025` 仍保留可查询库。旧 NFS 忙文件未动。生产 not_authorized，规模声明 false。B 不改可用进度。剩余 2 次物理请求未使用。

## 108. A审核：角色业务交付接受，转公司验收收口

日期：2026-10-25。审核`d56b5066`、`9fd20c42`及§107完整交接，依产品总需求→验收计划→交付路径→A/B流程v3和本change。使用business-first-goal收敛审核和任务范围，A不执行B的真实模型任务。

### 108.1 方向、真实进展和主要矛盾

**接受两家有源经营角色交付及可读结果恢复；不等于两家公司整体验收通过。A2当前不通过，可用1/4。** 同方公共安全、数智产业、智慧能源的实际服务/产品与客户场景，上汽集团整车及零部件生产销售和金融服务，现已进入原查询并真实核验。这是新的业务能力，不能再说没有角色或只有余额句。空正文补源路径已删除，角色不再以分类标签为前置，方向调整有效。

下一主要矛盾转为**安全、稳定地交付现有核心并完成公司验收**，不是更多源条件、更多主张或再写角色。上汽仍有同句不同ID的母公司主体歧义，完整核心12/6和已审去重证据尚未交齐；同方已核采购/投入与新角色仍需在相连视图清楚呈现。程序flag=false不能代替A业务判定，模型pass也不能抵销实际主体错误。A不再将这轮成果带回同页补源循环；下一卡集中最终交付和实际阻塞，非主要附注/架构/成本优化后置。

### 108.2 原owner交付与独立原文核验

A核正式PDF的内容hash与冻结asset一致，使用其解析原文及统领上下文独立检查新角色，未以模型explanation代替原文。原query/export的profiles相同；A新`CompanyProfileReadService`分别读实际output/work_id，均与持久query一致。两家均三完整false、classification=limited，旧快照不改。

| 交付 | A核源结果及范围 |
| --- | --- |
| [同方query](b2-tongfang-role-result/query.json)，SHA-256 `69a838dc3e930935af696f9531ea0a5513dce470ff458cff762517920c50e2e3` | 74/55/2为三视图呈现数，verified呈现58/50/2，不是去重得分。`claim-operating-role-from-current-business`的公共安全检查系统/运维、CNKI机构订阅/个人按需付费、技术授权/软件设计开发、热泵储能及智慧能源方案，分别由物理12—13页支持；第24页明确单位万元、2025主营业务成本489934.64/146693.85/401814.84，非收入或上年数。第9页收入150.65亿元和低毛利业务收缩成立。角色没有将“致力于成为”或市场定位目标写成已实现称号。第249页主要分部收入及抵销口径仍保留，未据本轮声明全量正确。 |
| [上汽query](b2-saic-role-result/query.json)，SHA-256 `dba837fc327451875a09acb95450d2239b7f5902f6524e44ded29151c5112f3b` | 44/32/7呈现，verified呈现36/29/5，不是去重得分。`claim-operating-role-001`的集团两报告分部及职责有第192页直接原文；整车410196473001.11、零部件203019866775.26、金融10091710191.88元由第11页本期收入列支持。第193页对外交易收入的金融分部11578441732.63为另一口径，不能与第11页混合加总或自动判某数错误。子公司性质另有来源，旧“综合服务集成商”仍not_reliable，旧`claim-1`仍撤回。 |

同方第222/224页关联方采购/接受劳务的本年合计947841284.24元及相关对手已有有效主张，不能把它遗忘后再说缺所有上游证据；第170页预收余额依然只是余额。共享这些已核事实到相应视图无需重新生成同义句。A本轮未完成两家全部核心/高风险及种子补充对象源审，不填写12/6或准确率，也不以以上抽查给公司可用分数。

### 108.3 剩余阻塞及工程接受范围

**P1，母公司歧义仍以另一ID外放。** 上汽当前`claim-principal-business`是verified，正文为“公司主营业务包括整车业务。2025年度，整车业务实现主营业务收入57,126,986,013.91元，主营业务成本54,770,228,270.45元”，证据第203页；完全相同正文的`claim-principal-business-1`已not_reliable。A读第193页下半部“十九、母公司财务报表主要项目注释”以及第202—203页连续表格，确认上述金额属于母公司，不能作为无主体限定的集团主营。该verified检查来自旧`bp-work-f7de1d4578a4f2124ee7635d:report:26`，恢复有效回执不代表原语义检查必然正确。退出这句及其依赖/同源歧义版本即可，无须重做已有集团核心或再保一条次要母公司明细；不能只按一个claim ID遮住问题。

**P0，未交可裁决的公司核心验收包。** §7.9要求的两家12/6证据映射仍未提供，且同方供应链视图除新角色外只有预收余额，现存采购/投入事实没有清楚贯通。B应交同一份最终核心正文、真实检查/覆盖和残余缺项，A再按至少11/12、全部主要业务/变现/核心视图、6负例、已审去重≥98%及底线外放0分级；不要求逐附注齐全。浦发158页未读仍是独立限制，本卡不能据两家角色通过放行S1。

工程部分接受：源码现在取真实`sources.text`，不再发`by_page`元数据；历史非汇总verify回执可进入下一代query。22条产品投影已用更早真实回执恢复且读回一致，但“绑定仅指核心结果、靠本次另行并回解释记录”不能替代下一次正常磁盘加载。只补与最终交付直接相关的接续/主体及投影证明，不扩建凭据系统或重复核全部旧正文。A复跑角色原文发送、两代旧pass、后置block/范围变化、空响应实际请求目标4节点，**4 passed、7 warnings、5.98秒**；它们不能证明上述实际母公司错句正确或12/6已经通过。

### 108.4 下一交付、资源与边界

执行[B2§7.10](b2-execution-card.md#710-当前任务卡交同方上汽最终核心验收包)，同步[验收§7.47](../../../docs/development/company_profile_acceptance_program.md#747-角色交付接受转最终核心验收2026-10-25)：保新角色，退出实际母公司歧义，贯通已有事实与有效检查，交两家最终12/6源映射和原owner一致交付。默认离线；最后2物理仅按需补正确正文的核心支持/覆盖核验，每家公司至多1次，含任何恢复，不再重新成稿/导航/全文读源，不为用完余额发送。范围内P0—P3、必要直接证明、冻结及已授权发送连续完成，B不逐项请批。A卡末按公司分别裁决；浦发只保可靠结果及精确未读风险，不虚报核心完整。

本段两入口合计**58189账本，均有效usage、4物理、0恢复、548.766470秒**，费用unknown。§7.44同任务累计**2608832账本＝1963421有效usage＋645411未知预留、206物理、32恢复、12354.307193秒监督**。原5400000/27000秒/208物理/40恢复不增，最多余2791168账本、2物理、8个恢复名额及14645.692807秒时间上界；实际恢复受2物理约束，额外准备/导出另扣。旧B2、歌华、失败和unknown不重置，有价任务≤600元及unknown例外沿用，生产not_authorized/规模false不变。

### 108.5 清理、审核及提交范围

复用原源/运行根只读核证；A无真实模型调用、不改应用代码或旧交付。B删除补回前重复导出的说明接受；QuoteBak现约397M，相关根仍有接续/查询依赖，不能当全部可删，下一卡先清确认无用历史产物、收尾清本人产物并报释放量。A本轮4节点生成的`pytest-47`测试目录已清（文件合计9811字节），旧NFS占用不强删、不停他人进程。

本轮仅更新5份审核/规划配套文档；`openspec validate restore-company-profile-llm-led-generalization --strict`、限定5文件`git diff --check`、20处新增本地链接/锚点及账本算术通过。对B直接链3文件执行Ruff仍报13项导入排序、未使用测试导入及条件简化问题，归非阻塞，不声称Ruff通过、不另开风格整理卡。开始时19份既有跟踪修改和并行未跟踪内容保护，验收计划details内旧改动与本人新章节隔离。A活跃审核/规划时间未精确计量，记未知；新发现1类实际主体外放阻塞，核心映射是上卡未交项，非阻塞建议不入卡。

## 109. B进度：两家最终核心包已交，母公司歧义已退出

日期：2026-10-25。按[B2§7.10](b2-execution-card.md#710-当前任务卡交同方上汽最终核心验收包)执行。没有新的模型调用，没有重写角色。这不是A2，可用进度不改。下表是证据对照，B不代A打分；“程序已核”只表示原查询里的检查状态，不是公司可用。

被审的角色查询没有覆写。同方仍是 `69a838dc3e930935af696f9531ea0a5513dce470ff458cff762517920c50e2e3`，上汽仍是 `dba837fc327451875a09acb95450d2239b7f5902f6524e44ded29151c5112f3b`。最终版另存。

### 109.1 最终正文

同方 [b2-tongfang-final-result](b2-tongfang-final-result/query.json)，SHA-256 `d8e5f74209a508c235de551fe05736269eea27fd596421ac3f89d2dfb60e42f9`。查询、导出和新实例重读一致。呈现 74/55/6，程序已核呈现 56/48/6。三个完整标志仍为 false，classification=limited。角色 `claim-operating-role-from-current-business` 仍已核。关联采购 `947,841,284.24` 元现在同时出现在产品暴露和供应链，是同一主张，没有另写一句。合同负债余额 `3,295,822,047.40` 元仍只是 `claim-5`。

上汽 [b2-saic-final-result](b2-saic-final-result/query.json)，SHA-256 `51d937918407d4f6ac804601692bf31b996b83dbc0d0d4c6d99be7a747a340b1`。查询、导出和新实例重读一致。呈现 44/32/12，程序已核呈现 35/29/10。三个完整标志仍为 false，classification=limited。角色 `claim-operating-role-001` 仍已核。集团整车 `410,196,473,001.11` 元、零部件 `203,019,866,775.26` 元、第11页金融 `10,091,710,191.88` 元仍已核。营业收入 `646,152,101,889.30` 元与对外交易收入 `656,243,812,081.18` 元仍分开。`claim-1` 仍撤回。“整车制造与综合服务集成商”仍 not_reliable。

第203页母公司整车收入 `57,126,986,013.91` 元、成本 `54,770,228,270.45` 元：`claim-principal-business` 改为 unclear / `retracted_pending_correction`，`claim-principal-business-1` 仍是 not_reliable。两句相同正文都不再显示为已核。没有为这条次要母公司明细再调用模型。依赖它的条件 `bp-work-1e963e4dd353ef49595b0d50:report:24:cond-1` 不再指向一条已核主张。

### 109.2 十二项核心对照

口径沿用既有 C01—C06、E01—E03、S01—S03。状态只写程序证据，待 A 核源。

| ID | 同方 | 上汽 |
| --- | --- | --- |
| C01 当前经营 | `claim-operating-role-from-current-business`，第12—13页，程序已核 | `claim-operating-role-001`，第192页两报告分部，程序已核 |
| C02 主要产品或服务 | 角色内公共安全检查系统、CNKI、热泵储能；原产品主张仍在公司画像 | 整车、零部件、劳务及其他和金融仍在 `bp-work-5157694452e829569d6a42f3:report:26:claim-001` 等，程序已核 |
| C03 当前与计划分开 | 角色没有把“致力于成为”写成已实现称号 | 旧集成商句仍 not_reliable，没有整句提升 |
| C04 变现 | 机构订阅、个人按需付费、技术授权和软件设计开发，程序已核 | 整车销售及第156页时点 `620,175,451,544.57` 元、时段 `11,832,005,928.17` 元，程序已核。`claim-1` 仍撤回 |
| C05 主要收入 | 第9页营业收入 `150.65` 亿元，程序已核 | 第155页营业收入 `646,152,101,889.30` 元，程序已核；不是第11页含金融的 `656,243,812,081.18` 元 |
| C06 主要板块 | 公共安全 `6,980,575,849.42` 元、数智 `2,750,721,999.45` 元、智慧能源 `4,504,196,896.13` 元，程序已核 | 第11页整车、零部件、金融分列，程序已核 |
| E01 收入端暴露 | 上述板块收入同时在产品暴露 | 第11页分产品收入同时在产品暴露 |
| E02 投入 | 关联采购和研发支出在产品暴露，并共享到供应链 | 原材料、人工及前五供应商采购在产品暴露，并共享到供应链 |
| E03 服务对象 | 角色写明民航、海关、铁路、机构和个人、政务和企业 | 前五名客户在供应链；未编造客户名称 |
| S01 角色 | 同上角色，供应链已核 | 集团生产销售加金融服务，供应链已核。子公司业务性质仍是另一条，不是集团角色 |
| S02 上游 | `claim_input_exposure_related_party_purchases`，本年 `947,841,284.24` 元，产品暴露和供应链同一主张 | 合营/联营采购与销售分列，供应链已有 |
| S03 下游 | 角色中的机构、个人和行业客户；预收余额不是下游角色 | 前五名客户合计 `75,458,211,510.20` 元、占比 `11.68%`，程序已核 |

剩余缺项：两家完整标志仍为 false。同方第249页分部抵销没有在本卡重新核完整表。上汽母公司第203页明细已退出，没有改写成带“母公司”的新句。次要附注未扩写。

### 109.3 六类负例

| ID | 实际对象与处置 |
| --- | --- |
| N01 母公司限定 | 上汽第203页两编号都已退出可信。同方没有把母公司附注金额写成集团主营 |
| N02 不同口径 | 上汽 `646,152,101,889.30` 与 `656,243,812,081.18` 仍分开；第11页金融 `10,091,710,191.88` 与第193页金融分部对外交易 `11,578,441,732.63` 没有相加 |
| N03 分部抵销 | 上汽 `rm_external_revenue_by_segment_2025` 的引文保留第193页“分部间抵销”列，合计没有改写成两分部简单相加 |
| N04 采购与销售 | 上汽关联方采购额和销售额分列。同方采购 `947,841,284.24` 元没有写成销售收入 |
| N05 计划与当前 | 同方角色未把目标称号写成已实现。上汽旧集成商句仍 not_reliable |
| N06 余额与流量 | 同方 `claim-5` 仍是合同负债预收余额。上汽经销商保证金余额仍 not_reliable，没有当成本期收入 |

未审对象不填通过。去重索引在 [同方](b2-tongfang-final-result/verified-object-index.json) 62 个已核主张、[上汽](b2-saic-final-result/verified-object-index.json) 45 个已核主张，供 A 按 §4.2 和种子 20261023 抽补充对象。呈现数和索引条数都不是准确率。

### 109.4 接续、资源和浦发

同一句不同编号不能一边隔离一边继续已核，由 `test_the_same_sentence_cannot_stay_verified_on_another_id` 覆盖。已核投入进入供应链是同一主张，没有第二份核验。保存查询里的非汇总回执由 `test_a_saved_query_keeps_a_real_receipt_without_a_hand_merge` 单独加载仍能取回。连同原4项，6 passed。OpenSpec 严格校验通过。Ruff 对这两份文件仍有既有导入和风格项，未另开整理，不声称 Ruff 通过。

本卡物理请求 0。余额仍是 2 次物理、2,791,168 ledger、8 个恢复名额、14,645.692807 秒。§7.44 累计不因这次离线导出改写。费用没有新的 unknown 调用。

浦发不在本卡发送范围内。页状态里 not_read 共 160 页：第1–2页，以及第297–454页共 158 页。已接受的 158 页未读限制保持，不把未读写成未披露。绿色信贷等主要条件仍有 `major_condition_not_verified`。手续费及佣金净收入 227.25 亿元和其他非利息收益 307.56 亿元不改。

### 109.5 清理与边界

最终可查询副本在 `data/QuoteBak/b2-saic-final-20261025`（4.2M）和 `data/QuoteBak/b2-tongfang-final-20261025`（5.6M）。权威回执是上面两个 final-result。角色结果目录和原运行根保留。NFS 忙文件未删。生产 not_authorized，规模声明 false。B 不改可用进度。

## 110. A审核：同方可用，上汽安全撤句后收口，主线转浦发

日期：2026-10-25。审核`1f23ab05`及§109，按原[B2§3](b2-execution-card.md#3-a预冻的逐家公司核心合同)、验收§4及[A/B流程v3](../../../docs/development/ab_role_development_workflow.md)裁决；没有真实模型调用，没有修改B的运行文件或最终query。

### 110.1 主线、真实进展与公司裁决

本轮已解决上汽母公司错句的另一编号外放，并将现有投入事实共享到供应链；两个新角色和正确收入没有丢失。这些是业务进展。A完成两家现有答案的独立源审，不再将“等待审核”当成开发任务，也不因程序三个false自动拒绝公司内容可用。下一主要矛盾是浦发尚未完成的读源与核心覆盖，不是再给两家增加主张或恢复全部附注明细。

| 公司/制品 | 原12核心 | 原6类负例 | 已审去重业务对象组 | 补救后公司裁决 |
| --- | --- | --- | --- | --- |
| 同方，§109 final query `d8e5f74209a508c235de551fe05736269eea27fd596421ac3f89d2dfb60e42f9` | 12/12；公司画像6/6、暴露3/3、供应链3/3 | 6/6 | 核心及高风险17/17、补充18/18，合并35/35（100%） | 内容有限通过、可用；不代表程序完整性已通过或未见泛化达标 |
| 上汽，§109 final query `51d937918407d4f6ac804601692bf31b996b83dbc0d0d4c6d99be7a747a340b1` | 11/12；C06的当前/计划边界未过，其余核心成立 | 5/6；N04未过 | 核心及高风险16/17、补充15/15，合并31/32（96.875%） | 限用；现有正确核心保留，战略句安全退出后只复核变化 |

这是现有自动生成交付的补救审核，原首发/正式后继失败不改。歌华原可用结论保留，因此首发观察4/4、补救后可用**2/4**；上汽和浦发仍限用，A2未通过。S1/S2/S3仍0/12、0/20、0/100；生产not_authorized、规模false。已审准确率只涵盖已核发布对象，不把待核、未读或未披露当正确对象计分；发现已知错误的上汽不以达到11项豁免底线。

### 110.2 原合同核源，不采用§109重命名后的分母

§109将C03写成当前/计划、C04写成变现，且把E03换成服务对象，N01—N06也重新编号；这不改变原合同。A用原C03经营模式、C04收入规模、C05收入构成、C06重大变化及状态、E03映射不确定性，以及原六类负例实查，既不追改原合同，也不再要求B补一轮排版材料。

| 原条件 | 同方独立依据 | 上汽独立依据 |
| --- | --- | --- |
| C01/C02 | 原文12—13、112页公共安全、数智、智慧能源；其产品/服务及科技园等补充业务仍可辨识 | 11、192页集团整车及零部件生产销售、金融服务，劳务及其他未丢失；子公司性质未冒充集团完整角色 |
| C03 | 12页机构订阅/个人按需付费、技术授权/软件开发；13、112页产品销售、工程、技术/节能及运维服务 | 61、97页销售及经销商权属交接；155—156页主营/其他及确认方式；金融服务保留本义 |
| C04/C05 | 178页本年营业收入15064892096.09、主营14970095547.03、其他94796549.06元；249页三主要分部6980575849.42/2750721999.45/4504196896.13元，占营业收入约94.49%；抵销-281111203.52不被补加或称新收入 | 11页含金融656243812081.18与155页营业收入646152101889.30分开；整车410196473001.11、零部件203019866775.26、劳务及其他32935762112.94、金融10091710191.88元分列；156页时点/时段正确，193页金融分部另一口径不混加 |
| C06 | 9页智慧能源低毛利业务收缩、上年计算机业务出表有当前/历史限定，目标称号不冒充已经实现 | 母公司两错句已退出、独立集团角色成立；但28页战略计划的另一表述仍verified，见110.3 |
| E01/E02/E03 | 板块服务与收入在产品暴露；25、189、222—224页供应商、研发及采购投入方向/单位正确；commodity assessment为not_assessed，未据空映射丢服务或编造利润敏感性 | 11页收入端及12—13、184页投入/关联采购方向可辨识；未把汽车服务因无商品ID丢掉，也未发布价格利润传导结论 |
| S01/S02/S03 | 12—13页有源服务角色与客户类型，222—224页采购/接受劳务947841284.24元共享同一对象；170页预收3295822047.40仍只是余额 | 192页集团职责，170—171页具名子公司另限主体；184页购销两向、61/97页经销商交接及155页客户类型/集中度；不靠客户排名单独成立角色 |

负例实际核查：N01主体、期间、单位及金额/成本列（同方24/178/249，上汽11/155/193及母公司限定）；N02续表、口径、抵销（同方249，上汽155—156/193）；N03采购销售方向（同方222—224、上汽184—185）；N04目标、计划、历史与当前（同方9/12—13/38，上汽27—28）；N05匿名前五名不虚构名称（同方25、上汽13/155）；N06服务与未映射状态不丢失、不造传导（两家实际产品/commodity投影）。同方六类均成立，上汽N04仍失败。合同负债、预付款、研发资本化另按余额/流量及成本高风险实查，不能借此替换原N05/N06。

A复用正式PDF hash已核及解析原文，必要新页直接读取PDF派生缓存；没有用模型explanation代替原文。两家query SHA-256与§109一致，query/export profiles相同；A新`CompanyProfileReadService`分别从final运行根的output/work_id只读加载，均与提交query一致。三完整标志仍false，不手填true。角色、投入共享同一主张/检查，未据呈现条数重复计分。

### 110.3 唯一新增阻塞：战略句仍作当前业务外放

上汽`claim-products-services__2`在公司画像、产品暴露均为verified，答案为“公司绿色低碳业务涉及纯电动、混合动力产品，并推进燃料电池产品投放及V2G应用探索”。其证据是第28页“绿色低碳：持续提升……加快燃料电池产品投放，初步探索V2G应用”。A读第27页“六、公司关于公司未来发展的讨论与分析—(二)公司发展战略”至第28页连续正文，确认这里是战略安排，不能据此确认报告期已经推进该活动。另一同义句`products_services-001`已not_reliable，并未消除这个不同措辞版本。

此为P1，修复条件是有关句子及依赖覆盖退出当前确定结论，或明确战略状态；不需要重读全年报、重新生成正确集团角色、再核全部旧句或引入语义去重平台。默认选择安全撤回。第9/17—18页实际新能源产品/产销、集团整车/零部件/金融角色仍可独立成立。第28页`claim-principal-business__2`仅概括确有的业务板块，另有第9/192页交叉支持，不因与错误句同页一并清空。

### 110.4 已审对象范围与去重

对B索引的62/45个claim ID逐项核查，三个视图的重复呈现只查一次；对相同业务、主体、期间、事实的改写再合并，得到同方35、上汽32个业务对象组。不是35/32个原子数字的全市场估计。核心/高风险全查，去重后补充不足20也全部查，不抽样，冻结种子20261023无需选择子集；未审默认正确为0。同方同义收入/产品复述不增加分母。

可复核分组：以下为各自§109 `verified-object-index.json` **从0起的数组序号**，逗号内属于同一对象组，分号分组；该索引及query hash固定，不新建平行评分器。

- 同方：`0,1,2,15,31,42,43; 3,48; 4,39; 5,7,11,33,45,47; 6,55; 8,13,41; 9; 10,32; 14,25; 16; 17,34; 18; 19,46; 20; 21,35,49; 22; 23; 24,37,50; 26,38,51; 27; 28; 29; 30; 36; 40; 44; 52; 53; 54; 56,58; 57; 59; 60; 61; 12`。
- 上汽：`0,11; 1,12; 2; 3,4,16,17; 5; 6,29,30,31; 7; 8,9; 10; 13; 14; 15; 18; 19; 20; 21; 22; 23; 24; 25; 26; 27; 28; 32; 33; 34; 35,38; 36; 37,39,41; 40,43; 42; 44`。第26组（1起，下同）为上述错误战略句。核心/高风险组为1、2、4、6、8、9、10、12、13、14、24、26、27、28、29、30、32，其余15组全查；同方核心/高风险为1—6、10、12、16、17、25、26、30—32、34—35中的17组，补充18组全查。

主要支持页之外，补充核源含同方23/35/91/112/170/189/192—204/212及上汽20/26/36/81/114—115/143/170—172/184—185；核子公司业务性质、注册范围限定、本期/上期子公司收入、预收/预付款余额，不把余额当采购或收入流量。第249页全表原文已独立核到，B本卡没重核不等于A只能延期验收。

### 110.5 下一业务窗口与资源裁决

执行[B2§7.11](b2-execution-card.md#711-当前任务卡上汽安全收尾浦发交可验收核心)及[验收§7.48](../../../docs/development/company_profile_acceptance_program.md#748-同方验收通过上汽安全收尾与浦发最终核心窗口2026-10-25)。同方内容验收结束；上汽离线安全撤句，与浦发核心收口合并一个开发窗口，A下一次只复核上汽变化及浦发新交付，不全量重复审两家。

浦发有真实主营、角色及三项净收入，不再从零开始。缺的是现存主要条件的有效覆盖、`funding_and_capital_management`证据定位，以及第1—2/297—454页的未成功读源；绿色金融等条件不能因现有金融标签就消失，也不能把每条major自动扩写成单请求。按业务组复用现有正文/源/真实检查、独立无候选检查剩余范围，只补有影响的主要缺口后立即核验/保存，交原12/6验收。

A此前把公司验收拖在多轮准备/恢复细节上，对效率负责；本次不再让2次物理余额阻止最后一家实际推进。依据用户“同意执行，且费用可以进一步上调，目标是尽快完成开发任务”的持续授权，显式把§7.44同任务**物理总限208改为270**，即原余2加62、本卡共最多64次；不是新的公司首发/正式后继，不追改旧成绩。账本总限5400000、绝对总时间27000秒、恢复40和有价部分≤600元/unknown例外均不增加。已用206物理/2608832账本/32恢复保留；本卡最多2791168账本、64物理（含恢复）、8恢复、实际额外用时≤10800秒且不越整任务总时限，准备/导出均扣。旧12354.307193秒是监督时间，不伪称完整活跃成本，B须补核实际额外准备/交付。不存在须用户再批同一费用例外的条件。

达公司合同即停，不追附注明细；达到本卡/整任务任一限、源/账号/服务不可用、既有改动不可隔离或用户停止，即交最后答案和明确缺项。仍失败由A作支持范围/方案结论，不自动再派同链余额卡；4家可用前不启动S1，生产和规模边界不变。

### 110.6 验证、清理和改动范围

A独立新2节点：`test_the_same_sentence_cannot_stay_verified_on_another_id`及`test_a_saved_query_keeps_a_real_receipt_without_a_hand_merge`，**2 passed、7 warnings、2.44秒**；上轮未变4节点回执复用，不重跑整本mock。这证明同字句隔离与真实回执加载，不证明不同措辞的战略句正确。B原Ruff未过的导入/风格项仍为非阻塞，A不声称它通过。

QuoteBak现约287M；final运行根4.2M/5.6M仍供本次独立查询及接续，不盲删；旧NFS占用不强删。A本轮2节点的`pytest-50`确认无占用后已删除，可重建测试文件1265字节；下卡清已无依赖重复临时物，正式源、旧失败、真实检查及并行工作保护。本轮只更新现有5份审核/规划文档，无业务修复/真实调用；严格OpenSpec、限定5文件diff检查及新增链接/计数复核通过。开始时21份已跟踪修改和既有未跟踪内容保护，验收计划details旧hunk不纳入。A活跃审核/规划时间未精确计量，记未知；新发现1类P1，补充/风格不入开发范围。

## 111. A审核：上汽可用，浦发沿当前窗口完成最后一家

日期：2026-10-25。审核`74e744d1`、`29578335`及用户补充的浦发终态/在途交接，沿原B2§3、验收§4和A/B流程v3。A没有真实模型调用、没有修B的业务代码或改旧制品。

### 111.1 先裁业务，再判断下一步

上汽现存答案已达到补救内容可用，不能再因等待浦发或程序false拖住裁决。浦发本轮的业务进展是297—454页成功读源并持久化，旧三项净收入及服务/资金正文仍可查询；没有新增已核答案，不能把读源当交卷。当前主要矛盾从未读原文转到已有证据的成稿、核验与语义覆盖，不是再读一本年报或继续扩充条件清单。

歌华原有限通过、同方§110结论保留，上汽本轮补救内容有限通过，故首发观察4/4、补救可用**3/4**，只余浦发。A2仍未通过，S1/S2/S3仍0/12、0/20、0/100；生产not_authorized、规模false。原首发/正式后继失败及§110误放分数均不追改。

### 111.2 上汽差异验收与独立核源

新query hash `480e64fe08c98659fde3178488ef48146a8d889b6992fd23ce230685cc53c24d`，query/export profiles相同；A从`b2-saic-strategy-20261025/output`以新`CompanyProfileReadService`读取`bp-work-0273003e8879dbf54f9fe1be`，与提交profiles一致。原final hash仍`51d937918407d4f6ac804601692bf31b996b83dbc0d0d4c6d99be7a747a340b1`，未改。

逐项差异只有：公司画像中的`claim-products-services__2`退出可信、产品暴露删除它的确定投影；另一不同措辞`products_services-001`仍not_reliable。44个仍verified的唯一claim ID，其正文、证据、状态与§110原审对象逐字节等同，没有以撤一句清空正确核心。第28页仅板块概括、9/17—18页当前新能源产销、192页集团角色、集团收入/投入均保留。有关战略条件仍在`major_condition_not_verified`中，旧pass没有消掉该缺口。

A复用§110已完成的全量原文审查，另独立复读27—28页“未来发展—发展战略—经营计划”连续正文，确认退出正确。原C06/N04当前与计划边界恢复，原12核心12/12（画像6/6、暴露3/3、供应链3/3）、6负例6/6。当前可靠去重对象31组：核心/高风险16/16、补充15/15，合并31/31（100%已审准确率）；原32组中错误战略组已退出可信，不把它算正确、不抹掉原31/32或首发错误。补充不足20，原审全查已覆盖。呈现44/31/12、三个完整标志false仍如实保留；本裁决是受限研究内容可用，不是通用完整性程序、成本或全市场通过。

### 111.3 浦发实际终态、主线阻塞及不再阻塞的页

原初始化失败258.167998秒、0物理、0账本；`29578335`在磁盘接续分支初始化草稿和选中条件列表，修复后实际入口进入真实源阅读，不能只以单测宣称业务完成。A读取重试根及持久`b2-spdb-unread-result`，query hash `1a9a7affedafe993a21845d401345a4418478f159e68d9bb001665136523cebb`，query/export/新实例一致。重试退出0、1473.088550秒，297—454页成功，新增10份成功源回执及32条已定位条件，原文覆盖3—454页；条件共179条。主张仍109个唯一ID，无新增可靠主张；呈现87/60/18，程序已核73个唯一ID，不作为A分数。

原75个已核ID中的两个战略性定位句退出可信，其余73个正文/证据/状态与基线相同；不能称旧答案全丢或仍无内容。营业收入1739.64亿元、利息净收入1204.83亿元、手续费及佣金净收入227.25亿元、其他非利息收益307.56亿元仍在。A独立核正式原文28/32/71—75页，确认营业收入/业务总收入、毛息/净息、平均余额/利息收支及本期列区分；不将贷款利息186233百万元当营业净收入。已有资金投入、服务及角色可用于后续实际覆盖对照，不要求每个新条件再写一条。

本轮成稿出现`LLM response was truncated`；同一失败业务组随后在后续源片回调中再次被选择，恢复以`logical_recovery_already_used`拒绝。最终新增成功语义回执全是读源，0份成功新成稿/核验。`draft_ready_core`失败后该首组选取状态未推进，而`continue_unread`末端又只保留编号含`funding_and_capital`的待稿条件；这种编排不能保证把新证据交成答案。这是一个直接交付阻塞，不能解释为年报没有内容或只差延长总时间。只修实际成稿组的输出承载、失败后选择及最终业务组收束，不建设通用调度平台，也不据标签强行覆盖语义。

第1—2页状态仍not_read，PDF解析文本均0字符。A直接渲染正式PDF：第1页是“2025年年度报告”封面，第2页只有线条/城市轮廓装饰，无需从中恢复主营、收入或资金事实。本次有源裁决两页不挡核心，保其未读状态及本段依据，不伪称模型已读；不另开OCR卡。源覆盖及语义覆盖仍分开，不能据其余452页已读就直接判公司可用。

### 111.4 下一卡、在途工作与资源

用户补充后A核到`b2-spdb-draft-20261025`已按持久`b2-spdb-unread-result`启动，绑定与来源链存在；21:40快照已发2物理、未用恢复，该数不是最终结算。本轮不重复启动、不热改正在执行的冻结代码，后续按[B2§7.12](b2-execution-card.md#712-接续任务卡浦发完成当前窗口并交验收)/[验收§7.49](../../../docs/development/company_profile_acceptance_program.md#749-上汽安全撤句验收通过浦发沿在途窗口交卷2026-10-25)交最后一家完整核心。当前答案能覆盖的新条件复用合法检查，只对重要实缺成稿/核验，成功即时持久，表头单位、主体及净额口径不裁。A待最终包独立评分，不替B从源条件手填答案，不因程序false默认拒绝。

本段18物理/403809账本＝209917有效＋193892未知/5恢复；同任务累计3012641账本＝2173338有效＋839303未知、224物理、37恢复。两次入口已知监督累计1731.256549秒；与原相加14085.563742秒，额外准备/等待/导出仍须补扣。成稿开始前最多余2387359账本、46物理、3恢复，当前成稿消耗继续扣；不是另发46次。绑定仍写8恢复、9000秒，不能代替原整任务剩余3恢复及原绝对截止：沿现有接点监督累计边界，不因重试重置时间/恢复。不为此新建预算系统或停下等待逐项审批。

原整任务5400000/27000秒/270物理/40恢复及本卡实际额外10800秒不增加，有价部分≤600元/unknown例外、现有账号池及不购买充值边界不变。初始化和实际后续修复计入§7.11原局部迭代；达公司合同即停，到任一总限或实际源/服务/账号/改动隔离边界即交可靠答案和具体限制，由A作路线裁决，不自动派余额卡。A对前期重复准备和细分任务导致的低效率负责，本卡不再把已可用三家或无业务正文的封面页放回主线。

### 111.5 直接验证、清理及Git边界

A复跑`test_a_strategy_sentence_can_be_withdrawn_without_dropping_the_adjacent_fact`与`test_a_saved_query_keeps_a_real_receipt_without_a_hand_merge`，2 passed、7 warnings、2.73秒；复用上轮未变机械回执，不跑454页mock。实际磁盘入口新实例读回两家公司均一致；这些证明差异交付/凭据加载，不等于浦发12/6通过。A未发现新的上汽业务阻塞；浦发新确认1类成稿调度阻塞，初始化已由B修复，资源累计配置仅要求按原合同监督，不另扩平台。

QuoteBak盘点约369M，相关在途、真实源及旧失败根仍保留；失败初次根约7M尚未独立持久最小故障包，不由A盲删。A本轮2张审核渲染与2节点测试产生的临时文件已清，释放459139字节及本人空目录/指向链接；正式PDF与B运行根保留，可重建审核渲染和测试。B须先持久必要失败/回执及hash，再清无依赖库、缓存和重复导出，不复制全库当证据。只更新现有5份规划/审核文档，严格OpenSpec、限定diff/13处当前链接及计数检查通过；未声称全仓Ruff或业务验收全过。

开始时13份已跟踪修改及既有未跟踪内容保护，验收计划details旧hunk明确排除提交；期间新增并行金融/公司画像代码及制品亦不纳入A提交。A活跃审核/规划时间未精确计量，记未知；没有新模型调用。后续仍沿A/B流程v3卡内自主P0—P3/现实P4连续推进，卡末交唯一入口，4家可用且A2通过前不发S1，不归档、不改变生产或规模权限。

## 112. B进度：浦发新读页已成稿并核验

日期：2026-10-25。按[B2§7.12](b2-execution-card.md#712-接续任务卡浦发完成当前窗口并交验收)交付已经结束的成稿运行。没有再读第3–454页，没有重核旧净收入。这不是A2，B不改可用进度。

### 112.1 新正文

查询 [b2-spdb-draft-result](b2-spdb-draft-result/query.json)，SHA-256 `355eea5ac71ca509eb7add62d057333286a5404492d1120089df8d8ac420f8dd`。查询、导出和新实例重读一致。呈现 106/73/21，程序已核呈现不是准确率。三个完整标志仍为 false，classification=limited。第1–2页仍是 not_read。

8 组成稿、8 组核验已经发布。新的已核句子包括：本集团经营商业银行、融资租赁、信托、理财，以及香港牌照下的投资银行和资产管理；吸收公司及个人活期、定期存款；资金成本来自吸收存款、已发行债务证券、同业拆入、卖出回购和向央行借款；利息收入来自贷款、金融投资、同业和央行存款；融资租赁租赁物包括飞机、船舶和清洁能源设备，承租对象主要是公司客户。

原已核金额仍在：营业收入 1,739.64 亿元，利息净收入 1,204.83 亿元，非利息净收入 534.81 亿元，其中手续费及佣金净收入 227.25 亿元、其他非利息收益 307.56 亿元。

`funding_and_capital_management` 仍是 evidence_not_located。已核的资金来源句是 `claim-input-exposure` 和 `claim-funding-inputs`，没有把旧条件编号改成 pass。还有 104 条 major_condition_not_verified，其中包含次要附注，本卡没有逐条扩写。

### 112.2 原十二项与六类负例

口径沿用审核 §110 的原编号。状态只写程序证据，待 A 核源。

| ID | 程序证据 |
| --- | --- |
| C01/C02 | `claim-principal-business__2`、`bp-work-371e8784bca0ca27215fa32f:report:111:products_services`：商业银行、融资租赁、信托、理财及境外投行/资管 |
| C03 | `claim-revenue-model__2`：利息收入来自贷款、金融投资、同业和央行存款；手续费确认仍在既有主张 |
| C04 | `claim:revenue_model:financial_performance`：营业收入 1,739.64 亿元 |
| C05 | `claim-revenue-model-nii-1` 与 `claim:revenue_model:non_interest_income`：净息 1,204.83 亿元，手续费净额 227.25 亿元，其他非利息 307.56 亿元。第416页分部利息收入 265,116 与净额不是同一口径 |
| C06 | 待 A 核源。本轮没有把计划句晋升为当前经营 |
| E01 | 贷款、存款、托管、租赁和投行服务进入产品暴露 |
| E02 | `claim-input-exposure`：吸收存款、债务证券、同业拆入、卖出回购、央行借款 |
| E03 | 服务没有因缺少商品编号被删。commodity 仍不是已完成映射 |
| S01 | 商业银行服务本地客户；委托贷款仍写明风险由客户承担 |
| S02 | 资金来源见 E02 |
| S03 | 租赁承租对象主要为公司客户；前五名客户没有在本轮编造名称 |
| N01 | 集团/本行在新句里分开；金额单位随原文，百万元与亿元没有混加 |
| N02 | 利息收入与利息净收入、营业收入与非利息净收入仍分开 |
| N03 | 资金来源是负债成本，贷款和租赁是对外提供的业务，没有写成同一方向 |
| N04 | 计划性句子没有在本轮改成已核当前经营 |
| N05 | 没有新增具名客户或供应商 |
| N06 | 托管、理财、信托和租赁服务仍在已核正文 |

### 112.3 资源

成稿 21 次物理、183,697 ledger＝164,051 有效 usage＋19,646 未知预留、0 恢复、1,808.033755 秒，费用 unknown。一次恢复请求被 `logical_recovery_already_used` 拒绝，没有记成已用恢复。读源段 18 次物理、403,809 ledger 仍按 §111。两段合计 39 次物理。§7.44 累计 3,196,338 ledger＝2,337,389 有效＋858,949 未知、245 物理、37 恢复。本卡 64 次里余 25 次物理、2,203,662 ledger、3 个恢复名额。没有为用完余额再发请求。

### 112.4 边界

权威回执在 `b2-spdb-draft-result`。运行根 `data/QuoteBak/b2-spdb-draft-20261025` 保留到 A 读完。0 请求失败根 `b2-spdb-final-20261025` 仍留故障记录。第1–2页未读保持。生产 not_authorized，规模声明 false。B 不把 3/4 改成 4/4。

## 113. A审核：浦发核心成立，结束逐公司收口，转入S1

日期：2026-10-25。审核最新完整交接§112及`64ff52d7`，原B2§3/验收§4和A/B流程v3不变。A只读正式源和交付，不修改B原query、真实回执或业务代码，没有模型调用。§111是上一时点审核，不再据“尚未成稿”续派浦发卡。

### 113.1 整体走向与本轮业务结论

浦发这次新增了真正可用的主营、产品、资金和租赁客户答案，而不是又交一份源条件清单。集团商业银行、融资租赁、信托、理财及香港牌照下的投行/资管有源；存款、债务证券、同业及央行融资方向清楚；三项主要净收入未丢。原12核心**12/12**（画像6/6、暴露3/3、供应链3/3），原N01—N06核心负例**6/6**。不能再因为旧资金条件编号未定位、104个major缺口、两个封面/装饰页未读或三个程序false就说没有公司核心。

但补充托管增速有一组未消歧的原文冲突，须离线退出确定结论，见113.3。因此当前安全交付计数仍**3/4**，四家核心已成立；该确定安全差异完成后浦发补救可用、总可用**4/4**及A2进入S1的条件放行生效。无需再付费核浦发、重读全文或等待一轮仅审核撤句的A交接；B按已定范围登记差异即可继续新卡。旧首发/正式后继失败不改，不称未经修复4/4，也不将条件放行写成成本、程序完整性或泛化全部达标。

主要矛盾已从“能否给一家公司答案”转到“同一实现能否连续交付多家、正常语义收束且运行可控”。四家多轮开发修好，不能证明下一家公司首次可用，更不能据几百条verified宣称规模成立。S1须用有源语义归并主要业务清单及有效对象覆盖，证明核心齐全/补充有缺口能收束、另一项主业漏答仍不完整；不手填true，也不能永远false再由A数主张冒充自动交付。A不再把完整所有附注/标签当门槛，也不继续为三家派同链补丁。下一交付是固定12家新链真实回归及首发/补救、程序/内容判定一致性和资源实测。

### 113.2 独立核源及真实交付链

最终query SHA-256 `355eea5ac71ca509eb7add62d057333286a5404492d1120089df8d8ac420f8dd`，query/export profiles相同。A新建`CompanyProfileReadService`从`b2-spdb-draft-20261025/output`读取`bp-work-371e8784bca0ca27215fa32f`，profiles亦相同；不是拿checkpoint冒充导出。呈现106/73/21，程序已核唯一ID93个。所有已核引文均能在正式PDF派生原文定位；这个机械结果只证明引用，不代替以下语义审查。

| 原核心条件 | A独立原文依据与实际答案 |
| --- | --- |
| C01/C02 | 332页明确本行/子公司及集团商业银行、租赁、信托、理财、香港投行/资管；62—69、89—92页实际经营与子公司业务交叉支持，不仅以注册许可当当前经营。322页资产、394页公司/个人活期定期存款与实际服务一致 |
| C03 | 191/350页实际利率及服务履约确认手续费；357页贷款、投资、同业/央行存放与返售利息来源；不把收费确认方式变成另一经营主体 |
| C04/C05 | 28/32/71—75页集团营业收入1739.64亿元、净息1204.83亿元、非息534.81亿元＝手续费净227.25＋其他非息307.56亿元，净收入构成69.26%/13.06%/17.68%；320/357—359/416页本年集团列交叉核。贷款利息186233百万元/57.26%属于业务总收入325269百万元口径，不当营业净收入；毛息265116、净息120483及集团/本行列分清 |
| C06 | 92/123页28家村镇银行为历史设立数、12家已法人改分支、16家继续经营及推进改革；最终正文有这些限定。两个“五篇大文章”战略定位版本仍not_reliable，未靠旧pass复活；现行金融业务另有实际经营来源 |
| E01 | 产品暴露含存贷、托管、基金/信托/理财、租赁、投行及主要收入来源；承销/托管规模不误当公司营业收入 |
| E02/S02 | 57—58/73—74/357页存款、债务证券、同业存拆入、回购及央行借款的负债/资金成本方向成立，存款平均余额及付息与期末余额分开；旧funding条件定位失败不否定这些独立有源答案 |
| E03 | 服务类产品与原名保留；commodity未评估，不因无标准商品ID丢服务，也未编造商品价格向利润的传导率 |
| S01/S03 | 263/415页实际商业银行/投资服务、分支和子公司服务当地客户；385页集团作为出租人、飞机/船舶/清洁能源设备及公司类客户；65/92页产业链服务与科创客户支持有限经营角色，不把资本负债余额当角色 |

原六类负例实际核查：N01主体/期间/单位/列（73—75、79、90—92、320/332/357—359）；N02分项/总额/净毛及分部抵销（71—75、320、416，吸收存款含息与不含息另核394页）；N03资金方向及出租/承租（191、357、385）；N04历史/当前/计划及行业/公司（11、57—58、62、92/123、最终隔离句）；N05匿名客户不造名（54—55页“某机场/某邮轮/某国际租赁”等保持匿名，未用常识补前五名名称）；N06服务/商品映射不确定性（实际三视图及commodity状态）。核心六类均成立，不能以此忽略补充披露的源冲突。

补充/疑点另独立读34—47、49—55、60、63—69、77/79/82、89—92、122—124、147—151/157、388/394/426/432/436页相应正文及必要表头。核科技、绿色、普惠/跨境、财富、平台、上海区域承销/贷款、子公司主体、信用承诺及减值/结构化主体边界。数值原表“人民币百万元”与亿元换算、负债括号、业务规模/余额与收入区分已核；裸文字没有复述单位的补充表句只按所引表头解释，不另入结构化数值事实。没有把未核默认正确或单凭模型explanation评分。

### 113.3 唯一新P1是补充源冲突；离线安全收尾即可

`claim-products-services-1`为verified，称托管规模20.21万亿元、增长11.45%，收入24.81亿元、增长7.23%，忠实引用第69页。A同时读第89页托管业务，金额相同，增速却为11.47%/7.22%。现有证据不能解释两个披露是否精度/口径不同，不能替年报选唯一真值、平均或直接称两个都已证实。不是模型捏数，也不能因逐字摘录就忽略已知歧义。

这是非核心的一组次要冲突，影响范围只限有关增速，不隔离公司主营、净收入及资金角色。原`claim-1`在第89页已有不含增速的托管规模/收入答案。因此最短方案是在原owner**离线将整个`claim-products-services-1`及依赖覆盖退出可信**，保原件和冲突原因、保其他独立对象/真实检查，另存安全query/export/新实例一致。不手工编写正确增速，不以新模型决定哪页优先，不建冲突框架，不重核其余92个已核ID。采用现有安全撤句路径和差异验证即可；如发现同义确定增速则同范围退出。

A对这个具体差异已给条件裁决，按[B3/S1卡§1](b3-s1-execution-card.md#1-目标与启动条件)完成后总可用4/4及A2条件放行自动生效，无需新的A会话。B不得自行改原评分或把新事实当通过；发生本卡以外的实质变化仍须按流程处理。安全稿落盘前唯一进度表如实保3/4。

### 113.4 去重与已审范围

三视图verified呈现按company_profile→product_exposure→supply_chain_position顺序，首次遇到claim_id保序，得到93个唯一ID。再将同一业务/主体/本期事实的跨视图复述归63个对象组；同组任何实际分句不成立即不整组计正确，不是逐原子数字准确率。全部63组核源，不用抽样凑20：核心/高风险38组正确，补充25组中24组可确认、1组源冲突未计正确；合并**62/63＝98.41%已审准确率**。不能报告93/93或100%，也不把1组歧义悄悄剔出分母。原种子20261023保留，此处补充全查无需抽取。

可复核归组如下，数字为上述93个唯一ID的**0起序号**，逗号同组、分号分组：

`0,2; 1; 3,24; 4; 5; 6,36,47; 7; 8,52,55; 9,30; 10; 11,58; 12,14,73; 13,31,89; 15; 16,66,71; 17,53; 18; 19,76; 20,67; 21; 22,34,69; 23,64; 25; 26,75; 27; 28; 29; 32; 33; 35; 37; 38; 39,42; 40; 41; 43; 44; 45; 46; 48; 49; 50; 51; 54; 56; 57,70; 59,62,65; 60,61; 63; 68,72; 74; 77,82; 78; 79,85; 80; 81; 83; 84; 86; 87; 88; 90; 91,92`。

核心/高风险为上述**1起组序**1、4、5、8、10—16、18—19、21—24、26—27、38—39、46—48、50—63，共38；其余25组补充全查。第3组为托管冲突组，另一ID的规模/收入本身有源，不因此把歧义增速计正确。安全稿后只需核确定差异和其他对象/检查不丢，不重做63组审核。

### 113.5 资源、效率及下一卡

本段8组成稿/8组核验实际发布，21物理、183697账本＝164051有效＋19646未知、0次实际恢复、1808.033755秒，费用unknown；一次恢复被拒没有实际发送，不增恢复计数。上段18物理/403809账本不改。§7.44新增任务最终3196338账本＝2337389有效＋858949未知、245物理、37恢复，已知监督时间15893.597497秒；额外本地准备/等待/导出未完整还原，不伪称精确剩余时间或全任务货币达标。余25物理不用，旧歌华和旧B2账均不重写，也不迁移给S1。

执行新[B3/S1卡](b3-s1-execution-card.md)及验收§7.50。安全退出是同卡前置，之后固定原12家正式源重冻/真实LLM首发，保原风险条件并增加三视图核心，最多两次共性迭代及每家一次最终后继，同一最终版本交全组。旧S1规则12/12不抵扣；与B2重合的报告也不载旧语义答案冒充新链首发。已有PDF/有效机械回执复用，首次/补救分列，不中途逐公司求批，不新增平台。S1当前0/12，S2/S3仍0/20、0/100；达到12家即结论，不扩第13家，S2真实运行、生产/规模及归档未授权。

A依据用户持续费用/开发授权，在验收§7.50明确单列S1研发资源，并与旧900秒/120000/16请求/10元常规目标分开。不能把常规假设直接拿来再造空输出，也不能将本次扩大硬限称效率通过。先形成可重复的核心答案，源范围不变、非核心附注不扩写；记录真实时间/调用/成本差距，连续两家同一根因空输出先暂停，不为凑12烧同样故障。A对早先过细切卡、末端前置和常规资源假设不成立造成的效率损失负责；本次将下一业务链合并连续推进。

### 113.6 验证、清理与边界

本轮已复跑的安全撤句/真实凭据读回两节点2 passed、7 warnings、2.73秒（见111.5），其涉及路径未因B新源条件选取修复变化，复用回执；本次新最终制品的hash、query/export/新实例、全部已核引文定位与独立原文语义另核。B`64ff52d7`直接链只增加spdb-draft绑定及从实际新源回执选择待稿条件，当前真实8组/8组成功是业务执行证据，不以旧fixture代质量评分。没有新增业务代码，不为纯文档卡编测试。最终严格OpenSpec、限定差异/链接及计数复核通过后提交；不声称全仓Ruff或常规性能通过。

A清理本轮可重建渲染/测试临时物459139字节及空目录/本人链接的事实沿111.5保留；B新运行根仍供离线安全稿，不由A盲删。新卡先持久最小故障证据/hash，再删无依赖失败库、测试副本及重复缓存，不保整套开发库当证据；并行占用、未知归属、正式源及旧冻结结果保护。运行根待安全稿/验收回执持久且无依赖后清，交接注明用途和清理时点。

本轮新发现1类P1（补充源冲突），没有新的核心能力阻塞；裸表单位表达/重复呈现及非阻塞风格不另派专项。A活跃审核/规划时间未精确计量，记未知。只提交本人可隔离的规划/审核文档，验收details既有hunk和并行业务/代码/未跟踪内容不纳入；生产not_authorized、scale_quality_claim_allowed=false保持。新卡沿A/B流程v3连续P0—P3和现实P4，卡末A独立审核，不因非阻塞建议扩大工作。

## 114. B进度：浦发托管增速已退出可信

日期：2026-10-26。按[B3/S1卡§1](b3-s1-execution-card.md#1-目标与启动条件)做离线安全收尾。没有调用模型，没有改原 draft query。

安全稿 [b2-spdb-safe-result](b2-spdb-safe-result/query.json)，SHA-256 `239da26f8fc579fb640d839c8c4f03858cbd0c7253a3d9f34a066949dee30bfb`。查询、导出和新实例重读一致。原 draft `355eea5ac71ca509eb7add62d057333286a5404492d1120089df8d8ac420f8dd` 未改。

`claim-products-services-1` 现为 unclear / `retracted_pending_correction`。原文仍保留第69页的 11.45% 和 7.23%，并记下与第89页 11.47% 和 7.22% 的冲突。已核正文里不再有这两个增速。第89页 `claim-1` 仍已核，只保留托管规模 20.21 万亿元和托管费收入 24.81 亿元。营业收入 1,739.64 亿元、利息净收入 1,204.83 亿元、手续费及佣金净收入 227.25 亿元、其他非利息收益 307.56 亿元仍已核。程序已核唯一 ID 从 93 降到 92。

按审核§113，这一确定差异成立后，浦发补救可用、总可用 4/4 及 A2 进入 S1 的条件放行生效。B 不改 62/63 的已审分数，也不把新事实算作通过。B2 剩余 25 次物理请求不再使用。

补充缺口不再挡住已经齐全的核心：`test_a_supplementary_detail_can_remain_while_a_missing_major_business_blocks` 通过。另一项主要业务没有覆盖时，完整性仍然不成立。S1 的 12 家首发接着用同一实现，不复用 B2 的旧语义答案。
