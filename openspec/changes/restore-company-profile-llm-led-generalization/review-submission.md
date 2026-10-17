# 公司画像开发路线纠偏：文档交付与审核

日期：2026-10-16。结论：规划材料具备实施条件；不是LLM业务验收通过，不启动S2或生产。

## 1. 唯一实施指引

- [产品总需求](../../../docs/development/company_profile_product_and_industry_semantic_requirements.md)：公司画像、产品暴露、供应链定位；LLM语义/程序硬约束分工。
- [验收计划v2](../../../docs/development/company_profile_acceptance_program.md)：数量、门槛、预算、补救和唯一进度。
- [A/B顺序任务卡](../../../docs/development/company_profile_delivery_plan.md)：当前只推进A0/B1，一家公司真实LLM纵向闭环，A1为首个审核点。
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
