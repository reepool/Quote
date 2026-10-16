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
