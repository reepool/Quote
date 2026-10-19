# 公司画像开发路线纠偏：文档交付与审核

日期：2026-10-16。结论：规划材料具备实施条件；不是LLM业务验收通过，不启动S2或生产。

当前状态（2026-10-19，[A审核§43](#43-a审核1bcfd517结束整组合成方案重整分批业务交付)）：B1/A1未通过；第9次失败及上下文/查漏标记修复接受，R30按批核验保存未完成，B1-J结束，不批准第10次。当前仅[执行卡§23](b1-execution-card.md#23-b1-k按批核验与持久交付重整开发当前卡)/R32—R33重整按批合成/核验/原owner持久交付及局部语义，真实入口/模型请求/付费新增0。九入口累计2110.163851秒/21请求/439992账本，费用unknown；真实已核主张仍0。观察1/4、可用0/4；S1 0/12、S2 0/20、S3 0/100。不归档、不扩批，生产not_authorized、规模声明false。原§1—42正文不改写。

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
