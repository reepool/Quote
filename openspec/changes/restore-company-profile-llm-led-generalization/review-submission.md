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
