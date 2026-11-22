# 6.EZ B交付：有源正文保留、指标局部限用与新版脚注转录

已交新20股研究根，**599条可读事实记录/932个可消费指标**，较EY恢复8条有源描述和12个原指标；15个不可靠指标仍隔离，4个粗粒度原对象仍不可读/仅源参考。以上是消费记录数，不是新的正确率成绩。新真实模型/供应商usage/连通性探针均0；EY原97.8967%及其失败、作者与审核记录不改，不扩100/生产。

## 交付与使用

唯一新研究根：`/home/python/Quote/data/research/company_profile_metric_safe_release_20261122`。当前API为`annual-api.json`；原owner导出在`exports/annual_profiles.json`、`exports/annual_profiles.md`，同一集合含CSV及单股JSON/人读稿。旧EY根及旧20根未改；显式指定root，不切共享配置。

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python - <<'PY'
import json
from research.company_profile.reads import CompanyProfileReadService
root = '/home/python/Quote/data/research/company_profile_metric_safe_release_20261122'
print(json.dumps(CompanyProfileReadService(root).research_profile('600348.SH'), ensure_ascii=False, indent=2))
PY
```

原描述/metrics值、名称、主体、期间、证据、generation、模型quality/作者/usage均未重写。8事实的消费决定来自A的`a-source-use-decisions.json`，按exact fact hash和原metrics索引绑定，不进生产规则；原writer保存40条新副本。metric_gaps给原索引、原name、state/reason，不给坏value；安全指标保持原值，并用metric_source_indices保原位置关系。limited及information_gaps/suggested_acquisitions均机器可读。旧整条隔离的8条独立显示说明在新副本中细化为“正文恢复/指定指标仍缺”；原模型limitations前缀、旧根原说明不删。

示例见`final-verification.json`：华阳F007为limited，metric_gaps[0]为原索引0、name=煤炭产品销售收入、state=withheld，不含value；原公司总收入指标按原索引1保留。四个粗粒度对象华夏F25/F37、学大F12、兴证F34不因该能力解除。

## 最小源码改动

- workpaper.py：可选WorkpaperMetricRestriction；exact fact hash绑定全原metrics及次序，未知/重复/负数/bool/越界索引拒绝；无新增字段时serializer省略空值，旧bytes/hash不增字段。LLM draft不能提供UseReview。
- workpaper_delivery.py：仅有明确独立决定才保原描述、过滤指定metrics并标limited/缺项；whole-fact restriction优先。坏正文/错证据不自动救，未推断新数或改标签。
- reads.py与原renderer：query/export/CSV/Markdown沿同一安全投影；新component链内选择现有hash验证的当前后继，不另建查询链。显式work_id仍是历史单轮读取，raw workpaper明确for_audit，不是安全numeric payload。
- workpaper_review.py：原保存计划可序列化保细粒度决定；内容/次序变化无法重绑旧位置时，继承为更保守whole-fact withheld，不丢限制或让模型自动解禁。
- core.py/sections/processing identity：原PypdfNativeAdapter最窄opt-in脚注保留；仅新v8启用，默认PDF消费者及旧v7不变。无其他parser、股票常量或数字修复规则。

### A提出P1及修复

初实现只把仍有metric_restrictions的节点列为query候选。A指出base→partial child→模型改变后coarse grandchild会因末节点不再有该字段而回退base，可能复露指标。

已改为：在已验证同报告/hash链存在component决定时，选**实际最新后继**，包括收紧为coarse的节点；快照不一致时拒绝而不回退base。export仍核同报告、当前workpaper及原链。三节点真实writer→新ReadService→query/export回归验证：最新为coarse-grandchild，fact=null；父/子显式work_id历史可读，annual lineage保留，坏指标不复露。未扩大旧无component决定的默认选择语义。

## 脚注供源

新身份`toc_sections_native_layout.v8.structured-note-markers`，packet分别用`shared-pdf-pypdf-layout.structured-notes.v1`或带限制的`shared-pdf-layout-with-native-fallback.structured-notes.v3`。v8的table_context警告会在实际抽取payload.source_text片段旁内联，非只存在reading_scope。旧v7仍沿原分支。

原native owner在显式layout opt-in时，同一pypdf页用visitor取得字号/基线/位置；同时要求独立页下注释定义、上标相对位置、紧邻较大数量及唯一原布局锚点，才把原note glyph结构化为[注N]。普通小号表格数、无注释对应、科学计数/公式样式等不据小字猜数；锚点歧义保原文并给限制。没有计算或补写数量。

真实已见华夏p44得到[注4]4,012.44、[注5]2.02、[注6]56.11。仅将三个标签换回原单字note，可逐字重现**整个**old layout，包括表格对照、其余字形及顺序。原F25/F37及旧响应不改、不绑新供源。`transcription-verification.json`还精确复建了原v7 source_text，与原receipt002实发内容相等；旧偏移通过原validate_pages。主A另独核华阳21/格力24新旧实表一致，此处不以程序自证替代A原文判断。

## 精确验证与边界

所有命令使用Quote解释器，先source统一开发环境；新增验证均离线/受控，不是新真实模型成功。没有重跑整仓或旧37用例。

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_metric_use.py tests/unit/test_research/test_company_profile_note_layout.py tests/unit/test_research/test_company_profile_workpaper_delivery.py tests/unit/test_research/test_company_profile_workpaper_ordered_remap.py -q --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-metric-safe-consumption-20261122/combined-tests.xml
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_metric_use.py tests/unit/test_research/test_company_profile_note_layout.py tests/unit/test_research/test_company_profile_review_execution.py -q --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-metric-safe-consumption-20261122/recovery-tests.xml
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_metric_use.py tests/unit/test_research/test_company_profile_note_layout.py -q --basetemp=data/QuoteBak/company-profile-6ez-b-md4SqAgB/pytest-close --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-metric-safe-consumption-20261122/close-tests.xml
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_metric_use.py -q --basetemp=data/QuoteBak/company-profile-6ez-b-md4SqAgB/pytest-p1-final --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-metric-safe-consumption-20261122/p1-final-tests.xml
```

依次54/41/26/16通过、0skip，范围重叠不相加。前两组为相关原owner回归及保存阶段；最终26覆盖完整三节点及v8实际请求警告，末次16是P1快照最终直接复验。本轮先前一次夹具错误漏ReviewRecoveryLink必填字段、两次观察断言误取request顶层而不是payload，原失败XML保留；修夹具/观察位置，未删业务断言。准备脚本曾误读旧manifest不存在的work_id键，发送0时改由原路径stem精确定位，原数据未变。

`git diff --check`通过。OpenSpec strict通过（命令设置OPENSPEC_TELEMETRY=0 DO_NOT_TRACK=1）；首次只读OpenSpec状态工具曾意外尝试其默认遥测并DNS失败，后续已进程级禁用，无业务画像/模型发送或共享配置修改。

数据闭环实际经原writer/ReadService query、export、CSV/Markdown及新实例；599/932、8正文/12安全指标恢复、15指标名称/缺项、4粗粒度状态、原model限制前缀和旧源hash见consumption-verification/transcription-verification/final-verification。后续P1仅改变三节点收紧分支，本20股单后继的数据/导出内容无变化；用专门三节点受控原owner回归证明，不重复完整导出或模型。

## 保护、清理和Git交接

EY原研究release87文件、首发257文件及更早138保护文件全hash一致，正式PDF/有效cache/raw/预算/旧20交付均保留。当前副本及9个本人代码/测试文件hash在implementation-freeze.json；旧97.8967%及原400样本裁决不改，当前仍不获100/生产权限。

仅本人QuoteBak根`company-profile-6ez-b-md4SqAgB`及已核归属的pytest-37（本轮新component用例与确切review-execution节点集合、无lock）清理；重复首次导出、visitor全文、临时脚本及测试库不永久归档，必要证据一套见本目录，释放字节与确切状态见cleanup.json。原6EY确切根开工已核不存在。其他未知或并行缓存不删。

基线90ce0055/master，B未暂存/commit/push。本人写集为上述7个业务owner文件及2个新增测试；未改脏core_skeleton/core_evidence_selection、配置、scheduler或A管理文档。A负责独立内容/最终Review及Git。

本轮是已有真实内容的消费能力与转录工程交付，不是新公司/新LLM语义验收；B活跃实施约一小时（估算），等待主要为直接回归及原owner导出，无额外付费/逐字段审批等待。
