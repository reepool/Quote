## Context

v36旧change有限收口后验证下一对未见报告。服务/制造仅抽样标签，画像继续全行业通用三维合同。

## Goals / Non-Goals

Goals：完整38家排除、官方版本重复冻结；执行前新来源实质合同，现有owner两家公司共享预算、首次交付与逐对象复核。
Non-Goals：全年报或规模/生产授权；量价、成本、行业、正文精简、性能和无关技术债。

## Decisions

- 复用freeze_m4_next_batch_plan，显式delivered_ids及既有SSE/SZSE/BSE、代码升序；v36完整身份及五开关，cutoff2026-09-17，独立m4_v36_unseen_small_batch。本卡不入队或预演。
- 独立原PDF和必要续页重建正文/原生重要收入/具名肯定当前动作，不用上一轮分母或实际模型输出来确定召回条件。主体/期间/状态/动作/原栏目/金额/单位及负例检查范围同时保存。
- execute_published_task依次run/query/export，第二家remaining_token_budget；monotonic整轮覆盖首次execute至第二export及重试。数据库stage_results.reused_scope_ids与predecessor_lineage为真实复用依据，允许实际已有PDF解析缓存但计时不隐藏。
- 每实际事实和已答正文唯一计分；正文核实质，Measurement仅准确率，角色别名/多证据不重复召回，准确率动态分母。完整bounded_quote与continuation_pages一起核。
- 原change归档路径映射后哈希保护，首次失败/超限原样保留；只登记真实业务缺口，后继修复需新身份/独立目录，有限验收后才另行收口。

## Risks / Trade-offs

新披露可产生业务缺口→可信首次观察保留并暂停后续。有限检查页不等于全年报完整；生产not_authorized、规模false。

## First formal finite failure observation

The independent pre-execution contract fixed170conditions (6substantive bodies,116income conditions,48explicit actions/relations) for600037.SH and600075.SH. First owner delivery takes221.739774seconds and0/50000tokens with no five-stage or predecessor reuse. Recall59/170 and unique accuracy107/122 (118accepted facts+4answered bodies) fail acceptance;critical numeric errors are0. Two unanswered principal-business bodies count only as recall omissions. Fifteen erroneous objects comprise one three-row concatenated commodity Activity,ten incorrect native-column facts and four incomplete answers. Original source bytes,first18delivery files and two review artifacts are preserved. Scope helper-label interpretations are attached separately without changing original source actions,conditions,denominator or output. Further batches pause and this change remains unarchived;only source-backed cards4.1–4.4 are recorded. Production remains not_authorized and scale quality false.
