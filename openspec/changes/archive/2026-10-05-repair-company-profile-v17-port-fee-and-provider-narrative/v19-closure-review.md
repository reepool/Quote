# v19 收口交付与 B 角自检

三张卡 4.1–4.3 已执行，等待 A 角验收。未归档、未扩批、未授权生产。v18 的原始快照及分数不改写，审核限制独立保存在 `v18-review-limitations.md`。

## 业务交付

- 上港真实完整 p12 的“公司经营模式”进入接受 Overview 正文、查询及导出收入答案，包含港口及相关服务、港口作业包干费、库场使用费、港口其他收费。覆盖 公司/公司的/本公司/本公司的 合法变体。计划收费、否定收费、第三方收费仍拒绝，免费附带服务不抹掉已成立的收费机制。
- 万东提供商定位绑定当前公司主体，保留含 2025 年及“谋篇布局‘十五五’”发展的完整来源句；子公司、第三方、公司拟作为/计划进入不升格为公司当前主营或产品。反例同时覆盖简句和完整真实 p11 的主语/计划替换。
- 观察从 runtime `reused_scope_ids` 及其持久化 stage_results 记录章节复用，独立于 enqueue 标记和 fresh token 消耗。部分章节复用同时产生 fresh token 时保留复用状态及成本；新进程读取队列记录同样保留复用事实。

## 正式轮和唯一复核

沿用冻结计划 `a87005165e1d0bd5354de938178669938235cf9525459355ae4e948166400662`，使用累积 `revenue_sentence_repair=v19` 身份及首次创建的 `m4_v19_closure` 输出目录。调用现有 execute→query→export owner、真实资产访问与两份冻结 PDF；正式轮未注入 fixture 页面，未删除旧结果后重跑。

| 项目 | 实测 |
| --- | --- |
| 上港 execute 至 export 返回 | 162.28588021826 秒 |
| 万东 execute 至 export 返回 | 89.40375118143857 秒 |
| 首次 execute 至第二份 export 返回，含两次查询/导出 | 251.68984960764647 秒 |
| shared tokens | 0 / 50000 |
| persisted runtime 章节复用 | 两家公司所有阶段均 `[]` |
| scoped source recall | 26 / 26 |
| unique source accuracy | 51 / 51 |
| critical numeric errors | 0 |

Token 为 0 是本轮确定性抽取没有 provider 调用，不是扣除复用成本。两家公司均处理 overview、segment 两章，新执行身份无 predecessor lineage。`expansion_gates_met=true` 为门槛计算结果，仍须 A 角审核，不能据此自行放行下一组样本。

准确率按实际查询交付构建唯一 `(instrument_id, answer dimension / record_id)` 集合，并核对导出 JSON 与查询 profile 完全一致：上港 20 条事实、万东 25 条事实、6 个答案。每个对象只有一条 finding；收费 Activity 不再用另一个名字重复计分。

召回分母来自独立阅读的固定验收范围：6 个答案披露、19 个当前年收入表行、1 个收费机制。收入表行通过 Segment 计一次，Measurement 另作准确率判断；收费机制由上港 Overview 承载，收费 Activity 仅作一次准确率判断。复核 schema 的 `disclosed_in_source=false` 在这些 finding 中表示“仅计准确率，避免重复召回”，不表示文字不在来源里。逐对象映射、人工源表数值、单位、分组、实际页码及文件 SHA-256 见 `v19-review-evidence.json`。

无商品角色反例仅覆盖实际核对的证据范围：上港 p12/18/19、万东 p11/24/25。未声称整份报告没有商品风险，亦未将笼统“燃物料”推断为特定油品角色。源表保留报告期与公告可得日，未借用未来资料。

## 验证与 Review 范围

定向验证命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_m4_next_batch.py \
  tests/unit/test_research/test_company_profile_source_review.py \
  tests/unit/test_research/test_company_profile_core_assessment_projection.py \
  tests/unit/test_research/test_company_profile_v19_closure.py \
  tests/unit/test_research/test_company_profile_v7_source_delivery.py \
  tests/unit/test_research/test_company_profile_revenue_sentence_repair.py \
  tests/unit/test_research/test_company_profile_repair_isolation.py
```

上述定向测试 **96 passed**（7 条既有依赖/配置警告）。OpenSpec 严格校验、修改文件编译及 diff 空白检查通过。B 角手工 Review 核对当前代码 diff、主体/计划反例、运行身份、runtime 复用、计时边界、对象唯一性和冻结源表。原审核四项 A 类问题均完成对应修复；没有为 B 类风格/架构建议扩展范围。

扩大检查时，runtime 测试出现 18 个失败；在任务前未修改 HEAD 的独立 `/tmp` 副本复现同样 18 个失败，为 C 类既有测试/确定性抽取预期不一致，本卡不处理。定向验收结果独立列示，不声称全仓测试全绿。

万东 overview scope 仍记录 `explicit_activity` 的 `required_coverage_missing` 与 `task_complete=false`，三维答案及接受 Overview 已交付。本轮 100% 仅适用于声明的六维答案、源表行、收费机制及实际接受事实；不证明所有字段或全年报披露均完整。

## 本地审核材料

- 正式执行及计时：`data/checkpoints/company_profile_common_core/reports/m4_v19_closure/formal_execution.json`
- 运行观察：同目录 `observation.json`
- owner 写入的独立源复核：同目录 `company_profile_source_review.v1.json`
- 上港导出：`data/exports/m4_v19_closure/600018.SH/600018.SH_2025-12-31.json`
- 万东导出：`data/exports/m4_v19_closure/600055.SH/600055.SH_2025-12-31.json`
- v19 runtime/checkpoint：`data/research/company_profile_common_core/m4_v19_closure/company_profile_common_core.v1/`

这些运行数据遵循仓库原有 ignore 策略，未将大型 runtime 输出加入 Git；本 change 提交审核摘要、51 对象映射、源表及原始输出哈希供核验。所有 12 份受保护 v18 文件经 SHA-256 核验保持原样。
