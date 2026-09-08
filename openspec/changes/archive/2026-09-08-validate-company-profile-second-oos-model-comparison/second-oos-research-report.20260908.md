# 广东中南钢铁 2025 年报第二个 OOS 与三模型适配性复核

## 结论

本轮完成了三模型同请求对照，但唯一一次正式 OOS 运行结果为 `failed`，没有形成可复用画像 bundle。失败不能解释为“模型无法分析年报”：六次对照 extract 全部成功；正式运行先连续发生三次 Scorpio connect/DNS deadline，随后暴露冻结 Evidence plan 中的未知字段 `energy_input`。

模型适配性方面，本样本选择 `gemini-3.8-flash`：其与 Grok 的离线合同结果同为 overview 4/4、表格 24/24 complete，但 Gemini 两 scope 合计约 73.6 秒，Grok 约 213.0 秒；GLM 表格出现 occurrence conflict，scope 不完整。该结论只适用于本样本和本合同。

## 三模型同请求对照

| 模型 | structured parse | overview 离线合同 | 表格离线合同 | 两 scope 网关时延 | 主要发现 |
| --- | --- | --- | --- | ---: | --- |
| grok-4.6 | 2/2 | 4/4 accepted，complete | 24/24 accepted，complete | 212.963s | 无重复冲突；数值标点较多被规整 |
| glm-5.3-flash | 2/2 | 6/6 accepted，complete | 20/28 accepted，incomplete | 372.059s | 重复行触发 occurrence conflict，12 个人审项 |
| gemini-3.8-flash | 2/2 | 4/4 accepted，complete | 原始 28 条，经 occurrence reconciliation 后 24/24 accepted，complete | 73.607s | 语义结果与 Grok 同级，速度明显更快 |

两类 request hash 在三个模型间分别一致：overview `7b224aaf...7c50`，表格 `b4021016...b6cc`。对照只执行 extract，`verifier=not_run`；候选没有进入正式画像。

## 冻结 Evidence 与原文

- 业务概述/Activity，物理页 10–11，Evidence `stage5-evidence-37a588de249dec04022042e7`：
  > 公司主营范围包括制造、加工、销售钢铁冶金产品、金属制品、焦炭、煤化工产品（危险化学品除外）、技术开发、转让、引进及咨询服务。
- 分部财务，物理页 16–17，Evidence `stage5-evidence-a9ca583432d52da458a08914`：
  > 钢铁产品营业收入 22,503,383,794.45 元、营业成本 21,697,078,850.72 元、毛利率 3.58%。
- 产销存，物理页 16–17，Evidence `stage5-evidence-866ffd1fdf2a63f098ca266b` / `stage5-evidence-df00d5b37c0749ea6a030aa8`：
  > 销售量 7,317,012 吨；生产量 7,329,756 吨；库存量 77,243 吨。
- 原材料/能源，物理页 10–11、17：
  > 进口矿采购以国际大矿长协为主；煤炭采购以国有大矿长协采购为主。钢铁产品原燃辅料 16,490,836,728.60 元，能源动力 2,790,270,023.47 元。
- 客户/供应商，物理页 18–19，Evidence `stage5-evidence-5d6d665a2b2d407bb9f03598`：
  > 前五名客户合计销售金额 8,658,228,190.97 元，占年度销售总额 33.13%；前五名供应商合计采购金额 11,167,936,031.99 元，占年度采购总额 42.52%。
- regime，物理页 10–11，Evidence `stage5-evidence-3aa537b438c95572ea67c9fd`：
  > 报告期内公司的主要业务及经营模式未发生重大变化。

这些文本证明样本存在足够的结构化信息，但本轮正式 run 没有 accepted facts；上表数值只能作为冻结 Evidence 和 comparison-only 候选审计，不能冒充正式画像结果。

## 唯一正式 OOS 运行

- run ID：`stage55-second-oos-formal-000717-20260909-a`。ID 中的 `20260909` 是误写的未来日期标签，为保持网关日志 identity 不再改名；实际执行时间和失败清单 `created_at` 均为 2026-09-08 UTC。
- 选定模型：`gemini-3.8-flash`，route `semantic_extraction__scorpio_gemini`。
- 参数：extract 16,000；verify 12,000；单次 deadline 300 秒；最多 27 次 provider call。
- 前三个 scope：均在 connect 阶段记录 `dns_failure` / `ConnectError`，最终为 `deadline_exceeded`。
- 第四个 scope：在构造语义 checklist 时发现未知 `energy_input`，原子写入 failure manifest。
- 正式 accepted facts：0。合法空结论：0。正式 report state：`failed`。
- 无 `.stage5-tmp-*`，无正式 bundle，无跨模型或历史 run 拼接。

## 代码修复

semantic run 现在会在任何 provider 调用前，对全部 prepared scope 的 field IDs 做 `_FIELD_CONTRACT` 闭集检查。该修复不会新增 `energy_input`；现有合同继续以 `material_input` Relationship 同时承载原材料和能源输入。

本次已冻结 Evidence plan 保持不变，因而失败审计可复现。后续若继续验证，必须另开 change、新 Evidence plan revision、新 run ID，并在模型调用前把两个原材料/能源 scope 收敛为 `material_input`。

## 可用性与授权

- 模型适配性证据：有；Gemini 是本样本的优先模型，Grok 是较慢但语义稳定的备选，GLM 本次不适合作为主模型。
- 第二样本完整画像：没有，本轮状态 `failed`。
- 是否支持 bounded reuse：模型抽取层“部分支持”；完整 Stage 5 运行“尚未证明”。
- `production_authorization=not_authorized`：结果只允许研究审查，不得写 approved 表、启动 scheduler/backfill、发布商品暴露/价值链或进入 Stage 6。
