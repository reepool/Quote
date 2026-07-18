# A 股业务画像免费结构化来源 Pilot 基线

## 1. 结论

2026-07-18 已完成六个首批行业各 5 家、共 30 家的只读 pilot。东方财富主营
构成和同花顺主营介绍均达到 30/30 请求成功，说明当前受控 HTTP transport、
timeout、pacing、来源隔离和解析链可用。

当前仍不满足生产 candidate promotion：

- 东方财富 24/30 家达到 `possible_row_cap=200`，历史完整性不能确认；
- 2,797 条产品行仅 61 条命中精确产品别名，覆盖率约 2.18%；
- 未匹配 2,698 条，约 96.46%；歧义 38 条，约 1.36%；
- 尚未完成与正式报告的材料性样本核对，不能计算要求的 99% mapping
  precision；
- 部分源报告日期为 `2015-04-30`、`2021-10-31` 等非标准期末，需要逐条
  复核源语义。

因此生产配置和 scheduler 继续关闭，不执行 production candidate backfill。

## 2. 样本

样本来自项目现有 point-in-time 申万行业和上市生命周期 universe，并复用
benchmark selector 做交易所和上市年代分层：

| 维度 | 覆盖 |
|---|---:|
| 行业 | 煤炭、有色及固体矿产、钢铁、石油石化、基础化工、建筑材料各 5 家 |
| BSE | 4 |
| SSE | 8 |
| SZSE | 18 |
| 1990s / 2000s / 2010s / 2020s | 11 / 7 / 6 / 6 |

selection 本身仍标记 `evidence_incomplete`，原因是正式报告 benchmark 证据尚未
覆盖。这不影响其作为结构化来源只读 probe 名单，但不能替代后续官方报告核对。

## 3. 来源结果

总运行耗时约 32.03 秒，无失败公司，不写 candidate、approved、公司商品暴露
或 DCF 输入。

### 3.1 东方财富主营构成

| 指标 | 结果 |
|---|---:|
| success / empty / failed | 30 / 0 / 0 |
| 原始行 / 规范行 | 5,554 / 5,554 |
| 总请求耗时 / 单公司平均 | 9.91s / 0.33s |
| 触发 200 行疑似上限 | 24 / 30 |
| 报告期范围 | 2010-06-30 至 2026-03-31 |

固定字段共 11 个：股票代码、报告日期、分类类型、主营构成、主营收入、收入比例、
主营成本、成本比例、主营利润、利润比例、毛利率。

| 可空字段 | 空值数 | 空值率 |
|---|---:|---:|
| 主营收入 | 7 | 0.13% |
| 收入比例 | 107 | 1.93% |
| 主营成本 | 1,020 | 18.37% |
| 成本比例 | 1,060 | 19.09% |
| 主营利润 | 1,072 | 19.30% |
| 利润比例 | 1,097 | 19.75% |
| 毛利率 | 1,110 | 19.99% |

成本、利润和毛利字段不能作为全样本必有字段；缺失必须保持空，不得由收入或比例
反推。

### 3.2 同花顺主营介绍

| 指标 | 结果 |
|---|---:|
| success / empty / failed | 30 / 0 / 0 |
| 原始行 / introduction | 30 / 30 |
| 总请求耗时 / 单公司平均 | 21.19s / 0.71s |

30 家均返回股票代码、主营业务、产品类型、产品名称和经营范围。该结果只证明
当前页面字段存在，不赋予其历史报告期，也不允许从叙述自动推断价值链关系。

## 4. 产品标签结果

| 行项目 | 行数 | 占产品行 |
|---|---:|---:|
| 产品行 | 2,797 | 100.00% |
| 精确唯一命中 | 61 | 2.18% |
| 未匹配 | 2,698 | 96.46% |
| 歧义 | 38 | 1.36% |

这是目录 coverage，不是 precision。下一步应按最新来源行去重后生成未匹配/
歧义审计，优先处理高频、高收入占比和跨公司重复标签。每个新增别名都必须回看
正式报告并通过受控 promotion command；不得为了提高覆盖率增加模糊或包含匹配。

## 5. 可复现证据

本轮不写生产数据库。临时产物：

```text
/tmp/business_profile_structured_pilot/stratified_selection.json
/tmp/business_profile_structured_pilot/stratified/report.json
/tmp/business_profile_structured_pilot/stratified/checkpoint.json
/tmp/business_profile_structured_pilot/stratified/raw/
```

共保存 60 个内容寻址 gzip raw snapshots 和 1 个 run manifest。关键 hash：

| 产物 | SHA-256 |
|---|---|
| stratified selection | `8f5e073ea92bb059ba927e29e2fb0b76b0dadbe3b435de878414c5af01ba2e31` |
| sync report | `6dabbb4a9343e1c9cf171c087c6b6bebc3c3dd4b8939ea8f58130019eee56cd2` |
| raw run manifest | `7cc7f17aa0fc02d4c9ea56ee0527edfd0f28989559edd46c8d10a33bd6b4a5c8` |

`/tmp` 产物不是长期生产存储，以上 hash 用于本轮复核。后续 promotion 评估应将
冻结样本和正式报告核对结果放入受治理的 benchmark manifest。

## 6. 下一步门槛

1. 从 30 家 raw snapshots 生成按最新来源行去重的标签频次和材料性审计；
2. 对高频标签抽取正式年报对应主营构成页，核对名称、分类、期间和数值；
3. 只有人工核对 precision 的 95% 置信区间下限达到 99%，才允许升级目录；
4. 对 24 家疑似 200 行上限样本确认最新两年是否完整，并定义历史截断语义；
5. 修复或隔离非标准报告日期，避免进入历史 DCF 时点；
6. 完成正式报告精度 gate 后，才允许单行业 production candidate pilot。

## 7. 临时库回补验收

同日使用相同 30 家名单在全新 `/tmp` research DB 完成三阶段验证：

1. 首轮真实请求：1 个同花顺来源瞬时失败，写入 59 evidence 和 5,554
   candidate segments；
2. checkpoint 恢复：只重试失败的 `instrument × source`，补写 1 evidence，
   segment 新增为 0；
3. 全量重放：60 个来源 payload 均为 unchanged，evidence/segment 新增均为 0。

最终临时库：

| 项目 | 数量 |
|---|---:|
| candidate evidence | 60 |
| candidate segment | 5,554 |
| approved evidence/segment | 0 |
| value-chain role | 0 |
| company commodity exposure | 0 |
| ingestion runs | 3 |

`PRAGMA quick_check=ok`，`idempotent_replay_run=true`，
`zero_candidate_to_dcf_leakage=true`。验证产物：

```text
/tmp/business_profile_structured_pilot/temp_backfill_20260718_v2/
```

| 产物 | SHA-256 |
|---|---|
| validation report | `eeacbf62a4423ac0dbbd81828fb8e0bb8c03aff336eaa2c0a0152501855a4eb0` |
| isolated research.db | `b7a9cfb0663652e029519bd340becfa07766f1b832ee3cab1f91bfeef23e3815` |

该验证证明 candidate 写入、失败来源恢复和同 payload 重放语义正确，不改变产品
目录覆盖率不足和正式报告 precision 尚未验证的结论。
