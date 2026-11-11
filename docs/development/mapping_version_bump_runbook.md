# 映射目录版本升级 Runbook（v6→v7→…）

> 对应教训：2026-11 的 V6 事故链（目录空窗 45h + 存量 v5 戳被读侧拒绝 + unchanged-skip 阻断自愈 = 对账 12h 死循环）。
>
> 本 runbook 是 `backfill-financial-fact-lineage` change 的移交交付物（tasks 4.1）：在结构性修复（入库去重键纳入 mapping_version）落地前，**任何 MAPPING_VERSION 升级必须按本流程执行**。

## 为什么升级必然复现（不做防护时）

1. 读侧血统过滤要求 `local_core_mapping.mapping_version == 当前版本`——**全体旧版戳行在切换瞬间失准**（这是版本语义的设计行为，非 bug）；
2. 抓取层 unchanged-file 跳过使旧行永不重解析 → 不会自然刷新；
3. 若切换时目录未装载（V6 事故第一环），新导入也不盖戳。

三环叠加 = 全市场伪缺口死循环。**任何一环被流程阻断即可防复发。**

## 升级流程（强制顺序，缺一不可）

```bash
# ① 代码合入新 MAPPING_VERSION 前：先装载新目录（杜绝 45h 空窗重演）
python scripts/financial_pit_panel/load_mapping_catalog.py   # 以仓库实际入口为准

# ② 验证目录装载量级（应与上一版同量级，v6 为 521 approved）
sqlite3 -readonly file:data/financials.db \
  "SELECT mapping_version, COUNT(*), SUM(approved_for_core) FROM financial_source_field_mappings GROUP BY 1"

# ③ 重跑血统刷新迁移（幂等，按当前代码的 MAPPING_VERSION 自动定向新版本；
#    旧版戳→新版、未盖且可命中→补盖、新版已收回→保留）
bash scripts/quote_dev_env.sh python scripts/financial_fact_lineage_backfill.py apply --table both
# 预期：首次在新版本上运行会刷新全量旧版戳行（v6→v7 时≈700万行、~2h 量级）

# ④ 验收
bash scripts/quote_dev_env.sh python scripts/financial_fact_lineage_backfill.py verify --sample 1000
#   mismatches=0 且 stale_version_seen=0；探针标的 ready=True

# ⑤ 观察首个对账/增量窗口：修复目标无全市场量级骤增、无 0.2s/标的空转批次
```

## 窗口与回滚

- ③ 与迁移同规约：无 financials.db 写者窗口（白天增量前或凌晨 ≥02:00）；
- 回滚：`financial_fact_lineage_backfill.py rollback`（瘦身备份表整批还原，保留至连续两个周日验收后清理）。

## 结构性修复（owner 域，本 runbook 为过渡）

根治 = 入库去重键纳入 mapping_version（或版本切换时失效源文件去重），使"数据未变但准入语境已变"的行在下次自然导入时自动重盖戳。归属映射版本生命周期 owner（V6 变更 `c402cd08` 一系）；落地后本 runbook ③④ 可退化为抽检。
