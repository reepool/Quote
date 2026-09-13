# 公司画像：按事实交付的研究 MVP

## 已交付的用法

从已运行的批次导出 accepted 事实，报告不完整不阻止读取：

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/export_company_profile_research_data.py \
  --batch-directory var/company_profile_expanded_cohort/20260913/batch-manufacturing-materials-expanded-cohort-eight-pool-20260913-a \
  --output-directory var/company_profile_research_delivery/expanded-eight-partial-v2
```

输出包含 accepted-facts.csv（统一事实表）与 accepted-research-data.json
（完整对象、证据、用途限制、未决项和 coverage）。相同输出目录不能覆盖。
本次八报告可交付 742 条 accepted_for_review 记录，仍需遵守逐条用途限制。
它们是现有 verifier 接受的研究数据，导出不构成另一次语义验证。

## 改变的完成标准

- 单条事实正确就可交付；未决候选单独排队，缺项不冒充零或未披露。
- 报告完整性与事实交付分别展示，报告 hold 不封锁其他 accepted 数据。
- 不要求每家公司字段齐全、人工项清零或 Gold 全满；复核数量是成本指标。
- 原文数值、单位、对象、期间和证据错配仍应隔离，不能靠降低门槛接受。
- 沿用已有 report_default_group_scope 政策；明确母公司、分部、子公司证据优先。
- 结构检查不等于语义准确率。a3dc726 的结构检查自动打成 correct 的审计已撤回，
  详见 active change 的 AUDIT-CORRECTION.md。

## 距离持续规模化运行的实际工作

1. 章节合并：本批次 10 个 required_coverage_missing 项在本报告已有同字段
   accepted 记录；逐一核对对象、期间和 owner 后修正合并判断。同字段本身不证明等价。
2. 修正已出现的表格 schema 丢行与同页不同 Activity 冲突；只隔离错误记录。
3. 复用现有采集 owner 补齐持久化、幂等续跑与错误隔离的运行验收，先做 50 家
   制造/材料公司的限额试运行。失败任务不阻塞已经完成公司的交付。
4. 试运行按真实原文抽样衡量事实准确性、关键数值错误、每家公司成本与人工量。
   精度建议门槛为 95%，关键数值错误须修复或隔离；这是后续试运行建议，
   本次导出没有估计准确率，也没有宣称已达标。
5. 试运行达到上述条件再扩大至数百家。其他行业先选代表样本核对字段语义，
   不把制造/材料的物理量、产能规则直接套给银行、保险或软件企业。

交付入口已可用；50 家新试运行、持续采集以及生产 approved 接入尚未完成。
本次没有开启交易、估值、商品暴露或价值链自动发布。
