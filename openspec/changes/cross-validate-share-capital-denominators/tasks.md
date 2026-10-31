## 0. 开发前置验证（D0，结论回填 design 后才可继续）

- [ ] 0.1 THS 股本时点语义：选 3~5 只 2024–2025 有明确股本变动（增发/回购
      注销）的公司，比对变动前后报告期 THS 股本 fact 是否随之变化，结论
      （期末 vs 抓取时点）写入 design D0-1。
- [ ] 0.2 valuation market_cap 语义分类：按 `source`/`calc_method` 分组抽验
      总市值 vs 流通市值；输出允许进入取值优先序的来源清单，写入 design D0-2。

## 1. 映射层单位机制（卡 1）

- [ ] 1.1 `financial_source_field_mapping.py`：THS equity 行声明
      `source_unit=shares`；实收资产行 `source_unit=CNY`、
      `approved_for_core=false`；`MAPPING_VERSION` v5→v6，走既有
      upsert_financial_source_field_mappings + audit 管道。
- [ ] 1.2 `financial_fact_aliases.py`："总股本"别名移至 `shares_outstanding`；
      `share_capital_amount` 增加混合来源语义注记。
- [ ] 1.3 aliases 加载断言：股数类 canonical `unit=='shares'`、金额类
      `=='CNY'`，违反即抛错（含单测：违规别名被拒）。
- [ ] 1.4 映射管道回归测试全绿。

## 2. 全期间坏行扫描与隔离（卡 1）

- [ ] 2.1 实现锚扫描脚本（复用 pit-panel-dev 双锚脚本升级）：
      shares_outstanding / THS / 实收资本 / valuation（0.2 白名单来源）/
      EPS 隐含，全期间、**hot 与 history 两层**输出候选 suspect 集
      （THS 背离其他 ≥2 锚且 >50%）。
- [ ] 2.2 suspect 集抽样人工复核 ≥20 只（对交易所披露/公开行情核对），
      复核结论记入本文件。
- [ ] 2.3 受控迁移：canonical → `share_capital_suspect`，row_version+1，
      change log 留痕，前后行数对账；迁移可逆性验证。
- [ ] 2.4 扫描复跑归零（suspect 无新检出）。

## 3. 面板交叉验证与备注（卡 2）

- [ ] 3.1 分母装配改为 D1 锚体系规则（shares_outstanding > THS >
      valuation 白名单 > EPS 隐含；实收资本仅校验/末位兜底）：
      consensus / discrepancy / single_source / null 四态 +
      `share_basis` + `share_candidates_json`。
- [ ] 3.2 表结构迁移：`financial_pit_panel` 新增三列（ALTER TABLE，
      幂等）；API 与 coverage 透出 `share_validation` 分布。
- [ ] 3.3 EPS 锚双条件跳过（净利或 basic_eps 近零）；同锚多 fact 取
      updated_at 最新；suspect 行自动降级（D3）。
- [ ] 3.4 单测：共识 / 分歧备注 / 单锚 / 零锚 / 近零净利 /
      实收资本面值分歧 / suspect 隔离后降级，全绿。
- [ ] 3.5 全量重建 + valuation 锚分歧率报告（该序列质量定案，回填文档）。

## 4. 验收（卡 2）

- [ ] 4.1 #12 复跑：bps/ocfps 对比 v5 基线（10.07%/9.74%）；
      未收敛部分逐股归因（永续债/口径清单核对）。
- [ ] 4.2 `share_validation` 分布报告：consensus 占比、discrepancy 占比、
      single_source 占比（预期前者为主，如实记录）。
- [ ] 4.3 回归：17 项面板单测 + 同步/披露回归 + 映射管道测试全绿。
- [ ] 4.4 `openspec validate cross-validate-share-capital-denominators`
      通过；更新需求文档 §6.2 为交叉验证机制（撤过渡机制标注）。
- [ ] 4.5 可选增强决策记录：是否新增外部股本采集（东财 F10 总股本）——
      按 4.1/4.2 结果决定，默认不做。
