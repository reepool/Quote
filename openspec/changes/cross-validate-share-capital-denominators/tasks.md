## 0. 开发前置验证（D0，2026-10-31 完成，结论已回填 design）

- [x] 0.1 THS 股本时点语义：数据驱动验证（428 只抽样，344 只跨期变化组），
      变化组逐期对 valuation 隐含股本 n=416：median=1.000、p10=p90=1.000
      → **期末股数语义成立**，结论写入 design D0-1。
- [x] 0.2 valuation market_cap 语义分类：全库单一来源
      （local_quotes_financial_facts/valuation_history_builtin，328 万行），
      已知总股本抽验 600519/601398/000858 比值 1.0000–1.0034 → **总市值，
      可进取值优先序**；结论写入 design D0-2。

## 1. 映射层单位机制（卡 1）

- [x] 1.1 `financial_source_field_mapping.py`：v6 拆行——THS equity 行
      `source_unit=shares`（approved，semantic=ths_equity_share_count）；
      sina 实收资产行 `source_unit=CNY`、`approved_for_core=false`
      （rejection_reason=cny_paid_in_capital_requires_par_value_
      cross_validation）；`MAPPING_VERSION` v5→v6，走既有 audit 管道，
      单测验证两行单位声明。
- [x] 1.2 `financial_fact_aliases.py`："总股本"别名移至 `shares_outstanding`；
      `share_capital_amount` 目录条目保持 CNY（该目录承载 sina/cninfo
      金额流）并注明 THS 股数流走 v6 映射。
- [x] 1.3 aliases 加载断言 `_validate_canonical_units`：股数类
      （shares_outstanding）必须 unit=shares、其余不得为 shares，违反即抛
      ValueError；正反双路径已验证。
- [x] 1.4 映射管道回归：test_financial_source_field_mapping 9/9（单位宇宙
      断言扩至 shares，属预期契约更新）+ xbrl parser 21/21。

## 2. 全期间坏行扫描与隔离（卡 1）

- [x] 2.1 锚扫描（2026-10-31）：65,169 组核验（eps 跳过 228 期），
      **suspect 400**（SH 200/SZ 199/BJ 1），模式一致：THS 单锚孤立 vs
      valuation+EPS 双锚互证（如 000035：THS 3.56 亿 vs 双锚 25.24/25.20 亿）。
      history 层：0 行 suspect（隔离迁移亦覆盖，hot=400/history=0）。
- [x] 2.2 抽样复核：外部公开数据精确核对 2 只——中国天楹 000035 现总股本
      23.88 亿股与 val 锚 2,388,032,769 **一股不差**（THS 3.56 亿为 Urbaser
      重组前陈旧值）；航天发展 000547 15.98 亿股与 val 锚 1,598,468,269
      **一股不差**；其余 398 只由双锚独立一致机制覆盖。复核结论：THS 侧
      陈旧/坏值，隔离正确。
- [x] 2.3 受控迁移：400 行 canonical → `share_capital_suspect`，
      row_version+1，change log 400 条（old_hash=new_hash，元数据重命名
      不推面板 available_at）；残留 0；suspect 行可逆（canonical 改回即恢复）。
- [x] 2.4 扫描复跑归零：**suspect=0**（ok=65,169 与首轮一致，无误伤）。

## 3. 面板交叉验证与备注（卡 2）

- [x] 3.1 分母装配 D1 锚体系（shares_outstanding > THS > valuation >
      EPS 隐含；实收资本仅校验/末位兜底带 par 标注）：四态 + basis +
      candidates_json 已实现于 `_assemble_share_denominator`（分层规则：
      unit_mismatch 不翻 consensus；股数锚<2 时实质校验分歧计入；
      与出带锚同向则强化）。
- [x] 3.2 表结构：三列经 `initialize()` 幂等 ALTER 迁移；API items 解码
      `share_candidates`；coverage 逐期输出 `share_validation` 分布。
- [x] 3.3 EPS 锚双条件跳过（|净利|≥1 元且 |eps|≥0.005 才参与）；
      suspect 行因 canonical 改名自动退出锚装配；同锚多 fact 取
      updated_at 最新。
- [x] 3.4 单测 19/19：共识（含 corroborated）/ 分歧备注 / 单锚 / 零锚 /
      近零跳过 / unit_mismatch 不翻状态 / 隔离降级 single_source /
      非未隔离行 discrepancy 标注不静默修 / refresh 同规则。
- [x] 3.5 全量重建（66,124 行 / 5,596 只 / 913.7s，含隔夜同步新增）+
      锚质量分布：consensus 61,055（92.4%）/ discrepancy 4,497（6.8%）/
      single_source 528 / null 44；discrepancy basis=ths 4,463（隔离前
      存量快照标注）、valuation 28、eps 6。valuation 锚分歧率定案：
      其与 THS/一致性集群的分歧极低（两锚互证模式占主导），此前
      "系统性偏低 18%"系测量锚伪影的结论获得进一步支持。

## 4. 验收（卡 2）

- [x] 4.1 #12 复跑（重建后）：eps 三期 PASS（0.26–0.58%）；ocfps
      2024Q1 0.87% / 2024Q2 0.62% **PASS**（v5 基线 1.41%/1.21%）；bps
      10.2%/9.83% 与 v5 基线（10.07%/9.74%）持平——残余为已定性口径差
      （AK 每股净资产分母与永续债处理），**标注机制已就位**，quantwin 可
      按 discrepancy 过滤；逐股归因需 quantwin 联调窗口。
- [x] 4.2 分布报告（§3.5）：consensus 92.4% 为主，与预期一致。
- [x] 4.3 回归：面板单测 19/19；映射 9/9 + xbrl 21/21；同步 61/61、
      披露 58/58（含钩子）。**环境阻塞项**：test_storage.py 在隔夜调度
      持锁窗口下 flock 等待挂起（零 I/O 50 分钟，两次），与本次改动无关
      （storage 仅新增只读查询方法），需安静窗口重跑：
      `pytest tests/unit/test_research/test_storage.py -q`。
- [x] 4.4 `openspec validate` 通过；需求文档 §6.2 已更新为交叉验证机制
      （过渡机制标注移除于本 change 文档族）。
- [x] 4.5 可选增强决策：暂不新增外部股本采集——四锚已自洽且 ocfps 达标；
      待 quantwin 联调若 bps 口径差仍阻碍拼接再议。
