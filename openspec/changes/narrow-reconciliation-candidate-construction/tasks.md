## 1. M1 基线与等价性

- [ ] 1.1 扫描段调用分解基线：用既有 `--decompose` 工具形态测每候选的官方源合并调用次数/取回行数、profile 解析次数与行业/画像 DB 读次数；产出基线报告（docs/development）钉死 144ms 的构成。
- [ ] 1.2 冻结样本扩充：在上一 change 的 220 标的样本上增加"仅官方源有数、本地无数"标的与谓词三形态（source 命中/仅 raw_fact 命中/不命中）样本（生产存在则纳入，否则 fixture 构造）。
- [ ] 1.3 双跑等价扩展：`_merge_official_cninfo_readiness` 输出（facts/missing_fields/ready 全字段）宽窄对比工具与黄金基线存档；同时比对"批量 profile 解析 vs 逐候选解析"在同一标的集上的结果一致性。

## 2. M2 合并读取窄化（N1）

- [ ] 2.1 扩展窄查询设施：列集参数化 + 官方源谓词 SQL 下推（LIKE 语义对齐 haystack；不等价则 SQL 预筛 + Python 终判双层）；不出现第三套事实查询实现。
- [ ] 2.2 `_official_cninfo_facts_by_canonical` 切换到窄路径；宽方法零改动。
- [ ] 2.3 谓词边界单测（三形态）+ 合并等价门禁 0 不一致后合入。

## 3. M3 profile 按标的去重（N2）

- [ ] 3.1 扫描循环按 instrument 共享解析结果；优先接入既有 `resolve_financial_statement_profiles_for_instruments` 批量设施并删除逐候选重复实现。
- [ ] 3.2 解析一致性门禁（1.3 的比对）通过后合入。

## 4. M4 验证与收口

- [ ] 4.1 夜间窗口全量 dry-run：扫描段 ≤60 分钟（scan_rate ≥17/秒）；不达标启动 N3（按标的批量就绪）并复测。
- [ ] 4.2 生产验收：连续两个周日自然调度总时长 ≤2.5 小时。
- [ ] 4.3 达标后 `max_runtime_seconds` 43200 → 18000 回调并记录两个周日的实测数据。

## 5. 清理与交接

- [ ] 5.1 基线/等价报告归档 docs/development；openspec validate 通过；生产不变量自查（宽方法零改动、判定语义等价、写路径零变化）后交 A 审核。
