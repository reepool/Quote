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
- [x] 4.4 `openspec validate` 通过；需求文档已同步（交付后审计补齐）：
      §6.2 改为交叉验证机制全文、§4.1 补 share_validation/share_basis/
      share_candidates_json 三列、§5.3 补 share_validation 分布字段。
- [x] 4.5 可选增强决策：暂不新增外部股本采集——四锚已自洽且 ocfps 达标；
      待 quantwin 联调若 bps 口径差仍阻碍拼接再议。


## 5. 交付后对照审计（2026-11-01，spec/需求逐场景核对）

发现 6 项，全部修复（测试 50/50）：

1. **v6 修正未覆盖克隆 profile（断言抓到的真缺陷）**：profile 克隆机制把
   旧 approved CNY 股本行复制进 bank/securities/insurance——此前只验证了
   nonbank。新增 `_validate_mapping_unit_classes` 导入期断言（hygiene spec
   "mapping 侧拒绝违规单位"场景的落地实现）当场拦截；v6 修正行现克隆至
   全部四个 profile，逐一验证通过。
2. **refresh 一致性断言缺失**：tasks 3.4 声称已测但旧测试只断言值传播；
   已补 share_validation/share_basis 断言（spec 场景 "Incremental refresh
   applies the same anchor assembly"）。
3. **微利护栏未实现**：design 风险表的"净利<营收 0.5% 跳过 EPS 锚"落空，
   `revenue` 参数为死参；已实现营收比例护栏 + 单测（微利时 eps_implied
   记 skipped）。
4. **pic 非正值未排除**：spec 场景要求零/负值锚视同 absent，实收资本
   校验锚此前仅排除 0；已改 >0（两处 loader）。
5. **需求文档漂移**：§6.2/§4.1/§5.3 三处未随实现更新（tasks 4.4 曾因此
   表述失实）；已同步，本条目即修正后的真实状态。
6. **审计脚本只存于 QuoteBak（gitignore）**：违反证据留存要求；扫描与
   隔离脚本已固化至 `scripts/share_capital_audit/`（带 change 引用头）。

**EPS 分歧幅度实测（带宽调优决策项，未擅自改设计）**：4,463 个 ths 基
discrepancy 中 88.5% 为 eps_implied 单锚分歧，幅度中位 1.21（p10 0.73 /
p90 1.47），85% 落在 0.7–1.5 加权漂移带——15% 带宽会把典型加权/期末漂移
标为分歧。属"诚实标注过宽"而非错误；是否放宽 EPS 带宽（如 25–30%）或
将 EPS 锚降为 advisory（不参与状态判定）留给 quantwin 联调决策。

审计后验证：单测 50/50（面板 20 + 映射 9 + xbrl 21）；四 profile 映射
逐一验证；护栏重建（v7，66,124 行 / 1,152s）完成——分布与 v6 完全一致
（consensus 61,055 / discrepancy 4,497 / single_source 528 / null 44），
微利护栏激活（26 行 discrepancy 带 eps skipped 标记）未改变标注格局。

## 6. 第二轮交付后审计（2026-11-01，"修正后再审"）

**审计首先抓到上轮修复的残留**：fix ③（营收护栏）**从未实际生效**——当时
装配脚本的 pic 部分断言失败导致整个脚本回滚，营收护栏随之丢失，而新测试
恰好被绝对阈值（|NI|=0.5<1.0 元）满足而掩盖。验证不靠记忆的原则再次兑现。

本轮修复与变更（全部落地，51/51）：

1. **营收护栏真实施装**：|净利| < 0.5%×|营收| 时跳过 EPS 锚；测试改为
   NI=30/营收=10000（过绝对阈值、不过比例阈值），无护栏必失败；
2. **EPS 分层建议制**（用户授权修复上轮量化发现）：加权漂移中位 21%、
   88.5% discrepancy 为 EPS 单锚分歧——单独出带记
   `eps_weighted_drift_advisory` 不翻状态（与实收资本分层规则同构）；
   仅当与出带期末锚互证或期末锚 <2 时升级。spec 新增场景、design D1
   与需求 §6.2 同步；
3. **实施期自伤一次**：首版共识门槛误用 period_end_count≥2，把
   "THS+EPS 双锚一致"（估值覆盖前期次）从 consensus 错降 single_source
   （528→6,305）——v8 重建分布检查当场暴露，v9 修正为"无分歧且锚≥2 即
   共识"。重建-验证闭环的价值实证；
4. 测试层两处小错（note 嵌套层级、浮点容差）修复；一个无效占位测试删除。

**v9 终态分布（66,124 行）**：consensus 64,326（97.3%）/ discrepancy
**1,128（1.7%**，仅期末锚真实分歧）/ single_source 626 / null 44；
advisory 注记 3,024 条。对照 v7（discrepancy 4,497/6.8%）：标注信噪比
显著改善，discrepancy 恢复为可执行信号。

**test_storage.py**：两次安静窗口重试均冻结于 flock（隔夜与日间调度都持
coordinated 锁；rchar 停滞 400KB 级）。与本 change 无关（storage 仅新增
只读方法），重试命令不变，留待调度空闲窗口。

## 7. 第三轮审计（2026-11-01，需求覆盖 + 架构/治理维度）

**架构修复（纯重构，51/51 全绿，行为不变无需重建）**：
1. **分类逻辑重复 ×2**：build 与 refresh 两个装载器各自内联同一套
   fact_name→锚分类规则（pic 正值修复曾被迫改两处即其症状）——统一为
   `_classify_share_anchor_row` 单一权威，双路径委托；
2. **no-op stub 被调用**：`_apply_delay_estimate` 早已被 derive_availability
   取代却留作空壳，且两处调用点仍在调用（比死代码更误导）——删除；
3. **死方法 ×2**：`_resolve_denominator_shares`（被交叉验证装配取代的旧
   灾难替换器）、`_load_lifecycle_universe`（被 sets 版取代）——删除；
   模块 1,810 → 1,774 行。

**硬编码/特殊处理清单（逐项判定，记录不修）**：
- `fact_name` 字符串分类（'equity'/'实收资本'）：理想方案是按 v6 映射在
  ingestion 给 fact 盖 `canonical_unit`、面板按单位分类；实测存量行
  canonical_unit 全部为 'CNY'（v6 语义之前入库），单位驱动对存量不可行——
  fact_name 是当前唯一可靠判别，且已收敛到单一函数。后续卡：新入库按
  v6 盖单位 + 存量重盖后切换单位驱动；
- `9999-12-31` 哨兵（malformed 期间隐藏，实际无此类行）、交易日历 SSE
  兜底（A 股域内合理）、`_BULK_FIRST_PERIOD=2023-12-31`（需求文档规定
  的数据边界，具名常量）——均判定合理保留。

**治理合规（§10.2 对照）**：单写入 owner（panel 模块 own 表/构建/查询）✓；
API 入口只转发（`_pit_panel_store` 对齐 `_backtest_financial_store` 先例，
industries 走 data_manager 应用服务链）✓；data_manager/storage 未新增
大段领域逻辑（storage 仅一个读查询，符合 repository 角色）✓；增量挂钩
经既有同步链路而非平行循环 ✓；无兼容门面 ✓。

**需求覆盖（v1.2 全文对照）**：§3–§7 功能项全部实现且为三轮修复后终态
语义；§8 验收 1–11/13–17 完成，#12 部分（ocfps PASS、bps 口径差已标注，
逐股归因待 quantwin 联调）；§9 非目标零越界。需求文档与两个 change 的
文档族三处一致。

## 8. 硬编码判定复查（2026-11-01，用户质询后对抗性复查）

原判"fact_name 分类合理保留"**淡化了一个真实弱点**：其失败模式是**静默的**——
新来源用未见过的字段名时锚悄然缺席（体现为 single_source/null 上升，但无
任何信号指向原因）。已修正：`_classify_share_anchor_row` 对 share_capital_
amount 下未识别的 fact_name 记 once-per-name WARNING（来源漂移可观测），
配单测（52/52）。分类逻辑本身维持 fact_name 驱动（存量行 canonical_unit
均为 v6 前旧章 CNY，单位驱动对历史不可用），v6 盖章+存量重盖的后续卡不变。
其余三项判定经复查维持：9999-12-31 哨兵（实际不可达的防御分支，仅 API
展示层面的 cosmetic）、SSE 日历兜底（A 股域内未知交易所的合理缺省）、
2023-12-31 边界常量（需求文档规定的历史边界，永不移动，具名常量比数据
推导更诚实）。

## 9. 生产事故记录与修复（2026-11-01 12:50，句柄泄漏）

**事故**：主应用（PID 334306）12:50:54 文件句柄达 1024 上限（sys.log:555246
Errno 24），API 拒连、SQLite 无法打开库文件，3 个日常任务同刻失败；事故时
进程挂 579 fd（454 指向 quotes.db）；quotes.db-wal 膨胀至 12.04GB（常驻读
连接阻断 WAL 回收）。外部诊断报告逐条核实**属实**。

**根因（本 change 引入）**：`financial_pit_panel.py` 全部 14 处连接使用
`with sqlite3.connect(...)` / `with self.connect()` 裸写法——`with` 作用于
连接对象只管事务提交**从不关闭连接**（项目 storage.get_connection 为
try/finally close 的生成器，本模块偏离了项目惯例）。每次披露事件刷新/
查询各自新开连接且在应用常驻上下文中滞留，累积耗尽句柄。

**为什么三轮审核都没抓到**：三轮维度分别是 spec 合同、修复核实+量化、
架构治理——都不含**进程生命周期/资源管理**维度；冒烟验证全部短进程
（TestClient/脚本），未在常驻进程中反复调用。教训已记：长驻服务的模块，
连接生命周期必须有回归测试。

**修复（23/23 全绿）**：
1. 全部 14 处改为 `contextlib.closing(...)` 包裹（3 处 ro 连接 + 11 处
   self.connect）；refresh_pairs 因失去 with 自动提交补显式 commit；
2. 新增 fd 回归测试：25 轮 cross-section/panel/coverage/changes + 锚
   装载，断言 /proc/self/fd 计数稳定（±2）；
3. storage.get_connection 复核为 finally-close（我加的 industries 查询
   走该路径，无同类问题）。

**恢复步骤（需运维执行，代码修复不释放已泄漏句柄）**：
1. 重启主应用进程（释放 579 fd）；
2. 重启后无读者时回收 WAL：`sqlite3 data/quotes.db "PRAGMA
   wal_checkpoint(TRUNCATE);"`（预计收回 ~12GB）。

**半成品代码风险（同日 09:30:36 事件，属实）**：运行中应用懒加载读到了
脚本化多步编辑的中间态文件（映射断言已入、克隆修复未入），导致
DatabaseOperations 初始化报错。整改：对运行中应用的工作区，文件级编辑
必须保持原子（一次成文），跨文件依赖的变更先写消费者再写校验方。

## 10. 泄漏修复后的复审（2026-11-01，新出现问题专项）

1. **closing 转换自身核查（无新问题）**：全模块无裸 with-connect 残留；
   refresh_pairs 显式 commit 位置正确（块内、循环后、return 前）；早退在
   块外；hook 的 initialize 仅首建 store 时执行（每事件仅 DDL 一次的担心
   不成立）。
2. **模块级无界集合**（`_UNCLASSIFIED_SHARE_FACT_NAMES`，违反"避免全局
   可变状态"且无上界）→ 加 128 名上限。
3. **同步回归抓到一个真新缺陷**：此前别名目录编辑把 `TOTAL_SHARE`（总股本
   语义）从 share_capital_amount 别名中**删除而非迁移**（"总股本"移了、
   TOTAL_SHARE 漏移），用该字段的载荷解析 canonical 变 None——同步套件
   的既有断言当场暴露。修复：TOTAL_SHARE 移入 `shares_outstanding`（股数
   canonical，与语义一致），测试期望更新至 v6 契约
   （canonical=shares_outstanding / unit=shares）。
4. **恢复闭环确认**：quotes.db-wal 现为 0 字节（用户执行 TRUNCATE 返回
   0|0|0 正常——重启时干净关闭已先行清空 WAL）；主应用新进程在跑、泄漏
   句柄已随旧进程释放。

验证：面板 23/23、provider 14/14、同步三件套 61/61。

## 11. 遗漏终审（2026-11-01 下午，生产安全余量专项）

1. **生产关键项（需运维动作）**：现运行应用进程 13:17:25 启动，先于
   泄漏修复写入工作区（13:19:22）2 分钟——懒加载时机不可判（重启后
   panel 模块零活动、无触发痕迹）。**建议今晚披露同步高峰前再重启一次
   应用**以消除内存版本不确定性。实测：重启后窗口零 Errno 24、
   quotes 句柄采样稳定（122→125 总抖动、quotes 恒 5）。
2. **全库同型模式清查（report-only，非本 change 引入）**：8 处裸
   `with sqlite3.connect(` 存在于 business_profile_production_
   operations / business_profile_structured_sync / scheduler/
   database_backup / migrations 等既有模块——同款潜在泄漏模式，
   按审查范围规则记录不代改，建议后续独立卡排查。
3. **lint 分诊**：ruff 204 项中 199 项为 pyupgrade 现代化（与项目
   自身 legacy typing 风格及 3.8+ 兼容声明相悖，不采纳）；真问题
   2 处已修（测试辅助冗余、import 排序）。项目无 ruff 配置，默认
   规则不构成项目标准。
4. 我方改动 git 干净；test_storage 仍为环境阻塞项（记录不变）。