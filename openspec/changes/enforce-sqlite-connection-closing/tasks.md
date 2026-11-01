## 1. 清单定案（排查收尾）

- [ ] 1.1 赋值型连接核实清单收尾：announcement_assets/repository.py:801、
      company_profile/shadow_batch.py:553、fx/special_commodity 的
      get_connection 尾部（确认 finally close），结论记入本文件。
- [ ] 1.2 16 处裸 with-connect 逐点判读"原块内是否依赖 with 自动提交"
      （读=无需 commit；写=需补），输出逐点结论表（文件:行 / 读写 /
      修复方式），作为修复卡的对照基线。

## 2. A 层修复（长驻应用路径，8 处）

- [ ] 2.1 business_profile_semantic_runtime.py:611（热路径）：closing
      包裹（或路由同文件安全工厂）；对应套件回归。
- [ ] 2.2 business_profile_production_operations.py:1139：同上。
- [ ] 2.3 business_profile_structured_sync.py:664/672：同上；注意写入点
      commit。
- [ ] 2.4 scheduler/database_backup.py:501/505/549/584：逐点判读备份源/
      目标连接的提交语义后修复；backup 相关测试回归。
- [ ] 2.5 A 层 fd 回归测试（semantic_runtime + backup 的连接路径循环
      N 次断言 fd 稳定）。

## 3. B/C 层修复（8 处）

- [ ] 3.1 business_profile_precision_review.py:790/906/963/1011。
- [ ] 3.2 financial_available_date_backfill.py:67（写路径，补 commit）。
- [ ] 3.3 migrations/announcement_legacy_cleanup.py:32/331（一次性脚本，
      一致性修复；dry-run 核对）。

## 4. 守卫与验收

- [ ] 4.1 模式扫描守卫测试：生产目录出现裸 `with sqlite3.connect(`
      即失败；allowlist 机制（文件:行 + 理由）就位且初始为空。
- [ ] 4.2 全量复跑守卫 → 零残留；business_profile / scheduler 相关
      套件全绿。
- [ ] 4.3 `openspec validate enforce-sqlite-connection-closing` 通过；
      修复点数与 1.2 基线表逐一对账（15/15）。
- [ ] 4.4 提交推送；change 待验收归档。


## 存量业务影响终审（2026-11-01，基线 d82fc744 全量 diff）

按影响半径分四层，结论：**无破坏性影响；1 处低危元数据失真；2 处性能注记。**

- **纯增量（零既有影响）**：routes +129 行零删除（5 新端点）；data_manager
  +22 零删除（1 新方法）；storage +113 仅 import 行改动（1 新查询）；
  面板模块/测试/文档均为新文件。
- **机械转换（语义零变化）**：backup API 自含事务无 commit 变化；
  test_database_backup **10/10**（关键路径闭环）；business_profile 系
  50 测试零失败（余部环境锁阻塞，命令在档）。
- **既有热路径插入（钩子）**：财报/披露同步为 best-effort（同步不可能
  因面板失败）；事故后日志 0 次 refresh 失败；隔夜同步实证 Q3 面板
  4,938→5,561 行增量正常落库。性能注记：批量同步下每 fact 一次
  refresh（数查询），隔夜完成无异常，增速未量化——若批量窗口变慢，
  后续可加"批量末聚合刷新"优化（可选，不立项）。
- **全局配置 v6**：THS 流 canonical 不变；别名变更存量零命中；cninfo
  实收资本照旧解析（测试过）。**已知低危失真**：sina_report fallback
  流的实收资本 fact lineage 元数据 unit 会盖成 shares（值实为 CNY）——
  该 fallback 非主要生产流（实测主要来源为 akshare direct 与 cninfo
  structured，走 catalog 解析不受影响），canonical 名不变、面板按
  fact_name 分类不受影响、存量不回写；随"ingestion 盖章"后续卡一并
  消除。
- import 期断言 ×2：当前合法；再发风险 = 编辑中间态被懒加载（09:30
  事件模式），整改为原子编辑纪律已记。