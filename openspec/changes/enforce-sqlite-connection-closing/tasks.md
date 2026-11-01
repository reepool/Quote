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
