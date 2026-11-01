## Context

事故根因与首例修复见 `cross-validate-share-capital-denominators` tasks §9：
面板模块 14 处裸 with-connect 已改 `closing()` 并配 fd 回归测试。全库清查
（2026-11-01 下午）确认同款模式在 8 个生产文件尚有 16 处；赋值型连接中
约 10 处经核实为标准 finally-close 工厂（安全）。

## Goals / Non-Goals

**Goals:**

- 生产代码中不再存在不保证关闭的 SQLite 连接；
- 写入语义不因修复而改变（with 自动提交 → 显式 commit 的等价转换）；
- 新增该模式在 CI 即被拦截，豁免需显式声明理由。

**Non-Goals:**

- 不统一/重构连接工厂体系（现有 contextmanager 工厂已正确）；
- 不处理 tests/、scripts/、一次性开发脚本中的同类模式（守卫扫描范围
  不含它们；若未来某脚本转生产路径，守卫会自然拦截）。

## Decisions

### D1 风险分层与修复次序

| 层 | 站点 | 理由 | 优先级 |
|---|---|---|---|
| A | semantic_runtime ×1、production_operations ×1、structured_sync ×2、database_backup ×4 | 长驻应用进程内路径，随调度/运行时反复执行，事故同款累积条件 | 先修 |
| B | precision_review ×4、available_date_backfill ×1 | 操作员长流程/批量回填，进程生命期长但非服务常驻 | 次之 |
| C | announcement_legacy_cleanup ×2 | 一次性迁移，进程退出即释放 | 一致性收尾 |

### D2 修复模式（对齐事故修复先例）

- 只读连接：`with closing(sqlite3.connect(...)) as conn:`；
- 写入连接：closing + **显式 `conn.commit()`**（逐点核对原 with 块内
  是否依赖自动提交；database_backup 的备份/校验流尤其要逐点确认）；
- 已有安全工厂可达的（如同文件内 get_connection），优先路由工厂而非
  新包 closing，减少同文件两套写法并存。

### D3 防再发守卫（模式扫描测试）

- 扫描范围：`research/`、`scheduler/`、`api/`、`database/`、
  `data_manager.py`、`main.py`、`utils/`、`data_sources/` 下的 `*.py`；
- 拦截模式：`with sqlite3.connect(`（裸用法）；
- allowlist：确有理由的站点登记 `文件:行` + 理由注释，评审可见；
  allowlist 条目数趋势只应减少。
- 不拦截：`closing(sqlite3.connect(`、工厂内赋值型（由人工核实清单
  覆盖，见 tasks 1.1）。

### D4 验证

- 每文件修复配套件套件回归（business_profile 系列、scheduler backup、
  迁移脚本 dry-run）；
- A 层新增 fd 回归测试：对 semantic_runtime/backup 的连接获取路径循环
  调用 N 次，断言 /proc/self/fd 稳定（复用事故修复测试模式）；
- 完成后复跑守卫测试确认零残留。

## Risks / Trade-offs

| 风险 | 处置 |
|---|---|
| 写入点漏补 commit → 静默丢数据 | 逐点判读原语义并列表记录（tasks 2.x 每点含"原是否依赖 with 提交"结论）；修后跑对应模块写路径测试 |
| business_profile 系列测试面大、真库 I/O 慢 | 分文件小步提交，套件按文件跑而非整批 |
| 守卫误伤合法用法 | 只拦裸模式字符串，closing/工厂不匹配；allowlist 兜底 |
| C 层迁移脚本无测试 | dry-run/空跑核对 + 进程退出释放的天然安全边际，低风险收尾 |
