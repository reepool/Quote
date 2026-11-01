## Why

2026-11-01 12:50 生产事故（主应用 fd 耗尽 1024 上限，3 个日常任务同刻失败，
quotes.db-wal 膨胀 12GB）的根因是 `with sqlite3.connect(...)` 裸写法——
`with` 作用于连接对象只提交事务、**从不关闭连接**，在长驻进程中累积泄漏
（详见 `cross-validate-share-capital-denominators` tasks §9 事故记录）。
面板模块 14 处已修（`contextlib.closing` + fd 回归测试）。

事故后全库清查：**同款裸写法在 8 个生产文件中仍有 15 处**，另有约 10 处
赋值型连接经核实为项目标准 `@contextmanager` + finally-close 工厂
（financial_store / quote_store / corporate_action_projection / storage /
fx / special_commodity 等，安全，不动）。

## What Changes

- 16 处裸 with-connect 按风险分层修复：
  - **A 层（长驻应用内路径，8 处）**：`business_profile_semantic_runtime`
    （运行时热路径）、`business_profile_production_operations`、
    `business_profile_structured_sync` ×2、`scheduler/database_backup` ×4
    （每日调度任务，每次泄漏至多 4 个句柄）；
  - **B 层（操作员/评审长流程，5 处）**：`business_profile_precision_review`
    ×4、`financial_available_date_backfill`；
  - **C 层（一次性迁移脚本，2 处）**：`migrations/announcement_legacy_cleanup`
    ×2（进程退出即释放，风险最低，为一致性一并修）。
- 修复模式统一对齐事故修复先例：`contextlib.closing(...)` 包裹；**依赖
  with 自动提交的写入点补显式 `commit()`**（事故修复中 refresh_pairs 的
  同款注意项）；已有 repository/工厂可路由的优先路由。
- **防再发守卫**：新增模式扫描测试——扫描生产代码目录
  （research/ scheduler/ api/ database/ data_manager.py 等）中的
  `with sqlite3.connect(`，出现即失败；确需豁免的站点写入显式 allowlist
  并附理由注释。
- A 层路径补 fd 回归测试（复用事故修复的 /proc/self/fd 采样模式）。

## Capabilities

### New Capabilities

- `sqlite-connection-hygiene`: SQLite 连接确定性关闭的代码级约束与防再发
  守卫。

## Impact

- Affected code: 上述 8 个文件的 16 个连接点（逐点小改，不改业务逻辑）；
  新增 1 个守卫测试 + A 层 fd 测试。
- Affected storage: 无 schema/数据变更；仅连接生命周期。
- 风险控制：写入点补 commit 需逐点核对（漏补=数据丢失风险，最高优先
  验证项）；每文件修配套件套件回归（business_profile 相关 + scheduler
  backup 测试）。
- Non-goals: 不重构连接工厂体系（现有 contextmanager 工厂已安全）；
  不动 tests/scripts/ 下同类模式（进程短命，由守卫 allowlist 机制声明）。
