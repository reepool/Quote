# Quote 开发入口

本文件只提供当前开发入口和权威文档导航，不记录版本宣传、历史规模或已经完成的阶段性需求。

## 1. 开发前必读

按以下顺序读取：

1. `AGENTS.md`：仓库级强制规则；
2. `docs/development/project_development_governance.md`：架构、复用、单一执行链和生产稳定性总纲；
3. 当前功能对应的 OpenSpec spec/change；
4. 当前域的 current 文档或 runbook；
5. 涉及框架改造时读取 `docs/development/framework_refactoring_program.md`。

历史 requirements、调查和回执只能作为背景，不能覆盖当前代码、OpenSpec spec 和开发总纲。

## 2. 当前架构原则

Quote 是本地优先的模块化单体，使用多个按数据域隔离的 SQLite 数据库。

```text
CLI / API / Scheduler / Telegram / Operator Script
                         |
                         v
             Application Command / Query
                         |
               Domain / Read Service
                         |
              Repository / Provider Port
```

允许多个外部入口，但同一业务动作只能有一个权威实现和一个写入 owner。

以下文件处于受控收敛阶段，不得继续新增大段业务逻辑：

- `data_manager.py`；
- `research/storage.py`；
- `scheduler/tasks.py`；
- `api/routes.py`。

框架改造工作流、依赖和完成状态见 `framework_refactoring_program.md`。

## 3. 主要目录

| 目录 | 当前职责 |
|---|---|
| `api/` | FastAPI 应用、路由、模型和中间件 |
| `data_sources/` | 行情、主数据、公司行为等上游 provider |
| `database/` | `quotes.db` 的异步 SQLAlchemy 模型和操作 |
| `research/` | 财务、估值、行业、股东、公告、期货、外汇、商品和研究服务 |
| `scheduler/` | APScheduler、job 配置适配、依赖和任务报告 |
| `scripts/` | 明确的 operator、回填、迁移和开发验证入口 |
| `config/` | 分文件生产配置 |
| `tests/` | 单元、契约和受控集成测试 |
| `openspec/` | 当前规范、change 和归档 |
| `docs/` | current 文档、runbook 和开发治理 |

生产模块不得 import `scripts/`、`tests/` 或 OpenSpec evidence。

## 4. Python 环境

项目当前使用 Python 3.10+。本机 Quote 环境通常位于：

```bash
/home/python/miniconda3/envs/Quote/bin/python
```

运行命令时优先显式使用该解释器，不永久修改用户 shell 配置。

OpenSpec CLI 位于：

```bash
/home/python/.nvm/versions/node/v24.7.0/bin/openspec
```

## 5. 开发流程

### 5.1 小型修复

1. 记录 `git status --short --branch` 基线；
2. 找到当前业务入口和最小调用链；
3. 修复错误并运行相关测试；
4. 检查没有新增第二条执行路径；
5. 更新 current 文档；
6. Review、提交并推送本人改动。

### 5.2 复杂功能或框架改造

1. 使用 OpenSpec proposal 明确业务验收和非目标；
2. 绑定现有 spec 或框架纲要 requirement/workstream；
3. 声明生产不变量、兼容和回滚条件；
4. 按纵向切片实施，不先建设通用平台；
5. 每个切片形成可运行的输入到输出路径；
6. 删除被替代的旧实现或登记限期兼容；
7. 完成后归档 change 并更新文档索引。

框架改造不得在同一 change 中同时重排多个无关业务域。

## 6. 测试与验证

优先运行与改动直接相关的测试：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest <target>
```

验证顺序：

1. 当前业务验收；
2. 核心金融和时间语义；
3. 当前 bug 回归；
4. 现实兼容边界；
5. 受影响的共享接口。

普通单元测试不得依赖实时网络。重构写入路径使用 fixture 或临时数据库，不在生产数据库上试验。

## 7. 文档规则

`docs/README.md` 是唯一总索引。新增或修改文档必须标明其类型：

- current：当前架构、接口、配置；
- runbook：当前可执行运维步骤；
- requirements：尚未完成或持续有效的需求；
- historical：已经完成的调查、回执或迁移记录。

同一功能只保留一份 current 文档和必要 runbook。历史内容在有效规则进入 current 文档或 OpenSpec spec 后删除，不建立长期 legacy 文档目录。

## 8. 关键文档

- `project_development_governance.md`：长期开发总纲；
- `framework_refactoring_program.md`：框架改造需求和进度；
- `open_issues_backlog.md`：当前数据能力问题；
- `research_data_engine_execution.md`：研究引擎实施主线；
- `../architecture.md`：按当前代码和配置维护的系统架构概览；
- `../README.md`：完整文档索引。
