# 6.FL B 实施交付（2026-11-25）

已冻结，交A独立验收和隔离Git，B未commit/push。真实模型/供应商联网/付费新增均0；当前20研究根、49既有dirty、配置、scheduler/core及七管理文档/卡均未由B修改。

交付写集：

- `research/company_profile/http_tasks.py`：原TaskPlan有限策略及凭据外的领域适配，请求/计划/安全摘要持久化、幂等冲突、immutable双root pin、任务数/累计上限、跨worker范围锁、显式原task_entry投递、进度和原reader结果；独立线程运行原异步owner，关闭走原stop并join真实线程，取消关闭包装器也不提前释放锁/关闭storage，重启不重发。
- `api/company_profile_routes.py`：三个显式任务端点；普通读取/计划保持原样。任务策略必须与原reader的显式root或DEFAULT_OUTPUT_ROOT一致，错误不切换原读数据集。
- `api/app.py`：安装与关闭薄接线，先join研究任务再关闭storage。
- `tests/unit/test_research/test_company_profile_http_tasks.py`：原owner纵向与直接必要边界。
- `docs/research/company_profile_http_tasks.md`：策略/凭据配置、提交/查询、有限额度、状态/质量区别、关闭和研究使用边界。

请求 `company_profile_http_task_request.v1`、策略 `company_profile_http_task_policy.v1`、返回 `company_profile_http_task.v1`。POST `/api/v1/research/company/annual-profile-tasks` 返回202；GET `/{task_id}`、`/{task_id}/result`仅查。review/enrich由原reader生成report/work/hash计划并执行前核stale；不接受客户端路径、模型、报告版本或预算。

验证结果：

1. 新文件前四直接用例最终 **4 passed / 77.32秒**。命令：`source scripts/quote_dev_env.sh; timeout 240 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_http_tasks.py -q --basetemp=<本人QuoteBak>/pytest-final`。在获准本地IPC环境执行，零供应商联网。
2. 首读→独立review→成功enrich实际经过原`execute_configured_task → execute_published_task → TaskService → queue/runtime/writer/read`。ScriptedTransport分别2/1/3次受控发送，得到非空待核两事实、审后两reading及补读后3reading和新增利率影响描述；重复/冲突及GET/plan无新增发送、fresh reader一致。仅测试将identity显式设controlled_fixture及provider.offline_only，不mock业务owner、不冒充真实年报或新质量成绩。
3. 凭据/默认禁用/范围/未知客户端参数/有限额度、root错位拒任务而保原读、两实例同范围串行（第二次执行确实在第一次结束后进入）、关闭及关闭被取消仍join、中断重启重复ID不重发、改policy_id/配对root不能刷新额度均在四直接用例内通过。锁边界用可控执行函数，只第一条纵向用原owner。
4. 末段发现并修正“后续任务新增稿误算本任务新增”及旧已存在稿归属；API lifecycle清理保持storage最后关闭。仅新增两个匹配直接用例复验，**2 passed / 9.04秒**，`-k 'terminal_progress or api_lifespan'`。前四主体无语义业务改动，A接管当前6用例及必要既有合并回归，不由B重复整组。
5. `ruff check --select E9,F63,F7,F82`四源码/测试文件通过，`git diff --check`两个既有API文件通过。普通全量ruff含原API风格/未用import及项目现有FastAPI Depends/BLE规则，未将无关风格修正扩大为本轮工作。

早期验证未计成功：受限环境首次pytest中断code130；随后real identity缺PDF被原源要求拒绝，未到transport；尝试PDF测试依赖fitz未安装，未安装依赖；改显式controlled fixture时一次monkeypatch需raising=False；首次零旧画像的produced_readings漏计已局部修正。均为工程/fixture定位，不是新增真实模型失败或公司质量成绩。

限制：这是HTTP工程闭环，不启动服务/生产、不装实际策略或执行新付费批次。completed表示原执行结束，review_status/缺项/独立限制另列；available_profiles不能充completed_items。当前结果复用原reader，是当前保存画像，不是第二事实库。优雅关闭需等待在途阶段原有限预算内结束；同范围HTTP有跨worker互斥，CLI操作者仍须避免另起并行写。历史质量/规模/生产边界未变。

清理：开工仅见A当前精确根，未删未知或并行内容。结束核本轮命令均已退出、无本人活跃测试；删除本人`data/QuoteBak/company-profile-6fl-b-DzcRyUGU`内193文件/7,085,498逻辑字节（测试DB/计划/重复受控回执等可重建物），另删四个本轮显式py_compile产物96,814字节；不删共享缓存/源资产/正式根/A快照。必要验证说明仅保本文件，由A合并权威HTTP及回归证据。

四个显式编译缓存为api app 8,678字节、api company_profile_routes 13,296字节、http_tasks 42,967字节、新test 31,873字节，合计96,814字节；总清197文件/7,182,312逻辑字节。实际删除完成后本段为清理回执，不保测试库备份。
