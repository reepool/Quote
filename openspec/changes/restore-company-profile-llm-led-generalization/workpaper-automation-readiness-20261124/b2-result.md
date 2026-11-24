# 6.FG B2：HTTP关闭超时是当前受限执行环境的本地唤醒问题

结论：无需应用源码修复。当前沙箱连本地 `socketpair.send()` 都以 `EPERM` 拒绝；不加载业务模块、不安装离线阻断器的 `asyncio.to_thread(lambda: 42)` 也会在worker返回后等不到唤醒。获准工具环境允许本地IPC、继续禁止外网后，线程池及事件循环正常关闭。此为非阻塞执行环境/验证脚本事项，可后置；不占自动化人工依赖主线。

本轮 `payload_scope=local_only`，0新模型、供应商消耗、外网载荷或业务写入，0应用源码/共享配置改动，不stage/commit/push。仅用原router一笔海直单股，不重跑5笔、20股源审、全系统或长CLI。基线 `e6ae151b`，沿当前整卡及business-first-goal只定位本卡生命周期问题。

| 实际检查 | Python实际退出码 | 证据 |
| --- | --- | --- |
| 受限沙箱：stdlib to_thread，带离线guard | 124（6秒外限） | worker 0.003秒返回，await未醒，主线程selectors.select |
| 受限沙箱：裸socketpair本地1字节send | 1 | pair创建成功，send报PermissionError/EPERM，无地址或connect |
| 受限沙箱：stdlib to_thread，无guard | 124（6秒外限） | worker 0.005秒返回，仍未醒；排除本轮guard是必要成因 |
| 获准本地IPC：stdlib to_thread + 显式shutdown + asyncio.run | 0 | 0.008401秒返回，只余MainThread |
| 原router首次：过严文件guard禁止全部写入 | 1 | 导入日志handler即被本轮guard拒绝，未得响应；仅调整本人日志位置/guard |
| 获准本地IPC：原router GET海直 + client/线程池/事件循环关闭 | 0 | 200、完整profile等当前快照，5.797308秒返回，无其他任务/工作线程 |

全部退出码是被测Python/timeout的实际 `$?`，并非最后打印命令的shell退出码。逐case字段、失败摘要及原日志hash见 `b2-checks.json`。

机制：Python 3.11的 `asyncio.selector_events._write_to_self()` 调 `socket.send(b'\0')` 唤醒selector，捕获OSError后通常不报到调用方。worker结果回调和 `base_events._do_shutdown()` 的完成通知都经过 `call_soon_threadsafe` / 同一self-pipe，因此即使worker已经完成，也可能停在selector或 `runners.close`。本轮没有发现业务代码遗留任务；不改 `reads.py` / router，也不以延长上限、轮询定时器或绕开shutdown掩盖现象。

离线guard允许AF_UNIX事件循环机制，拒绝IP socket创建、DNS、connect、bind、sendto及子进程，日志只允许本人QuoteBak。最终原router运行另有一次范围外文件open和一次IP socket创建被guard拒绝并由依赖捕获；未收集其具体路径/导入点，不声称零初始化尝试。没有DNS/connect/bind/sendto事件或外网发送。`http://in-process.invalid` 只作ASGI请求的进程内基址，没有真实网络HTTP、监听端口或供应商请求。完整当前profile只在本地内存比较，证据不复制年报正文、密钥或完整API载荷。

历史限制：6.FF原临时脚本已按清理合同删除，不能还原其确切socket阻断写法，也不能据本次单股检查补称原5笔全程clean exit或生产服务生命周期已验证。原 `http-first-attempt.json`、`http-verification.json` 和50秒exit124保持。本轮证明当前沙箱本身足以造成同类等待，并证明原router代表性路径在允许本地IPC时可正常结束。

复现仅用于必要复核，默认不继续运行。cwd `/home/python/Quote`；带 `--router` 的命令须用工具获准执行环境，保持guard和本地IPC。持久脚本与最终已运行脚本逐字节相同，不是新的测试平台：

```bash
source scripts/quote_dev_env.sh
b2_tmp=$(mktemp -d "$TMPDIR/company-profile-6fg-b2-replay-XXXXXXXX")
export B2_DIAGNOSTIC_ROOT="$b2_tmp"
# 最小stdlib用例：受限沙箱预期124；允许本地IPC且仍禁外网时预期0。
timeout 6 /home/python/miniconda3/envs/Quote/bin/python -u openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automation-readiness-20261124/b2-repro.py --guard
b2_rc=$?
printf 'python_exit=%s\n' "$b2_rc"
# 原router唯一单股；获准本地IPC环境，本轮实测0。
QMT_BUSINESS_API_SANDBOX=1 PYTHONPATH=/home/python/Quote timeout 25 /home/python/miniconda3/envs/Quote/bin/python -u openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automation-readiness-20261124/b2-repro.py --guard --router
b2_rc=$?
printf 'python_exit=%s\n' "$b2_rc"
```

保护/Review：router、冻结reads/test、原两份HTTP失败证据hash未变；现研究根154文件的排序sha256sum清单摘要前后相等，current API hash未变。A的277全范围保护及人工依赖审核仍归A/B1，本轮不重复其审核。无本卡应用P1/P2；requests依赖版本警告属既有无关项，不改依赖。49既有脏改、A文档、B1产物、正式PDF/cache及647/1050根均未修改。

开工核6.FF A/B旧根均不存在。本人root `/home/python/Quote/data/QuoteBak/company-profile-6fg-b2-Qm2PZf5P` 已在持久最小复现/结果核对、进程退出后删除并确认不存在：9文件、14,034逻辑bytes。删除的是可复建脚本/过程日志，一套最小复现及失败摘要已保留；没有业务库或20股全文副本。必要证据仅本目录 `b2-*`，原首次失败保持，A/B1及未知归属目录未动。活跃开发时间未精确计时，不把5.8秒检查耗时当开发总时长。
