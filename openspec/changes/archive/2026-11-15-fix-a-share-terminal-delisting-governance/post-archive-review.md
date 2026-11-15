# 归档后缺陷复核与修复（2026-11-15）

本记录补充原 `verification.json`，不改写首次测试成绩、生产纠正范围或历史运行结果。依据用户“复核属实且不过度审核的问题后修复”的授权，仅修复已归档规范中的明确行为；不新增主数据框架、表、调度、生产刷新或生产数据修正。

## 复核结果与实现

| 问题 | 属实依据与业务影响 | 原需求／任务 | 修复及对应验证 |
|---|---|---|---|
| P1：把收到决定日写成退市日 | 正文“股票于10月16日收到……终止上市的决定，摘牌安排另行公告”被解析为10月16日已退市；会错误停用 | DEL-04/05；3.1、3.6 | 日期只接受明确摘牌日标签或股票实际终止事件的连续表达；同一正文同时有决定日与实际生效日时只取后者；未确认则不生成终态证据 |
| P1：B股事件传播至A股 | A/B共同代码栏下，明确B股终止且A股继续上市仍产生A股终态 | DEL-02；2.3、6.1 | 区分明确A股、其他证券类别及未限定股票用语，排除B股／债券事件；分别验证B股标题、通用标题下B股正文及A股与B股不同日期 |
| P2：公告部分失败报完整成功 | 非空scan_result的partial／failed、is_complete及errors被忽略，正记录写入后可能整体报success | DEL-03/07；2.4、5.2 | 传播扫描状态、完整性和错误；单个关键词查询异常不丢弃另一查询已取得的正证据；partial、failed、exception均回归 |
| P2：过期未来计划无法闭合 | 新鲜一致证据成功退市，但过期计划的plan_key已在读取分支丢弃，之后fresh始终失效 | DEL-05/06/07；3.1、5.3、6.4 | 保留全部未闭合计划的身份和日期用于冲突检查，过期计划不能单独支持写入；新鲜一致证据通过原writer事务闭合对应旧key。覆盖无新证据、不一致新日期及audit不闭合生产计划 |
| P2：current与终态矛盾无冲突报告 | current仍含active行且有有效终态时，生命周期正确但conflict_count为0且报success | DEL-06/07；5.1、5.2 | 有效终态优先，按证券计一次冲突并返回warning；保存current与terminal两侧引用，fresh复用保留冲突及样例；audit不写生命周期 |
| P2：正式日期冲突证据丢失 | 两个实际日期冲突仅留下文字和计数，无法定位双方材料 | DEL-06/07；3.2、5.1 | 原差异表追加 `a_share_terminal_conflict`，保留既有生命周期及双方日期、来源URL／公告ID、hash、parser、观察时间等必要引用；25个冲突全部持久化、报告仅20个样例；保存失败明确error并禁止健康fresh |
| P2：BSE配置30页实际被限制2页 | 原max_pages配置被min(2, ...)覆盖，第三页以后候选不可达 | DEL-12；D5；3.6 | 保留既有页数配置及默认30页；沪深仍默认2页。验证1／3／30页配置、第三页候选、90日窗口、未完整扫描显式降级 |

以上均直接影响已承诺的金融语义、持久计划闭合、诊断可用性或配置兼容，不属于风格／架构偏好及推测性平台建设。

公告parser升级为 `official_stock_terminal_body.v2`。为避免旧提取逻辑在升级后仍通过生产计划恢复被执行，v1公告计划也须以新证据核验；仅限制已知存在日期／证券范围缺陷的公告parser，不影响结构化交易所名单。原 `future_effective / future_applied / verification` 的唯一存储与事务协议保持不变；冲突记录不参与计划读取或充当第二份计划载体。

## 验证与边界

- 本轮状态：7项缺陷修复、完整相关回归和提交前阻塞性自审完成。Review确认修改限于公告证券范围／日期、来源失败、计划恢复／闭合、冲突证据及BSE配置接线；未发现本轮改动引入的其他阻塞缺陷。未开展平台化、无关重构或语义识别框架开发。
- 核心测试：`tests/unit/test_a_share_delisting_reconciliation.py`，新增反例及现实兼容场景均采用合成公告或小型隔离SQLite；原康佳官方XLSX和数值16 fixture继续保留。
- 相关回归范围：官方provider、主数据sync／治理编排、财务增量／清单、报告、退市历史行情、HKEX主数据，共9个直接相关文件。
- 首轮新增fixture漏填AnnouncementRecord必填 `published_at`，造成6项构造失败；已补齐该字段后重跑，未放宽业务断言。
- 既有 `test_build_local_core_import_manifest_profiles_and_mapping_readiness` 已在原验收确认与任务基线同样失败，本轮继续排除，不修改其预期或无关profile实现。
- 全部开发命令通过 `scripts/quote_dev_env.sh`；SQLite／线程测试从获准执行环境启动，使用QuoteBak隔离库，不联网、不读写生产业务数据。
- 生产进程未重启，未执行全市场治理或财务任务；新行为由正常部署／进程加载生效。本文测试结果不代表自然生产任务成绩。
- Git基线为 `e88a2059`，开始时已有大量其他任务改动；本轮只提交可隔离的退市修复代码、测试及配套文档。

最终相关回归为 **244 passed / 1 deselected / 0 failures / 0 errors**，其中本模块由本轮基线32项增至53项，新增21个回归场景；原验收221项成绩保持原记录，不以两个阶段的总数相减推算新增用例。运行时长192.30秒。Black检查、flake8正确性检查（E9/F63/F7/F82）及本轮diff空白检查通过；4个主规范（`a-share-official-delisting-reconciliation`、`instrument-master-governance`、`instrument-master-sync`、`research-data-engine`）均通过OpenSpec `--type spec --strict --no-interactive`，退出码0。

完整回归命令（通过项目环境入口执行）：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_a_share_delisting_reconciliation.py \
  tests/unit/test_a_share_official_stock_master.py \
  tests/unit/test_instrument_master_sync.py \
  tests/unit/test_master_governance_orchestration.py \
  tests/unit/test_scripts/test_prepare_sina_ths_local_core_import_manifest.py \
  tests/unit/test_research/test_financial_disclosure_incremental_sync.py \
  tests/unit/test_scheduler_instrument_master_governance_report.py \
  tests/unit/test_delisted_a_share_quote_backfill.py \
  tests/unit/test_hkex_instrument_master.py \
  -k 'not test_build_local_core_import_manifest_profiles_and_mapping_readiness'
```

已验证代码SHA256：

- `instrument_master_delisting.py`：`21e1be291a4d5e0a45cf68606722b8db7a18687ad85b5c17b7f2bb2f9ce789b7`
- `tests/unit/test_a_share_delisting_reconciliation.py`：`9646693017712b11ad039ccb0ce5d510bb960ff36f6b690ecb74169e53b00719`

## 清理与交接

开发前盘点 `delisting_review_*`、`delisting_repair_*` 及前序 `instrument_delisting_20261115`，无残留相关临时目录。本人本轮目录 `data/QuoteBak/delisting_repair_n_msnbvv` 已在核对测试XML、源代码hash及Git隔离后删除，共104个文件、6,020,670字节，未保留无依赖临时制品。权威复核／验证结果保留在本文，回归输入保留在正式测试中。其他任务的目录、共享缓存及不明归属内容未清理。
