# 通用官方公告获取模块

## 1. 模块定位

`research.announcements` 是项目统一的官方公司公告获取边界。业务模块通过 source-neutral query 请求公告，不直接构造 CNInfo、上交所、深交所或北交所接口参数。

当前具体 provider：

- `cninfo`：支持 SSE、SZSE、BSE 的市场范围和单标的查询，单标的查询所需 `orgId` 由 provider 内部解析；
- `sse`、`szse`、`bse`：支持对应交易所的单标的官方公告查询；
- provider 能力声明与 purpose/exchange 路由分离。能力决定来源是否可用，`routing.official_announcements` 决定主源和 fallback 顺序。

通用层负责：

- 查询、能力校验和来源路由；
- 公告、附件、发布时间、来源身份和 raw lineage 归一化；
- 保守 checkpoint 与 purpose-specific audit；
- 附件 URL 信任策略、受限下载、hash 和媒体签名诊断。

业务层继续负责标题分类、PDF/OCR/LLM 解析、公司行动条款、股东事件、财务报告期、公司业务画像、归档目录和事实审批。公告下载成功不代表任何业务事实已确认。

## 2. Point-in-time 与身份规则

- 公告主键为 `source + source_announcement_id`，不同官方来源的相似标题不会自动合并。
- `published_at` 必须为带时区的规范时间；只有日期或时区不明确时保留 raw value 和 diagnostics，不伪造精确可得时点。
- provider 无稳定公告 ID 时可生成 deterministic derived ID，但必须标记 `identity_is_derived`。
- 业务筛选理由写入 `announcement_audit`，不会修改原始公告记录。

## 3. 配置

provider 参数位于 `config/10_research.json` 的 `research_config.sources.<source>.announcements`：

- endpoint、method、referer、artifact base URL；
- approved attachment hosts；
- request timeout、retry、backoff、pacing；
- provider 最大页大小和来源专用 options。

路由位于 `research_config.routing.official_announcements`：

- `default.sources` 提供默认来源顺序；
- `purposes.<purpose>.<exchange>` 可覆盖具体业务/交易所；
- `fallback_on` 只接受显式状态，例如 `failed`、`degraded`、`identity_not_found`、`indeterminate`、`success_empty`。

业务代码不得读取旧 `announcement_scan` 配置、构造 `column/plate/orgId`，也不得自行实现交易所 fallback。

## 4. 游标与失败语义

状态按 `purpose_key + source + scope_key` 存储在 `announcement_scan_state`。

只有完整且状态为 `success` 或 `success_empty` 的扫描允许推进游标。以下结果必须保留先前 committed cursor：

- 后续页请求失败；
- payload 无法归一化；
- provider identity 解析失败；
- 达到页数/请求上限但未完成范围；
- degraded、failed 或 indeterminate。

成功空结果与失败严格区分。失败不得转换成 `success_empty`。

## 5. 附件边界

`AnnouncementAttachmentRetriever` 只负责：

- 将相对路径解析到 provider 的 approved host；
- 限制 timeout、retry、redirect、字节数和 pacing；
- 返回 bytes、SHA-256、长度、final URL、媒体类型、PDF signature 和 retrieval time。

公司业务画像、公司行动和券商风控模块继续拥有各自不可变归档、manifest、parser、OCR、LLM 和 supersession 规则。业务模块不得硬编码 CNInfo 或交易所附件 host，也不得保留第二套下载 transport。

## 6. Legacy 迁移、删除与回滚

运行时最终 schema 只包含 `announcement_scan_state` 和 `announcement_audit`。初始化检测到旧 CNInfo 专用表时按以下顺序 fail-closed 执行：

1. 使用 SQLite backup API 生成一致性备份；
2. 验证 backup `PRAGMA integrity_check`、文件 SHA-256、大小和旧表行数；
3. 幂等回填通用表；
4. 对账 source-qualified key、cursor、selection reasons、ingestion lineage 和 raw payload hash；
5. 只有全部对账通过才在同一事务删除旧表。

legacy JSON 损坏、payload hash 不一致或任一行缺失都会中止清理并保留旧表。新版本不保留旧 scanner facade、storage wrapper、双写、fallback、旧配置或旧表作为回滚手段。

回滚步骤：

1. 停止新版本写入；
2. 校验 `*.pre_announcement_legacy_cleanup.*.bak` 的 SHA-256 和 SQLite integrity；
3. 恢复该备份；
4. 同时部署清理前的上一应用版本；
5. 在重新开放任务前核对旧表行数与关键 cursor。

不得只回滚代码而继续使用清理后的数据库，也不得把备份覆盖到仍有写入的生产库。

## 7. 零残留发布门禁

执行：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/dev_validation/check_announcement_legacy_residue.py
```

门禁会检查活动代码、配置、fixture 和文档中的旧 facade、storage wrapper、旧表、旧业务 fallback、旧配置键、consumer 直接 provider import 和业务域重复 transport。只有一次性迁移模块、迁移测试及历史 OpenSpec 记录允许出现旧表名。

任何命中都阻止上线，不能作为“后续清理”遗留。

## 8. 只读 live probe

必须显式指定来源、标的、日期、页数和 pacing：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/dev_validation/probe_official_announcements.py \
  --target cninfo:600000.SH \
  --target sse:600000.SH \
  --target szse:000001.SZ \
  --target bse:920833.BJ \
  --start-date 2026-07-01 \
  --end-date 2026-07-20 \
  --page-size 5 \
  --max-pages 1 \
  --request-timeout-seconds 8 \
  --request-interval-seconds 0.5 \
  --allow-live-network
```

脚本不打开 research storage、不写生产数据库、不下载附件；最多 8 个 target、5 页、每页 30 条。

### 2026-07-21 验证记录

- CNInfo `600000.SH`：`success`，1 页、1 条公告、约 `0.238s`；identity、附件和 raw response keys 正常。
- SSE `600000.SH`：该日期窗口 `success_empty`，1 页、约 `0.076s`。
- SZSE `000001.SZ`：该日期窗口 `success_empty`，1 页、约 `1.163s`。
- BSE `920833.BJ`：首次诊断返回 `text/html` 包装的 `null([{"listInfo": ...}])`。provider 已增加单元素对象数组解包并通过离线 fixture 测试。随后两次沙箱复跑遇到 DNS 解析失败，外部重跑审批服务返回 503，未获得额外联网授权。

BSE provider 因 live 可用性尚未形成稳定证据继续保持 `enabled=false`，不进入默认或业务路由；DNS/endpoint 稳定性复验通过前不得启用。该限制不影响 CNInfo 作为 BSE 当前主源。

## 9. 验证入口

核心验证包括：

- provider normalization、route fallback、附件策略；
- clean database 与 migrated database；
- payload mismatch/malformed JSON fail-closed；
- 备份恢复；
- 后续页失败、malformed payload、页界耗尽和 identity failure 不推进游标；
- 业务画像、股东、财务披露、券商风控和公司行动消费者输出回归；
- repository-wide zero-residue check。

实时官网检查只作为受限集成验证，不进入离线单元测试。
