## Context

现有 `AShareDelistingReconciler` 负责证据判断，`DatabaseOperations.mark_instrument_delisted → database.instrument_lifecycle.mark_delisted` 是生命周期写入 owner。普通 writer 拒绝不同退市日覆盖，这是正确保护，但尚无显式受控纠错路径。SSE 的 COMPANY_CODE 是公司代码，A/B 股行可共用，必须读取 STOCK_TYPE；不能仅依代码前缀隔离。

## Goals / Non-Goals

**Goals:** 修正 A/B 类别混用与限流遗漏；对固定 26 项形成可复现分类；通过同一 writer 校正已核实的历史退市日，原子保留正式证据和更正记录。

**Non-Goals:** 不改上市日、前身 lineage、财务事实或行情；不建立新表、审核平台、第二套同步/写入系统；不扩大清理名单、不放宽普通治理冲突规则、不改调度配置或历史验收材料、不将定向 audit 作为全市场生产 freshness。

## Decisions

### D1 证券类别与限流

SSE 请求 STOCK_TYPE=1,8；解析遇到 STOCK_TYPE=2 等非 A 类直接排除，类型字段存在却缺失/未知时报告解析错误；兼容无类别字段但证券代码明确为 A 股的旧格式。A_STOCK_CODE 优先，COMPANY_CODE 仅为兼容后备。parser terminal 版本升级。分页仍按原始响应的 total 处理，不按过滤后行数截页。SZSE 实际名单请求前调用已有 rate_limiter.acquire；本地 fixture 不占额度，不把 acquire 下沉到所有 HTTP 导致其他路径双重限流。

### D2 同一 owner 的显式更正参数

在现有 mark_instrument_delisted 添加可选 `date_correction` 映射，默认 None 完全保持原语义。内容为 expected_delisted_date、expected_listed_date、reason、reference。仅允许既有已退市 A 股、非空旧日期、明确官方终止名单证据、新日期已生效且晚于上市日、证据的 ID/交易所/类型/上市日与本地相同，来源 URL/hash/parser/observed_at 齐全。新旧日期相同不构成新更正。

writer 比较预期旧日与上市日；采用带旧日期/上市日/状态/来源条件的更新防止读取后并发修改。若此前同 reference/iid 的更正记录与请求的前后日期一致且当前值已是目标日，返回幂等成功而不追加记录。不同请求复用 reference 或本地不匹配则拒绝。

生命周期、受保护 metadata.a_share_terminal_evidence、`instrument_master_discrepancies` 的 `a_share_terminal_date_corrected` 行在同一 quotes.db 事务提交；记录 mode=production、scope=a_share_stock、reference、reason、before、after、证据和 corrected_at。失败全部回滚，不产生 production verification。原有 metadata 其他字段保留。正常同步不提供该参数，继续拒绝不同日期。

### D3 固定清单和有限操作

固定 26 项：SZSE 000003/000004/000015/000556/000588；SSE 600069/600074/600086/600175/600190/600240/600401/600555/600614/600625/600634/600646/600680/600687/600695/600701/600709/600747/600786/600799/601558。完整 canonical ID 及旧值保存在 evidence 的预览中，证券清单不进入生产代码。

使用获准网络环境重新取得 SSE/SZSE 批量官方名单，保存一套来源快照与 hash；固定证券无匹配、重复且日期不一致、主体/上市日不符、旧值变化则停止对应项而不是猜测。已有冻结样本预期23项退市日更正、2项上市日未决、600680不改。实际按本轮证据执行，逐项比较旧值。更正不重新激活股票、不修改事实表。

两条上市日期冲突仅核验并记录：000003 本地1991-07-03/官方1991-01-14；000004 本地1991-01-14/官方1990-12-01。无足够主体沿革证据时保留原值、保留 warning，并明确这不影响其已退市状态及当前财务池排除。

## Risks / Trade-offs

- 官方来源变化或不可用 → 不用旧快照冒充新核验；保留来源失败并只执行证据充分项。
- 更正时并发更新 → 预期旧值比较及条件 UPDATE；失败不写审计成功。
- 两个上市日差异无可确认结论 → 合法完成结果为保留待核；不承诺全市场 warning 清零。
- 实现代码未部署到常驻进程 → 不主动重启生产服务；下一次部署后自然治理验证，定向 audit 不替代生产核验。

## Migration Plan

1. 离线 provider 回归与小型 SQLite 更正验证通过后，冻结新来源和26项只读预览。
2. 已授权范围内经 owner 逐条更正明确项；每条有原子事务、前后证据。中断可幂等复跑。
3. 同一 reconciler 对固定26项 audit_only，核已解决差异与保留项；核 lifecycle 字段、instruments总数、财务池排除、只读 audit 无业务写入。
4. 保留一份权威来源/预览/结果在本 change，清理临时库与重复快照。正常提交推送并归档。
5. 如证据被推翻，停止余下写入，按保存的前后值复核；反向更正必须由同一 owner 且有新的明确官方依据，不通过直接SQL恢复或回写旧BaoStock日期。

## Open Questions

实现方案无待决项。000003/000004的上市日语义及主体沿革作为明确未决业务事实保留，不阻塞其余已确认退市日清理。
