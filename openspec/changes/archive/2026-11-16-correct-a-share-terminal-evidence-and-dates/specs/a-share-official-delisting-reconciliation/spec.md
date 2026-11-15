## ADDED Requirements

### Requirement: Terminal list parsing separates SSE share classes and observes source rate limits
官方终止名单 provider SHALL 按证券类别隔离 SSE A/B 股，SHALL 优先使用 A_STOCK_CODE，SHALL NOT 把共用 COMPANY_CODE 的 B 股行转换成 A 股证据。真实 SSE 请求 SHALL 限定 A 股类型1/8，解析仍 SHALL 排除非A类别；类别字段存在而值缺失/未知时 SHALL 报告错误。SZSE 每个真实终止名单请求 SHALL 在已有rate_limiter取得额度后发送，本地fixture SHALL NOT 消耗额度。

#### Scenario: SSE A and B shares share a company code
- **WHEN** 600680 的类型1记录上市日为1993-10-18，类型2记录上市日为1994-10-20，两行共用COMPANY_CODE/A_STOCK_CODE
- **THEN** provider SHALL 只输出类型1对应的600680.SH证据；本地A股日期正确时 SHALL NOT 生成冲突
- **AND** 仅有类型2行时 SHALL NOT 为600680.SH生成终态证据；回归 SHALL 使用原始真实快照

#### Scenario: SZSE uses a live request or a fixture
- **WHEN** provider读取SZSE终止名单
- **THEN** 真实请求 SHALL 先acquire再HTTP且不双重取额度，fixture读取 SHALL 零HTTP且零acquire
- **AND** 原始xlsx数值证券代码的补零规则 SHALL 保持不变

### Requirement: Historical terminal dates can only be corrected explicitly through the lifecycle owner
已有已退市A股的不同退市日 SHALL 仅通过现有生命周期owner的显式date_correction请求更正。请求 SHALL 包含非空预期旧退市日、预期上市日、理由和稳定reference，并提供与本地ID/交易所/类型/上市区间一致的正式终止名单证据及来源URL/hash/parser/观察时间；新退市日 SHALL 已生效且晚于上市日。普通同步 SHALL 继续拒绝不同退市日覆盖，不得因为本次清理而放宽。

#### Scenario: A confirmed legacy fallback date differs from the official date
- **WHEN** 已退市A股的预期旧值、上市日及明确官方证据均匹配
- **THEN** owner SHALL 以条件更新防止并发旧值变化，在同库事务更正退市日、保护终态证据并保存before/after、reason、reference与更正时间
- **AND** SHALL 保持已退市三字段一致、保留其他metadata、不修改上市日或历史财务事实

#### Scenario: Stale expectations or invalid identity prevent correction
- **WHEN** 旧值变化、证券仍active、证据属于B股/其他市场/不同上市区间、日期未生效或必需证据缺失
- **THEN** owner SHALL 拒绝本次更正并保留所有原值，SHALL NOT 写成功更正记录

#### Scenario: A correction is retried or its evidence write fails
- **WHEN** 同reference/iid及相同前后日期的更正已提交且本地仍为目标值
- **THEN** 重试 SHALL 幂等成功且不追加更正记录
- **AND** 任一生命周期/metadata/更正记录写入失败 SHALL 回滚整笔事务；reference内容冲突 SHALL 拒绝

### Requirement: The bounded legacy cleanup preserves unresolved listing dates and audit semantics
本次清理 SHALL 仅处理任务卡固定26项，并在生产更正前以新取官方名单和当前旧值生成逐项预览；无法确认的条目 SHALL 留待核验。预期23项退市日可更正、2项上市日未决及1项600680解析误报，实际结果 SHALL 以本轮来源为准。定向audit SHALL 复用现有reconciler，SHALL NOT 建立全市场production freshness基线或改写历史验收成绩。

#### Scenario: Listing dates remain disputed after terminal-date cleanup
- **WHEN** 000003.SZ和000004.SZ的本地上市日与官方名单不同，且没有充分主体沿革证据
- **THEN** 系统 SHALL 保留原上市日与退市状态，并记录未决原因及warning，SHALL NOT 为清除warning改值
- **AND** 600680.SH的A股原始日期正确时 SHALL 保持不变

#### Scenario: Review and post-correction audit use the same official evidence
- **WHEN** 固定26项处理完成
- **THEN** 交付 SHALL 保留一套官方来源/hash、逐项预期旧值/目标值、实际结果及未决项；同一reconciler的audit_only SHALL 验证已解决项不再冲突
- **AND** audit SHALL 不写业务生命周期、正式证据或生产verification；其他证券及财务事实 SHALL 保持不变
