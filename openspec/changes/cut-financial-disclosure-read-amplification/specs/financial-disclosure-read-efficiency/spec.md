## ADDED Requirements

### Requirement: 就绪度输入在同一任务运行内零重复读取

财报披露同步与对账在同一任务运行内对同一 `(instrument_id, report_period)` 的就绪度输入（数值事实、行业隶属等）SHALL 至多执行一次数据库读取，并 SHALL 在 stale-state 清理与主候选循环之间共享。

#### Scenario: 同一候选被主循环与 stale 清理先后评估

- **WHEN** 一次任务运行中主候选循环与 stale-state 清理都需要同一 `(instrument_id, report_period)` 的数值事实
- **THEN** 该输入在本次运行内 SHALL 只被读取一次
- **AND** 两条路径的判定结果 SHALL 与各自独立读取时一致

#### Scenario: 运行内缓存未命中

- **WHEN** 出现预取批次未覆盖的候选
- **THEN** 系统 SHALL 执行单点回读并回填运行级缓存，语义与现状逐候选读取一致

#### Scenario: 任务运行结束

- **WHEN** 一次任务运行结束（成功、失败或被停止）
- **THEN** 运行级读取结果 SHALL 随运行释放，不跨运行驻留

### Requirement: 就绪度路径不解码其不需要的载荷列

就绪度判定路径读取数值事实时 SHALL NOT 拉取或反序列化判定不需要的大 JSON 载荷列（`raw_fact_json`、`dimensions_json`）；确需载荷的合并分支 SHALL 按行回读。`get_numeric_facts` 既有调用方 SHALL 继续获得完整字段。

#### Scenario: 就绪度判定仅需标量列

- **WHEN** 就绪度判定只需要标量事实列
- **THEN** 查询与解码 SHALL 不包含大 JSON 载荷列

#### Scenario: 合并分支需要原始载荷

- **WHEN** 官方补齐分支需要某事实的 `raw_fact` 载荷
- **THEN** 系统 SHALL 仅对该事实回读载荷并得到与现状一致的完整结构

#### Scenario: 既有完整调用方

- **WHEN** 既有调用方以默认参数读取数值事实
- **THEN** 返回字段集合与解码行为 SHALL 与变更前一致

### Requirement: stale-state 清理读取成本有界

`_clear_stale_ready_local_gap_states` 判定一个状态的当前就绪性 SHALL 复用运行级就绪度输入，SHALL NOT 对每个状态独立执行比主循环单候选更贵的全量就绪度查询；未被运行输入覆盖的状态 SHALL 单点回读兜底。

#### Scenario: 季报积压批量清理

- **WHEN** stale 清理面对大量积压状态
- **THEN** 每状态的额外数据库查询 SHALL 至多一次且仅限输入未覆盖者
- **AND** 清理结果 SHALL 与逐状态独立查询时一致

### Requirement: 读取效率具备回归合同

财报披露同步 SHALL 以受控 fixture 的性能合同测试锁定就绪度阶段的查询次数与耗时有界性，且性能路径的判定输出 SHALL 与现状路径逐字段等价。

#### Scenario: 合同测试基线

- **WHEN** fixture 含 2,000 候选 × 10 报告期规模的数据
- **THEN** 就绪度阶段的数据库查询次数 SHALL 不超过候选数乘以配置常数（首版 3）
- **AND** 阶段耗时 SHALL 低于合同设定上限

#### Scenario: 判定语义等价回归

- **WHEN** 对同一 fixture 分别运行性能路径与现状读取路径
- **THEN** 就绪度结果、缺失字段集合与来源标注 SHALL 逐字段一致
