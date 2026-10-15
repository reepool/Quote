# v57专项正式复验：提交A有限审核

B正式自检达标，等待A有限验收。原后继 **2/2** 保持耗尽；A另授权专项 **1/1** 已使用，未新增执行。当前不归档、不扩批、不启动下一对，5.1仍待A通过；生产 `not_authorized`，规模声明 `false`。

## 正式结果及范围

大名城600094.SH／青山纸业600103.SH，原两份2025官方PDF、cutoff 2026-09-17、原 **307正向＋44负例** 和四条采购分类断言不变。

**307/307召回、712/712动态唯一准确率＝706事实＋六正文、数字错误0、44/44负例、四分类断言4/4。** 分母取本轮全部实际接受事实和已答正文，合同外输出计准确率，Measurement不重复计召回；逐对象与checkpoint唯一绑定。

- [全量正式对象、成绩及分类断言](v57-source-review-evidence.json)、[逐事实与正文决定](v57-actual-object-decisions.json)、[负例](v57-negative-review-evidence.json)
- [正式执行](../../../data/checkpoints/company_profile_common_core/reports/m4_v57_same_source_repair/formal_execution.json)、[监督回执](v57-special-formal-supervisor-receipt.json)、[首次制品保护](v57-formal-delivery-preservation-completion.json)
- [正式差异](v57-repair-difference.json)、[只读重查与来源核验](v57-readonly-verification-receipt.json)、[唯一复核owner回执](v57-source-review-owner-receipt.json)

相对v55仅四条采购栏目修正，706事实其余原生语义及Evidence不变、六正文逐字相同。咨询、招聘测试及建设项目保持服务/工程采购，商品及仅上期采购边界保留。九笔单位冲突租金继续保留原值和unclear，退出确定答案支持；原合同与金额未修改。

整轮 **204.54724270秒、0/50000 token**；监督进程总计205.22935227秒。计时从进程启动开始，包含首次registry、服务构造、PDF、布局和全部重试，到第二次export返回，没有把准备耗时移出。两家五阶段均成功、无复用/前驱；查询、导出、磁盘及现有唯一read owner的只读重查一致。耗时仅代表已有PDF解析缓存；原 `expansion_gates_met=false` 及计时解释保留，不因此修改门槛字段。

## 4.7的诊断、局部修复及证据限制

没有在诊断中调用正式run。先以SQLite mode=ro、隔离子进程限时执行，保留进度、调用栈和SQL。全市场registry共5,564家；原路径实测106.679秒，其中行业48.474秒、官方报告逐家公司读取54.359秒。重复连接建立是本次证实的集中开销，未发现本次需要处理的慢SQL或锁等待证据。

仅在资产repository添加同次registry的只读连接复用，退出即关闭、清除线程本地状态，不缓存数据、不固定事务快照、不新增索引，也未修改ResearchStorageManager。修复后registry52.159秒，官方报告循环4.051秒；全部5,564家、所有分类/资产字段与原路径 **完整hash相同**。默认身份、全市场分母、官方版本/字节校验和业务语义保持不变。正式入口将registry及服务构造时间扣入内部剩余预算；外部监督器也覆盖同步阻塞。

隔离新库服务构造4.466秒；人为同步阻塞在约1.5秒被终止并留下栈。详见[诊断回执及日志索引](v57-entry-diagnosis-receipt.json)、[同步阻塞回执](v57-synchronous-block-proof-supervisor-receipt.json)、[栈](v57-synchronous-block-proof-stacks.log)、[正式前Review](v57-entry-recovery-review-receipt.json)。正式日志另记录registry加载、服务构造及各阶段实际经过。

**不能追认v56的具体历史阻塞调用**，当时未取得栈；本次证明当前路径的集中开销与恢复，不把推测写成v56根因。两次带定期异步faulthandler转储的诊断探针以signal11结束，已保留，不能算恢复证据；对应不定期转储的完整前后只读探针均正常完成。隔离服务构造耗时不冒充生产库构造耗时。初次新测试缺as-of getter、诊断脚本解释器和副本历史键的局部问题也均保留说明，不影响正式代码或业务成绩。

## 验证、Review与原件保护

新入口专项 **4 passed**；完整官方owner v56/v57两版本及分类边界12项在组合中通过，另以独立保留目录重验v57两家公司 **2 passed、10 deselected**，实际局部全量307/307、712/712、44负例及四断言通过。candidate registry全部用例在组合中通过。Ruff七文件、六文件格式加registry改动范围、范围diff及当前change和三份主规格严格校验通过。不声明全套测试通过。

入口直接回归21 passed、5 failed；同五项在HEAD的repository/registry/operations定义下失败完全相同（[基线比较](v57-old-tests-baseline-comparison.log)），按C类既有模拟测试问题后置，未改预期或顺手修复。Review修复A类实际问题：重复资产连接开销及入口预算遗漏；线程隔离、嵌套/异常清理、作用域外写入恢复和预算耗尽不入队均验证。未观察到本轮剩余B侧P0/P1阻塞，A仍须独立核原文语义。

[代码冻结](v57-source-freeze-receipt.json)保留原38个company-profile模块，加本次直接修改的资产repository共39模块；[源码快照](v57-executed-code-snapshot.json)可重建。完整v57身份接续五开关、精确入队及主体展开，默认身份未改。

原首次95/307、259/315、数字10及旁挂负例更正保持；v55原B的712/712与A四错更正分别保留；v56超过309.16秒、无正式分数及token未知的超时回执保留。[v56原审核入口](v56-review-submission-original.md)与原保护哈希一致，当前入口路径映射另列，未改历史原件。3519项历史使用相同快照的v56只读机械回执；本次重核首次/复核/失败制品、旧源码重建、当前执行源码及19项既有改动，不宣称再次重哈希全部历史集合。详见[v57最终保护](v57-final-protection-verification.json)。

只提交本任务可隔离改动并普通推送；提交哈希和结果在任务回复中报告。19项任务前既有改动未触碰。行业增强、量价、性能治理、正文精简、框架及无关旧问题继续后置。B活跃开发与交接等待时间未精确计量，不用业务0token代表开发/审核消耗。一个入口交A有限审核，专项结束后不自行再运行。

只读核验说明：首次加只读SQLite guard的核验阻止了官方资产owner创建受控读取lease；失败日志保留，未写业务库。最终核验改用现有read owner只读重查，核正式绑定和PDF原字节、独立73页文本/73布局，并复用同源未改的v55完整输入等价回执；不宣称本次再次只读调用了带lease写入的page source。原冻结脚本、两次核验脚本及差异均保留于validation-review回执。监督回执stack_dump_requested_on_deadline_approach是启用配置，正常正式轮未触发信号；同步阻塞证明实际触发了信号和终止。

范围diff检查通过；完整staged默认检查另外标记原生CSV的4个CRLF行尾及signal11截断栈的1处尾空白。按保护要求保留原字节；使用cr-at-eol识别并排除该单一不可变栈原件后的检查通过，不整理历史格式。
