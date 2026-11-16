# 6.CV：必要后段已接通，真实调用待合并批准

本轮只完成接续工程，0新模型、0网络探针、0可发送绑定、真实执行0/3；没有新增真实修订结果或公司画像，不改变CT/CU失败、S1或生产权限。当前[任务卡](../company-profile-chapter-workpaper-card.md)与一次合并授权问题约束后续真实调用，文档本身不授权外传或消耗余额。

## 已完成的直接业务接点

- 上汽/海泰：原TaskService支持hash-bound `review_resume.mode=repair_and_verify`，只继承一份完整的原初核响应，然后请求一次合并修订/最终再核。不重复选章、抽取或初核；默认旧`verify_only`仍须原初核/修订两份，已有修订不能借新mode重复支付。
- 广州：支持hash-bound `workpaper_extraction_resume`，复用原accepted选章与原PDF章节，用原坏JSON和明确错误位置发成稿。程序不补括号、搬字段或写答案；作者区分继承的选章模型与本次实际抽取模型，原失败/usage保留。
- 复核seed重新绑定时，已有程序显示说明不重复追加，底稿hash不再因此改变。身份/源版本/回执raw及payload/hash、finish及磁盘接续都检查；篡改或缺失在发送前拒绝，未核范围继续限用。

三份CT真实保存输入已离线到达正确的第一个新请求：上汽/海泰均为`repair_profile_workpaper`，广州为`extract_profile_workpaper`。测试在该边界明确受控停止，无真实模型响应；小型受控成功与拒绝覆盖两后段及原writer查询/导出/新实例一致、从磁盘恢复及篡改拒发，不冒充真实纠错质量。

## 验证

统一前缀：`bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research`。

- `tests/unit/test_research/test_company_profile_workpaper_continuation.py tests/unit/test_research/test_company_profile_workpaper_review.py tests/unit/test_research/test_company_profile_workpaper_source_cells.py -k 'not task_service and not gateway_owner' --tb=short`：155 passed/3 deselected、35.93秒。
- 新continuation文件增加两条磁盘篡改反例后整文件：30 passed、14.16秒；与前述有重叠，不相加。首轮反例误按仓库根而非writer的namespace路径读取checkpoint，两例在到业务前失败，修夹具路径后通过，没有放宽断言或业务合同。
- workpaper、features、local_consumption三直接文件，`-k 'not task_service' --tb=short`：74 passed、30.79秒。未重复CU两条125秒真实保存响应TaskService、未修改公共LLM客户端或扩大修既有短时序失败。
- OpenSpec严格校验、diff、工程JSON/引用/代码与原件hash收尾。代码及原始输入hash见[engineering.json](engineering.json)。

Review只修本轮实际接续阻塞：错误参数必须传入`run_workpaper`而非`seed_material`，受控验证前已纠正；真实调用权限不随新分支自动放大。无新抽象层/框架/数据库/共享配置或调度变化，旧读取及最终再核模式兼容。展示样式和既有扩展时序失败不扩入本轮。

开工本人CU/CV无依赖垃圾0、释放0；所有本人测试根经TemporaryDirectory/finally删除，失败反例根也已删除，体积未量不虚记释放0或重复CU的4280216字节。没有operator/临时检出/完整源副本，正式PDF/有效缓存、CT/CU原回执/attempt、既有及并行改动保留。必要材料仅本说明与工程索引。

下一步只等已询的三家限定后段批准，批准后在获准联网环境完成接线预检、源码冻结及整卡真实执行/核源/清理，不再逐字段请批；未批准不启动窗口。不能以工程通过声称三家已纠错完成。
