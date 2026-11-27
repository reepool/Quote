# 6.FV B机械就绪交接

状态：单股离线准备完成，待A发送冻结和用户具体预算授权；不代表v15真实检出效果。

- 原入口：新薄`review_operator.run_one`仅参数适配到`execute_configured_task → CompanyProfileTaskService.review_batch → 原writer`；复用6.FU四个本地源/资产/storage helper，未修改或调用旧prepare/run。
- 当前山东安全稿`bp-work-1deffa57365ab12095599a8f`及两层父链共3条经原writer逐字节复制。输出只在`data/research/company_profile_v15_subject_20261127`；主39未写。完整25条作者事实/24个指标、原作者和原证据保持，不向模型发送旧quality/use_review、限制备注或A参考。
- 原reader以`CURRENT_DRAFT_REVIEW_POLICY`生成计划；原`CompanyProfileTaskPlan.model_validate_json`写前和磁盘重载均通过。计划位于`data/checkpoints/company_profile_v15_subject_20261127/600350.SH/execution-plan.json`，run_id=`6fv-600350-SH-20261127`，状态`planned`。原父稿v14保真，新初核v15；主体索引逐项精确匹配25事实和全部原metric位置/主体/期间/引用。
- 拟议累计上限6000秒/1000000保守账本/6物理含3恢复，每逻辑最多1恢复，高阶1200物理/2450逻辑、Flash600；费用无价unknown，有价10元，不宣称人民币总额上限。新授权应另写authorized文件，不覆盖planned。
- 就绪合同、计划/draft/source/index/hash及入口/有效回归hash统一见`b-ready.json`与`b-input-contract.json`。复用上一轮101有效用例并集和A9直接验证；本轮未改应用/提示/测试，未跑fullmock或重复测试。402个冻结保护文件检查无漂移。
- 本轮真实客户端构造0、供应商探针0、模型请求0。A负责具体授权、配置/源冻结、同联网权限预检及before-send累计保护；本operator不替代这些保护，不创建新业务循环。
- 清理：前序本人任务根已由原交接清理；开工只发现A活跃`company-profile-6fv-a-YiALnKD6`，未触碰。本轮没有新建B临时根/工作库/完整全文副本，释放空间0；必要计划、3条父链及紧凑合同保留待真实执行，不动未知/并行产物。CLI附带遥测DNS错误不影响本地计划，无供应商调用。
- 未提交或推送Git，未改规范、配置、主39或20项既有脏文件。A负责独立原文裁决与最终Git。开发活跃耗时未精确统计；正式尝试0，交接等待未计。
