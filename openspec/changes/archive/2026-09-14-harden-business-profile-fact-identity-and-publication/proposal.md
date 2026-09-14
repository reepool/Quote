## Why

当前公司画像流水线可以在正常样例下运行，但多个稳定身份没有覆盖合同、客体、主体范围和来源行，导致不同披露事实在写入或时点读取时碰撞。商品暴露生产还会把原始金额单位误判为数量，并且存在绕过完整晋升门禁的发布路径；供应链实体和候选诊断的边界也与生产要求不一致。需要在批量运行前一次性收紧这些契约，避免继续积累错误 approved 数据。

## What Changes

- 将活动、经营事实、供应链关系和商品暴露事实的稳定身份扩展到来源行、合同、主体范围和客体维度，保证同一报告中的并列事实不会互相覆盖或被时点查询压缩。
- 商品暴露事实统一复用单位目录判断维度并执行程序化规范化；未知单位保留 candidate，不得按“非货币即数量”降级。
- 商品暴露 publication 统一通过完整 promotion service；没有可执行商品/行情映射时只保留事实或候选映射，不得自动批准 DCF publication。
- 目录外的具名供应链实体保持 unresolved/人工队列，只有唯一官方标识、本地主数据唯一法定全称或已批准别名才能自动解析。
- 统一业务映射的半开有效期语义，并明确 freshness 配置的适用范围。
- 画像和商品暴露 API 默认只返回 approved 结果；候选与异常诊断必须显式请求并经过诊断访问边界。
- 增加身份碰撞、单位维度、发布门禁、实体解析、时态边界和候选 API 隔离回归测试。

## Capabilities

### New Capabilities

- `business-profile-fact-integrity`: 定义事实、合同/来源行、实体、单位和时态身份的唯一性与规范化规则。
- `business-profile-publication-boundaries`: 定义商品暴露 publication 晋升门禁以及候选/诊断 API 的访问和状态边界。

### Modified Capabilities

<!-- No root-level business-profile capability spec exists; the new capabilities formalize the current requirements. -->

## Impact

- 影响 `research/business_profile_activity_production.py`、`business_profile_semantic_runtime.py`、`business_profile_exposure_production.py`、`business_profile_governance.py`、`business_profile_review.py` 和画像 API 路由/模型。
- 不改变官方年报资产模块、公共 LLM 网关或其他团队已有工作区改动。
- 既有 approved 记录不删除；身份修复通过新版本/后继记录和兼容读取完成，需提供一次性诊断/重放入口。
