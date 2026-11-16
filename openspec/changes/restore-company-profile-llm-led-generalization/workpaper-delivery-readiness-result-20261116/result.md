# 6.CY 十二家现有画像统一交付

已更新唯一[公司画像研究预览](/home/python/Quote/docs/development/company_profile_research_preview.md)，替换陈旧CN内容及CO顶部指令；旧CN/CT/CV/CW/CX原制品与成绩不改，旧current页可由Git历史追溯，没有另建legacy副本。

- 九份6.CT真实稿、上汽/海泰6.CV后段、广州6.CX复核稿，均有真实模型正文来源。广州仍明确原抽取rejected和本地格式标准化，未伪造成12份全新抽取成功或同版首发。
- 原371对象/533指标中摘录266个唯一公司-fact对象，文字、原值/限定和章页来自原query；[结构化索引](index.json)只保原query/hash、fact ID/hash、证据引用、模型/阶段与限制，按需读取原底稿，不复制十二份正文/新事实库。
- 已知5坏对象屏蔽、4源参考/解释项单列、上汽1个重复构成不重复展示，未拆句或改scope放行。7 completed/3 open/2 incomplete仅是原模型状态，不作可用率或准确率。有限独立审核引用原适用版本，不把未查内容当支持。
- [发布条件与下一步](release-readiness.md)已收口为三项真实差距：pending workpaper固定未正式接受的核心状态、最终同版质量证据、完整入口资源/费用依据。原133核心112/8/13只保CT原轮，未重新拼分；S1仍0/12、生产not_authorized。

本轮0新模型/网络探针/外传/usage/模型费用，0新数据库/TaskService运行，未启生产；旧费用unknown不改。使用原Workpaper/Evidence读取模型，只读原query和审核，未修改业务源码、配置或原记录。既有各入口三读/87等有效验证复用，本轮没有新pytest，不用JSON往返冒充新TaskService或LLM闭环。

直接验证见[validation.json](validation.json)：12源及适用审核hash未变、266摘录/引用身份与原对象一致，查询hash错误、事实hash错误、未知ID、坏项/源参考外放及重复对象6种反例全部被拒。所有本地链接与当前索引/预览hash在收尾核对；这是交付一致性，不是语义真值核验或新质量统计。

有限Review仅处理本次会错误外放/误标版本的A类问题，坏项/源参考/重复对象已按已有明确审核分开；B类特色表达、章名、供应链细度及其他尾项后置，C类既有配置/骨架/混合文档及并行不动。OpenSpec严格校验及diff检查通过。开工未见本人CW/CX/CY无依赖残留；唯一临时生成脚本已保必要快照、核hash后删除，释放14,701字节，其他无新运行库/PDF副本/过程日志。正式源/有效缓存、旧回执/attempt与既有改动保留，不重报旧清理量。

复现/核对：`bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python <operator-snapshot副本>`为本轮生成命令形状，目标index已冻结时会主动拒绝覆写；复查只读原query及index/validation即可，不为复现再改写当前页。生成时逐源校验并运行6种内存反例，收尾另核12家公司普通段落的实际ID/原句、所有本地链接及hash；不需要联网或新模型。`openspec validate restore-company-profile-llm-led-generalization --strict`和`git diff --check`均通过。

业务下一步是原owner的分级研究读取/正式接受边界，而不是再付费跑12家、清复核状态或补单家公司尾项。整体change和正式规模验收尚未完成，本卡不归档它、不降低门槛。只提交本人策展交付和可隔离的CY文档段，普通推送；最终提交hash/结果见会话交付。
