# Quote System 文档中心

## 📚 文档目录

### 🚀 核心功能 (Core Features)
- [系统架构设计](architecture.md) ⭐ - 完整的系统架构和技术栈说明
- [Telegram任务管理器](telegram_task_manager.md) ⭐ - Telegram机器人界面和任务管理
- [历史数据下载](features/historical_data_download.md) - 全量和指定日期范围的数据下载功能
- [交易日历管理](features/trading_calendar_management.md) - 交易日历的获取、缓存和更新机制
- [每日数据更新](features/daily_data_update.md) - 定时任务更新日线数据
- [数据质量评估](features/data_quality_assessment.md) - 实时数据质量评分和完整性检查
- [断点续传](features/resume_download.md) - 下载中断后的自动恢复机制
- [数据缺口检测](gap_detection_usage.md) - 自动检测和修复数据缺失
- [数据库备份实施](DATABASE_BACKUP_IMPLEMENTATION.md) - 自动备份系统配置和实施

### 🔌 API 接口 (API)
- [RESTful API 参考](api/restful_api.md) - 完整的 HTTP API 接口文档 ⭐
- [WebSocket 实时数据](api/websocket_api.md) - 实时数据推送接口
- [认证与安全](api/authentication.md) - API 认证和安全机制

### ⚙️ 配置管理 (Configuration)
- [配置文件详解](configuration/config_file.md) - config.json 完整配置说明 ⭐
- [数据源配置](configuration/data_sources.md) - 各种数据源的配置方法
- [限流配置](configuration/rate_limiting.md) - 智能限流机制配置
- [数据库配置](configuration/database.md) - 数据库连接和优化配置

### 🔧 故障排除 (Troubleshooting)
- [常见问题](troubleshooting/faq.md) - 用户常见问题和解决方案 ⭐
- [错误代码](troubleshooting/error_codes.md) - 系统错误代码和解决方法
- [性能优化](troubleshooting/performance.md) - 系统性能调优指南

### 🛠️ 开发指南 (Development)
- [项目架构](development/architecture.md) - 系统架构和模块说明（已更新至architecture.md）
- [贡献指南](development/contributing.md) - 如何参与项目开发

### 📖 使用指南 (Usage Guides)
- [单个股票下载指南](single_instrument_download_guide.md) - 单个股票数据下载操作
- [股票下载更新指南](INSTRUMENT_DOWNLOAD_UPDATE.md) - 股票数据下载和更新操作
- [测试指南](development/testing.md) - 单元测试和集成测试指南

## 🆕 最新更新 (v2.3.0)

### 新增文档 (2025-10-18)
- ⭐ **[系统架构设计](architecture.md)** - 完整的系统架构、技术栈和设计模式文档
- ⭐ **[Telegram任务管理器](telegram_task_manager.md)** - Telegram机器人界面详细使用指南
- **[更新日志](../README.md#-更新日志)** - 包含所有版本的详细更新记录

### 文档状态
- ✅ **已更新**: 核心功能文档已更新到v2.3.0
- ✅ **已更新**: API文档已更新反映最新接口
- ✅ **已更新**: 配置文档已包含最新配置选项
- ✅ **已更新**: 故障排除文档已包含最新解决方案

### 文档特色
- 📊 **架构图表**: 详细的系统架构图和数据流图
- 🤖 **智能示例**: Telegram机器人使用示例和截图
- 🔧 **配置模板**: 完整的配置文件模板和说明
- 📈 **性能指南**: 系统优化和性能调优建议

### ✨ 新功能
- **指定日期范围下载** - 支持 `--start-date` 和 `--end-date` 参数
- **智能交易日历选择** - 根据下载场景选择合适的交易日历获取方式
- **续传逻辑优化** - 修复了批次计算Bug，支持更准确的断点续传

### 🐛 Bug 修复
- 修复了续传逻辑中的批次计算错误（Issue #001）
- 修复了多处 `db_operations` 错误引用
- 优化了交易日判断逻辑，优先使用交易日历表

### 🔧 改进
- 增强了错误恢复机制
- 改进了日志记录和监控
- 优化了数据库查询性能

## 📖 快速开始

1. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

2. **基本使用**
   ```bash
   # 下载历史数据
   python main.py download --exchanges SSE SZSE --start-date 2024-01-01 --end-date 2024-12-31

   # 启动API服务
   python main.py api --host 0.0.0.0 --port 8000

   # 启动完整系统
   python main.py full --host 0.0.0.0 --port 8000
   ```

3. **查看帮助**
   ```bash
   python main.py --help
   python main.py download --help
   ```

## 🔗 相关链接

- [GitHub 仓库](https://github.com/your-username/Quote)
- [问题反馈](https://github.com/your-username/Quote/issues)
- [功能建议](https://github.com/your-username/Quote/discussions)