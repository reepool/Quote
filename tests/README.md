# Quote System 测试套件

## 概述

这是 Quote System 股票行情数据管理系统的完整测试套件，包含单元测试、集成测试、性能测试和端到端测试。

## 测试架构

```
tests/
├── conftest.py                 # pytest配置和fixtures
├── requirements_test.txt       # 测试专用依赖
├── run_tests.py               # 测试运行脚本
├── unit/                      # 单元测试
│   ├── test_data_manager.py
│   ├── test_data_sources/
│   │   ├── test_baostock_source.py
│   │   └── test_source_factory.py
│   ├── test_database/
│   │   ├── test_operations.py
│   │   └── test_models.py
│   ├── test_api/
│   │   └── test_routes.py
│   ├── test_scheduler/
│   │   └── test_scheduler.py
│   └── test_utils/
│       ├── test_config_manager.py
│       └── test_cache_manager.py
├── integration/               # 集成测试
│   ├── test_data_flow.py
│   └── test_api_integration.py
├── performance/               # 性能测试
│   └── test_database_performance.py
├── e2e/                      # 端到端测试
│   └── test_full_workflow.py
└── reports/                  # 测试报告目录
```

## 测试类型

### 1. 单元测试 (Unit Tests)
- **目标**: 测试单个函数、类和模块
- **覆盖率要求**: 80%+
- **运行时间**: 快速 (< 5分钟)
- **特点**: 使用Mock隔离外部依赖

### 2. 集成测试 (Integration Tests)
- **目标**: 测试模块间的交互
- **覆盖场景**: 数据流、API集成、服务间通信
- **运行时间**: 中等 (< 15分钟)
- **特点**: 真实数据库连接，模拟外部API

### 3. 性能测试 (Performance Tests)
- **目标**: 验证系统性能指标
- **测试内容**: 数据库性能、API响应时间、并发处理
- **基准指标**:
  - 数据库插入: > 100 records/second
  - API响应: < 1 second
  - 并发处理: > 10 requests/second

### 4. 端到端测试 (E2E Tests)
- **目标**: 验证完整业务流程
- **测试场景**: 数据下载、API服务、调度执行
- **运行时间**: 较慢 (< 30分钟)
- **特点**: 完整系统环境

## 运行测试

### 快速开始

```bash
# 安装测试依赖
python tests/run_tests.py install

# 运行所有测试
python tests/run_tests.py all

# 运行快速测试（开发用）
python tests/run_tests.py quick
```

### 详细用法

```bash
# 运行特定类型测试
python tests/run_tests.py unit          # 单元测试
python tests/run_tests.py integration   # 集成测试
python tests/run_tests.py performance   # 性能测试
python tests/run_tests.py e2e          # 端到端测试

# 带选项运行
python tests/run_tests.py unit --coverage --verbose
python tests/run_tests.py all --skip-slow
```

### 直接使用pytest

```bash
# 运行所有测试
pytest

# 运行特定目录
pytest tests/unit/
pytest tests/integration/

# 运行特定文件
pytest tests/unit/test_data_manager.py

# 运行特定测试函数
pytest tests/unit/test_data_manager.py::TestDataManager::test_get_stock_list

# 生成覆盖率报告
pytest --cov=. --cov-report=html

# 并行运行测试
pytest -n auto
```

## 测试配置

### 环境变量

```bash
export TESTING=true                    # 启用测试模式
export LOG_LEVEL=WARNING               # 减少测试日志噪音
export DATABASE_URL=sqlite:///:memory: # 使用内存数据库
```

### 配置文件

- `pytest.ini`: pytest主配置
- `conftest.py`: 测试fixtures和工具函数
- `requirements_test.txt`: 测试专用依赖

## 测试标记

使用pytest标记来分类测试：

```bash
# 运行特定标记的测试
pytest -m unit           # 只运行单元测试
pytest -m integration    # 只运行集成测试
pytest -m performance    # 只运行性能测试
pytest -m e2e           # 只运行端到端测试
pytest -m "not slow"    # 跳过慢速测试
```

## 模拟和Mock

### 数据源模拟
- BaoStock API模拟
- Yahoo Finance API模拟
- 网络错误模拟

### 数据库模拟
- 内存SQLite数据库
- 测试数据工厂
- 数据库事务模拟

## 测试数据

### 标准测试数据集
- 股票列表: 2个样本股票
- 行情数据: 10个交易日数据
- 交易所数据: SSE, SZSE

### 大数据集测试
- 大量股票: 1000+ instruments
- 历史数据: 1年交易日数据
- 并发测试: 100+ 并发请求

## 性能基准

### 数据库性能
- 批量插入: > 1000 records/second
- 查询响应: < 100ms (single record)
- 并发查询: > 50 queries/second

### API性能
- 响应时间: < 500ms (95th percentile)
- 并发处理: > 100 requests/second
- 错误率: < 0.1%

### 系统资源
- 内存使用: < 200MB (正常运行)
- CPU使用: < 50% (峰值负载)
- 磁盘I/O: < 50MB/s (数据导入)

## 持续集成

### GitHub Actions
- 自动运行所有测试
- 代码覆盖率检查
- 性能回归检测
- 安全漏洞扫描

### 测试报告
- HTML测试报告
- 覆盖率报告
- 性能基准报告
- 安全扫描报告

## 故障排除

### 常见问题

1. **测试数据库连接失败**
   ```bash
   export DATABASE_URL=sqlite:///:memory:
   ```

2. **外部API模拟失败**
   - 检查mock配置
   - 验证fixture设置

3. **性能测试超时**
   - 增加超时时间
   - 检查系统资源

4. **覆盖率不足**
   - 运行特定模块测试
   - 检查mock覆盖范围

### 调试技巧

```bash
# 详细输出
pytest -v -s

# 停在第一个失败
pytest -x

# 只运行失败的测试
pytest --lf

# 显示本地变量
pytest --tb=long

# 进入调试器
pytest --pdb
```

## 贡献指南

### 添加新测试

1. **单元测试**: 添加到`tests/unit/`对应目录
2. **集成测试**: 添加到`tests/integration/`
3. **性能测试**: 添加到`tests/performance/`
4. **E2E测试**: 添加到`tests/e2e/`

### 测试规范

- 使用描述性测试名称
- 每个测试一个断言主题
- 使用适当的测试标记
- 添加测试文档字符串
- 验证边界条件

### 代码质量

- 遵循PEP 8编码规范
- 使用类型提示
- 添加适当的文档
- 保持测试独立性

## 报告分析

### 覆盖率报告
- 目标: 80%+
- 重点: 核心业务逻辑
- 排除: 测试代码、配置文件

### 性能报告
- 基准对比
- 趋势分析
- 回归检测

### 质量指标
- 测试通过率
- 执行时间
- 失败模式分析

## 最佳实践

1. **测试隔离**: 每个测试独立运行
2. **数据清理**: 测试后清理临时数据
3. **异步测试**: 正确处理async/await
4. **错误模拟**: 测试错误处理路径
5. **边界测试**: 验证极端情况
6. **性能监控**: 持续监控性能指标

## 相关文档

- [pytest文档](https://docs.pytest.org/)
- [测试覆盖率文档](https://coverage.readthedocs.io/)
- [性能测试文档](https://pytest-benchmark.readthedocs.io/)
- [项目主文档](../README.md)