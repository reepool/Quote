"""
Database module for the quote system.
Provides SQLite database operations with async support.
"""

from .operations import DatabaseOperations
from . import operations as db_operations_module

# 创建全局数据库操作实例（自动初始化）
# auto_initialize=True 确保在创建时就完成初始化
db_ops = DatabaseOperations(auto_initialize=True)

# 保持向后兼容
db_operations = db_ops

__all__ = ['models', 'connection', 'operations', 'db_ops', 'db_operations', 'DatabaseOperations', 'db_operations_module']