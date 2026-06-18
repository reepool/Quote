"""
Database module for the quote system.
Provides SQLite database operations with async support.
"""

from .operations import DatabaseOperations
from . import operations as db_operations_module

# 复用 operations 模块级实例，避免导入 database 时重复初始化全局 db_manager。
db_ops = db_operations_module.database_operations

# 保持向后兼容
db_operations = db_ops

__all__ = ['models', 'connection', 'operations', 'db_ops', 'db_operations', 'DatabaseOperations', 'db_operations_module']
