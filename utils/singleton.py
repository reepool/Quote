# 类单例控制，作为类的装饰器使用，未创建实例的创建新实例，已创建过实例的不再创建，仅返回该实例

import logging

# 获取 singleton 模块的专用日志器
logger = logging.getLogger("Singleton")

def singleton(cls):
    instances = {}  # 存储实例的字典

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
            logger.info(f"[Singleton] New instance created: {cls.__name__}")
        else:
            logger.info(f"[Singleton] Instance already created for using: {cls.__name__}")
        return instances[cls]

    return get_instance