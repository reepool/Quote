from pathlib import Path


# 获取项目根目录（即包含 backend 和 frontend 的那个目录）
BASE_DIR = Path(__file__).resolve().parents[1]

# 常用子目录路径
SERVICE_DIR = BASE_DIR / 'services'
CONFIG_DIR = BASE_DIR / 'config'
LOG_DIR = BASE_DIR / 'log'
DATA_DIR = BASE_DIR / 'data'


# 日志文件路径
LOG_FILE = LOG_DIR / 'sys.log'

# requirements 文件路径
REQUIREMENTS_FILE = BASE_DIR / 'requirements.txt'

# TEMPLATE_DIR = BACKEND_DIR / 'templates'

if __name__ == "__main__":
    print("BASE_DIR:", BASE_DIR)
    print("CONFIG_DIR:", CONFIG_DIR)
    print("LOG_DIR:", LOG_DIR)
    print("REQUIREMENTS_FILE:", REQUIREMENTS_FILE)