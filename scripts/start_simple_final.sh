#!/bin/bash

# Quote System 简化启动脚本 - systemd 兼容版本

# 设置错误退出
set -e

# 配置
PROJECT_ROOT="/home/python/Quote"
CONDA_BASE="/home/python/miniconda3"
CONDA_ENV="Quote"

# 日志函数（简化版本，不依赖date命令）
log_info() {
    echo "[INFO] - $1"
}

log_error() {
    echo "[ERROR] - $1" >&2
}

# 切换到项目目录
cd "$PROJECT_ROOT" || {
    log_error "无法切换到项目目录: $PROJECT_ROOT"
    exit 1
}

log_info "已切换到目录: $(pwd)"

# 初始化并激活 conda 环境
if [ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]; then
    source "$CONDA_BASE/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV" || {
        log_error "无法激活 conda 环境: $CONDA_ENV"
        exit 1
    }
    log_info "Conda 环境已激活: $CONDA_DEFAULT_ENV"
else
    log_error "找不到 conda 初始化脚本"
    exit 1
fi

# 验证 Python
log_info "Python 版本: $(python --version 2>&1)"

# 启动应用
log_info "启动 Quote System..."
exec python main.py full