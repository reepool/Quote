#!/bin/bash

# Quote System systemd 服务管理脚本
# 用于在 Linux 系统中安装和管理 systemd 服务

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
SERVICE_NAME="quote-system"
SERVICE_FILE="/home/python/Quote/scripts/quote-system.service"
SYSTEMD_DIR="/etc/systemd/system"
PROJECT_ROOT="/home/python/Quote"
ENV_DIR="/etc/quote-system"
ENV_FILE="$ENV_DIR/quote-system.env"
ENV_GROUP="python"

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 检查是否为root用户
check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "请使用 root 权限运行此脚本"
        echo "使用方法: sudo $0 [选项]"
        exit 1
    fi
}

# 检查项目目录
check_project() {
    if [ ! -f "$PROJECT_ROOT/main.py" ]; then
        log_error "项目目录不存在或main.py文件缺失: $PROJECT_ROOT"
        exit 1
    fi
}

# 设置环境变量文件权限
secure_env_file() {
    if getent group "$ENV_GROUP" >/dev/null 2>&1; then
        chown root:"$ENV_GROUP" "$ENV_FILE"
        chmod 640 "$ENV_FILE"
    else
        chown root:root "$ENV_FILE"
        chmod 600 "$ENV_FILE"
        log_warning "系统组 $ENV_GROUP 不存在，环境变量文件已设置为仅 root 可读"
    fi
}

# 追加缺失的环境变量键，不覆盖已有密钥
append_missing_env_key() {
    local key="$1"
    local note="$2"

    if ! grep -q "^${key}=" "$ENV_FILE"; then
        {
            echo ""
            echo "# $note"
            echo "$key="
        } >> "$ENV_FILE"
        log_info "已追加缺失环境变量: $key"
    fi
}

# 生成或维护 systemd 环境变量文件
install_env_file() {
    log_info "配置 systemd 环境变量文件..."

    mkdir -p "$ENV_DIR"
    chmod 750 "$ENV_DIR"

    if [ ! -f "$ENV_FILE" ]; then
        cat > "$ENV_FILE" <<'EOF'
# Quote System runtime environment variables
# Managed by scripts/service_manager.sh.
# Fill sensitive values here instead of tracked JSON config files.
# After editing, run: sudo systemctl restart quote-system

# Official commodity market data API keys
FRED_API_KEY=
EIA_API_KEY=

# Optional data-source credentials
TUSHARE_TOKEN=

# Optional Telegram credentials used by environment-aware components
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Optional API runtime overrides
# API_HOST=0.0.0.0
# API_PORT=8000
# API_WORKERS=1
EOF
        log_success "已生成环境变量文件: $ENV_FILE"
    else
        log_warning "环境变量文件已存在，保留已有值并只补充缺失键: $ENV_FILE"
        append_missing_env_key "FRED_API_KEY" "Official commodity market data API key: FRED"
        append_missing_env_key "EIA_API_KEY" "Official commodity market data API key: EIA"
        append_missing_env_key "TUSHARE_TOKEN" "Optional Tushare data-source token"
        append_missing_env_key "TELEGRAM_API_ID" "Optional Telegram API ID"
        append_missing_env_key "TELEGRAM_API_HASH" "Optional Telegram API hash"
        append_missing_env_key "TELEGRAM_BOT_TOKEN" "Optional Telegram bot token"
        append_missing_env_key "TELEGRAM_CHAT_ID" "Optional Telegram chat IDs, comma separated"
    fi

    secure_env_file
    log_info "请用 sudo 编辑密钥值: sudo nano $ENV_FILE"
}

# 安装服务
install_service() {
    log_info "安装 Quote System systemd 服务..."

    # 检查项目目录
    check_project

    # 生成或维护环境变量文件
    install_env_file

    # 复制服务文件
    cp "$SERVICE_FILE" "$SYSTEMD_DIR/$SERVICE_NAME.service"

    # 设置权限
    chmod 644 "$SYSTEMD_DIR/$SERVICE_NAME.service"

    # 重新加载systemd
    systemctl daemon-reload

    # 设置开机自启
    systemctl enable "$SERVICE_NAME"

    log_success "服务安装完成"
    log_info "服务文件: $SYSTEMD_DIR/$SERVICE_NAME.service"
    log_info "环境变量文件: $ENV_FILE"
    log_info "开机自启: 已启用"
}

# 卸载服务
uninstall_service() {
    log_info "卸载 Quote System systemd 服务..."

    # 停止服务
    if systemctl is-active --quiet "$SERVICE_NAME"; then
        systemctl stop "$SERVICE_NAME"
        log_info "服务已停止"
    fi

    # 禁用开机自启
    systemctl disable "$SERVICE_NAME" 2>/dev/null || true

    # 删除服务文件
    if [ -f "$SYSTEMD_DIR/$SERVICE_NAME.service" ]; then
        rm "$SYSTEMD_DIR/$SERVICE_NAME.service"
        log_info "服务文件已删除"
    fi

    # 重新加载systemd
    systemctl daemon-reload

    log_success "服务卸载完成"
}

# 启动服务
start_service() {
    log_info "启动 Quote System 服务..."
    systemctl start "$SERVICE_NAME"

    if systemctl is-active --quiet "$SERVICE_NAME"; then
        log_success "服务启动成功"
        show_status
    else
        log_error "服务启动失败"
        show_logs
        exit 1
    fi
}

# 停止服务
stop_service() {
    log_info "停止 Quote System 服务..."
    systemctl stop "$SERVICE_NAME"

    if systemctl is-active --quiet "$SERVICE_NAME"; then
        log_error "服务停止失败"
        exit 1
    else
        log_success "服务已停止"
    fi
}

# 重启服务
restart_service() {
    log_info "重启 Quote System 服务..."
    systemctl restart "$SERVICE_NAME"

    if systemctl is-active --quiet "$SERVICE_NAME"; then
        log_success "服务重启成功"
        show_status
    else
        log_error "服务重启失败"
        show_logs
        exit 1
    fi
}

# 显示服务状态
show_status() {
    log_info "服务状态:"
    systemctl status "$SERVICE_NAME" --no-pager -l

    echo ""
    log_info "端口监听状态:"
    netstat -tlnp | grep :8000 || ss -tlnp | grep :8000 || echo "端口8000未监听"
}

# 显示服务日志
show_logs() {
    local lines=${1:-50}
    log_info "最近 $lines 行日志:"
    journalctl -u "$SERVICE_NAME" -n "$lines" --no-pager
}

# 实时查看日志
follow_logs() {
    log_info "实时查看日志 (按Ctrl+C退出):"
    journalctl -u "$SERVICE_NAME" -f
}

# 显示帮助信息
show_help() {
    echo "Quote System systemd 服务管理脚本"
    echo ""
    echo "使用方法: sudo $0 [选项]"
    echo ""
    echo "选项:"
    echo "  install                 安装服务并设置开机自启"
    echo "  env                     生成或补齐 systemd 环境变量文件"
    echo "  uninstall               卸载服务"
    echo "  start                   启动服务"
    echo "  stop                    停止服务"
    echo "  restart                 重启服务"
    echo "  status                  显示服务状态"
    echo "  logs [行数]             显示服务日志"
    echo "  follow                  实时查看日志"
    echo "  -h, --help              显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  sudo $0 install         # 安装服务并生成环境变量文件"
    echo "  sudo $0 env             # 只生成或补齐环境变量文件"
    echo "  sudo $0 start           # 启动服务"
    echo "  sudo $0 status          # 查看状态"
    echo "  sudo $0 logs 100        # 查看最近100行日志"
    echo "  sudo $0 follow          # 实时查看日志"
}

# 主函数
main() {
    case "${1:-}" in
        install)
            check_root
            install_service
            ;;
        env)
            check_root
            install_env_file
            ;;
        uninstall)
            check_root
            uninstall_service
            ;;
        start)
            check_root
            start_service
            ;;
        stop)
            check_root
            stop_service
            ;;
        restart)
            check_root
            restart_service
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs "${2:-50}"
            ;;
        follow)
            follow_logs
            ;;
        -h|--help)
            show_help
            ;;
        "")
            log_warning "请指定操作选项"
            show_help
            exit 1
            ;;
        *)
            log_error "未知选项: $1"
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"
