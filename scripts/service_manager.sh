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

# 安装服务
install_service() {
    log_info "安装 Quote System systemd 服务..."

    # 检查项目目录
    check_project

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
    echo "  sudo $0 install         # 安装服务"
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