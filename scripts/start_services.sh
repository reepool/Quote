#!/bin/bash

# Quote System 服务启动脚本
# 用于启动调度器和API服务的不同组合

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目根目录
PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

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

# 显示帮助信息
show_help() {
    echo "Quote System 服务启动脚本"
    echo ""
    echo "使用方法:"
    echo "  $0 [选项] [模式]"
    echo ""
    echo "启动模式:"
    echo "  scheduler           仅启动调度器（推荐后台运行）"
    echo "  api                 仅启动API服务"
    echo "  full                启动完整系统（调度器 + API服务）"
    echo "  separate           分别启动两个进程（推荐生产环境）"
    echo ""
    echo "选项:"
    echo "  -h, --help          显示此帮助信息"
    echo "  -d, --daemon        后台运行"
    echo "  -s, --stop          停止所有服务"
    echo "  -r, --restart       重启服务"
    echo "  -t, --status        查看服务状态"
    echo "  --graceful-stop    优雅停止所有服务"
    echo "  --host HOST         API监听地址 (默认: 0.0.0.0)"
    echo "  --port PORT         API监听端口 (默认: 8000)"
    echo ""
    echo "示例:"
    echo "  $0 full             # 启动完整系统（前台运行）"
    echo "  $0 full -d          # 启动完整系统（后台运行）"
    echo "  $0 separate -d      # 分别启动两个服务（后台运行）"
    echo "  $0 scheduler -d     # 仅启动调度器（后台运行）"
    echo "  $0 --stop           # 停止所有服务"
    echo "  $0 --graceful-stop # 优雅停止所有服务"
    echo "  $0 --status         # 查看服务状态"
}

# 检查项目目录
check_project_dir() {
    if [ ! -f "$PROJECT_ROOT/main.py" ]; then
        log_error "无法找到main.py，请确保在正确的项目目录中运行此脚本"
        exit 1
    fi
    cd "$PROJECT_ROOT"
}

# 检查依赖
check_dependencies() {
    log_info "检查Python依赖..."
    if ! python -c "import baostock, telethon, fastapi, uvicorn" 2>/dev/null; then
        log_error "缺少必要的Python依赖，请运行: pip install -r requirements.txt"
        exit 1
    fi
    log_success "依赖检查通过"
}

# 停止所有服务
stop_services() {
    log_info "停止所有Quote System服务..."

    # 使用优雅终止脚本
    if [ -f "$PROJECT_ROOT/scripts/stop_services.sh" ]; then
        "$PROJECT_ROOT/scripts/stop_services.sh" --force --timeout 10
    else
        # 回退到原始方法
        pkill -f "python main.py" || true
        sleep 2
        pkill -9 -f "python main.py" || true
    fi

    log_success "所有服务已停止"
}

# 优雅停止服务
graceful_stop_services() {
    log_info "优雅停止Quote System服务..."

    if [ -f "$PROJECT_ROOT/scripts/stop_services.sh" ]; then
        "$PROJECT_ROOT/scripts/stop_services.sh" --timeout 30
    else
        log_warning "优雅终止脚本不存在，使用普通停止方式"
        stop_services
    fi
}

# 查看服务状态
check_status() {
    log_info "检查服务状态..."

    local scheduler_running=false
    local api_running=false
    local full_running=false

    # 检查调度器进程
    if pgrep -f "python main.py scheduler" > /dev/null; then
        scheduler_running=true
        log_success "调度器正在运行"
    fi

    # 检查API进程
    if pgrep -f "python main.py api" > /dev/null; then
        api_running=true
        log_success "API服务正在运行"
    fi

    # 检查完整系统进程
    if pgrep -f "python main.py full" > /dev/null; then
        full_running=true
        log_success "完整系统正在运行"
    fi

    # 显示进程信息
    if $scheduler_running || $api_running || $full_running; then
        echo ""
        log_info "运行中的进程:"
        ps aux | grep "python main.py" | grep -v grep | while read line; do
            echo "  $line"
        done
        echo ""
        log_info "API访问地址: http://localhost:8000"
        log_info "API文档: http://localhost:8000/docs"
    else
        log_warning "没有运行中的服务"
    fi
}

# 启动完整系统（单进程）
start_full_system() {
    local daemon=$1
    local host=${2:-"0.0.0.0"}
    local port=${3:-8000}

    log_info "启动完整系统（调度器 + API服务）..."
    log_info "API监听地址: $host:$port"

    if [ "$daemon" = "true" ]; then
        log_info "后台运行模式，日志输出到 log/full_system.log"
        nohup python main.py full --host "$host" --port "$port" > log/full_system.log 2>&1 &

        sleep 3
        if pgrep -f "python main.py full" > /dev/null; then
            log_success "完整系统已启动（后台运行）"
            log_info "日志文件: log/full_system.log"
            log_info "API文档: http://localhost:$port/docs"
        else
            log_error "完整系统启动失败，请检查日志: log/full_system.log"
            exit 1
        fi
    else
        log_info "前台运行模式，按Ctrl+C停止服务"
        log_info "API文档: http://localhost:$port/docs"
        python main.py full --host "$host" --port "$port"
    fi
}

# 分别启动两个服务（双进程）
start_separate_services() {
    local daemon=$1
    local host=${2:-"0.0.0.0"}
    local port=${3:-8000}

    log_info "分别启动调度器和API服务..."

    # 先停止现有服务
    stop_services

    # 启动调度器
    log_info "启动调度器..."
    if [ "$daemon" = "true" ]; then
        nohup python main.py scheduler > log/scheduler.log 2>&1 &
        scheduler_pid=$!
        sleep 2

        if pgrep -f "python main.py scheduler" > /dev/null; then
            log_success "调度器已启动（PID: $scheduler_pid）"
            log_info "日志文件: log/scheduler.log"
        else
            log_error "调度器启动失败，请检查日志: log/scheduler.log"
            exit 1
        fi
    else
        log_info "调度器前台运行..."
        python main.py scheduler &
        scheduler_pid=$!
    fi

    # 启动API服务
    sleep 2
    log_info "启动API服务..."
    if [ "$daemon" = "true" ]; then
        nohup python main.py api --host "$host" --port "$port" > log/api.log 2>&1 &
        api_pid=$!
        sleep 2

        if pgrep -f "python main.py api" > /dev/null; then
            log_success "API服务已启动（PID: $api_pid）"
            log_info "日志文件: log/api.log"
            log_info "API文档: http://localhost:$port/docs"
        else
            log_error "API服务启动失败，请检查日志: log/api.log"
            # 停止调度器
            pkill -f "python main.py scheduler"
            exit 1
        fi
    else
        log_info "API服务前台运行..."
        python main.py api --host "$host" --port "$port"
    fi

    if [ "$daemon" = "true" ]; then
        log_success "两个服务均已启动"
        log_info "调度器日志: log/scheduler.log"
        log_info "API服务日志: log/api.log"
        log_info "API文档: http://localhost:$port/docs"
    fi
}

# 重启服务
restart_services() {
    log_info "重启服务..."
    stop_services
    sleep 2
    start_separate_services true "$1" "$2"
}

# 主函数
main() {
    # 解析命令行参数
    DAEMON=false
    MODE=""
    STOP=false
    RESTART=false
    STATUS=false
    HOST="0.0.0.0"
    PORT=8000

    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -d|--daemon)
                DAEMON=true
                shift
                ;;
            -s|--stop)
                STOP=true
                shift
                ;;
            -r|--restart)
                RESTART=true
                shift
                ;;
            -t|--status)
                STATUS=true
                shift
                ;;
            --graceful-stop)
                GRACEFUL_STOP=true
                shift
                ;;
            --host)
                HOST="$2"
                shift 2
                ;;
            --port)
                PORT="$2"
                shift 2
                ;;
            scheduler|api|full|separate)
                MODE="$1"
                shift
                ;;
            *)
                log_error "未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # 检查项目目录
    check_project_dir

    # 创建日志目录
    mkdir -p log

    # 执行相应操作
    if [ "$STOP" = "true" ]; then
        stop_services
    elif [ "$GRACEFUL_STOP" = "true" ]; then
        graceful_stop_services
    elif [ "$STATUS" = "true" ]; then
        check_status
    elif [ "$RESTART" = "true" ]; then
        check_dependencies
        restart_services "$HOST" "$PORT"
    elif [ -z "$MODE" ]; then
        log_warning "请指定启动模式"
        show_help
        exit 1
    else
        check_dependencies

        case $MODE in
            scheduler)
                log_info "启动调度器..."
                if [ "$DAEMON" = "true" ]; then
                    log_info "后台运行模式，日志输出到 log/scheduler.log"
                    nohup python main.py scheduler > log/scheduler.log 2>&1 &
                    sleep 2

                    if pgrep -f "python main.py scheduler" > /dev/null; then
                        log_success "调度器已启动（后台运行）"
                        log_info "日志文件: log/scheduler.log"
                    else
                        log_error "调度器启动失败，请检查日志: log/scheduler.log"
                        exit 1
                    fi
                else
                    log_info "前台运行模式，按Ctrl+C停止服务"
                    python main.py scheduler
                fi
                ;;
            api)
                log_info "启动API服务..."
                log_info "API监听地址: $HOST:$PORT"

                if [ "$DAEMON" = "true" ]; then
                    log_info "后台运行模式，日志输出到 log/api.log"
                    nohup python main.py api --host "$HOST" --port "$PORT" > log/api.log 2>&1 &
                    sleep 2

                    if pgrep -f "python main.py api" > /dev/null; then
                        log_success "API服务已启动（后台运行）"
                        log_info "日志文件: log/api.log"
                        log_info "API文档: http://localhost:$PORT/docs"
                    else
                        log_error "API服务启动失败，请检查日志: log/api.log"
                        exit 1
                    fi
                else
                    log_info "前台运行模式，按Ctrl+C停止服务"
                    log_info "API文档: http://localhost:$PORT/docs"
                    python main.py api --host "$HOST" --port "$PORT"
                fi
                ;;
            full)
                start_full_system "$DAEMON" "$HOST" "$PORT"
                ;;
            separate)
                start_separate_services "$DAEMON" "$HOST" "$PORT"
                ;;
        esac
    fi
}

# 信号处理
trap 'log_info "收到中断信号，正在停止服务..."; stop_services; exit 0' INT TERM

# 执行主函数
main "$@"