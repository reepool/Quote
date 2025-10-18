#!/bin/bash

# Quote System 优雅终止脚本
# 支持多种终止方式，确保资源正确释放

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
    echo "Quote System 优雅终止脚本"
    echo ""
    echo "使用方法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help          显示此帮助信息"
    echo "  -f, --force         强制终止（最后手段）"
    echo "  -t, --timeout N     超时时间（秒，默认：30）"
    echo "  -v, --verbose       详细输出"
    echo "  --pid PID           指定要终止的进程ID"
    echo "  --mode MODE         指定模式 (api|full|scheduler|all)"
    echo ""
    echo "终止方式（按推荐顺序）:"
    echo "  1. SIGTERM (15)     - 优雅终止，允许清理资源"
    echo "  2. SIGINT (2)       - 中断信号，类似Ctrl+C"
    echo "  3. SIGKILL (9)      - 强制终止，不清理资源"
    echo ""
    echo "示例:"
    echo "  $0                 # 优雅终止所有服务"
    echo "  $0 --mode api      # 仅终止API服务"
    echo "  $0 --force         # 强制终止所有服务"
    echo "  $0 --timeout 60    # 60秒超时"
    echo "  $0 --pid 12345     # 终止指定进程"
}

# 检查项目目录
check_project_dir() {
    if [ ! -f "$PROJECT_ROOT/main.py" ]; then
        log_error "无法找到main.py，请确保在正确的项目目录中运行此脚本"
        exit 1
    fi
}

# 获取运行中的进程
get_running_processes() {
    local mode=${1:-"all"}

    log_info "检查运行中的进程 (模式: $mode)..."

    case $mode in
        "api")
            pgrep -f "python main.py api" || true
            ;;
        "full")
            pgrep -f "python main.py full" || true
            ;;
        "scheduler")
            pgrep -f "python main.py scheduler" || true
            ;;
        "all"|*)
            pgrep -f "python main.py" || true
            ;;
    esac
}

# 显示进程信息
show_process_info() {
    local pids=($@)

    if [ ${#pids[@]} -eq 0 ]; then
        log_warning "没有找到运行中的Quote System进程"
        return 1
    fi

    echo ""
    log_info "找到 ${#pids[@]} 个运行中的进程:"
    echo ""

    for pid in "${pids[@]}"; do
        if [ -d "/proc/$pid" ]; then
            cmd=$(ps -p $pid -o cmd --no-headers 2>/dev/null | xargs)
            start_time=$(ps -p $pid -o lstart --no-headers 2>/dev/null | xargs)
            memory=$(ps -p $pid -o rss --no-headers 2>/dev/null | xargs)
            cpu=$(ps -p $pid -o %cpu --no-headers 2>/dev/null | xargs)

            echo "  PID: $pid"
            echo "  命令: $cmd"
            echo "  启动时间: $start_time"
            echo "  内存使用: ${memory:-N/A} KB"
            echo "  CPU使用: ${cpu:-N/A}%"
            echo ""
        else
            log_warning "进程 $pid 已不存在"
        fi
    done
}

# 发送信号
send_signal() {
    local pid=$1
    local signal=$2
    local timeout=${3:-30}

    if [ -d "/proc/$pid" ]; then
        log_info "向进程 $pid 发送信号 $signal..."

        # 发送信号
        kill -$signal $pid 2>/dev/null || {
            log_error "无法向进程 $pid 发送信号"
            return 1
        }

        # 等待进程结束
        local count=0
        while [ $count -lt $timeout ]; do
            if [ ! -d "/proc/$pid" ]; then
                log_success "进程 $pid 已优雅终止"
                return 0
            fi

            sleep 1
            count=$((count + 1))

            if [ $((count % 5)) -eq 0 ]; then
                log_info "等待进程 $pid 终止... ($count/$timeout 秒)"
            fi
        done

        log_warning "进程 $pid 未在 $timeout 秒内终止"
        return 1
    else
        log_warning "进程 $pid 已不存在"
        return 0
    fi
}

# 优雅终止单个进程
graceful_stop_process() {
    local pid=$1
    local timeout=${2:-30}
    local force=${3:-false}

    if [ ! -d "/proc/$pid" ]; then
        log_warning "进程 $pid 已不存在"
        return 0
    fi

    log_info "开始优雅终止进程 $pid..."

    # 第一步：发送SIGTERM（优雅终止）
    if send_signal $pid 15 $timeout; then
        return 0
    fi

    # 第二步：发送SIGINT（中断信号）
    if [ "$force" = "false" ]; then
        log_info "尝试发送中断信号 (SIGINT)..."
        if send_signal $pid 2 10; then
            return 0
        fi
    fi

    # 第三步：强制终止（最后手段）
    log_warning "强制终止进程 $pid..."
    kill -9 $pid 2>/dev/null || {
        log_error "无法强制终止进程 $pid"
        return 1
    }

    sleep 2
    if [ ! -d "/proc/$pid" ]; then
        log_success "进程 $pid 已强制终止"
        return 0
    else
        log_error "无法终止进程 $pid"
        return 1
    fi
}

# 终止多个进程
stop_processes() {
    local pids=($@)
    local timeout=${TIMEOUT:-30}
    local force=${FORCE:-false}

    if [ ${#pids[@]} -eq 0 ]; then
        log_warning "没有进程需要终止"
        return 0
    fi

    log_info "开始终止 ${#pids[@]} 个进程..."

    local failed_count=0
    for pid in "${pids[@]}"; do
        if ! graceful_stop_process $pid $timeout $force; then
            failed_count=$((failed_count + 1))
        fi
    done

    if [ $failed_count -eq 0 ]; then
        log_success "所有进程已成功终止"
    else
        log_error "$failed_count 个进程终止失败"
        return 1
    fi
}

# 检查systemd服务
check_systemd_service() {
    if command -v systemctl >/dev/null 2>&1; then
        if systemctl is-active --quiet quote-system 2>/dev/null; then
            log_info "发现systemd服务 quote-system 正在运行"
            echo ""
            log_info "建议使用systemd命令停止服务:"
            echo "  sudo systemctl stop quote-system"
            echo ""
            read -p "是否使用systemd停止服务? (y/N): " -n 1 -r
            echo

            if [[ $REPLY =~ ^[Yy]$ ]]; then
                if sudo systemctl stop quote-system; then
                    log_success "systemd服务已停止"
                    return 0
                else
                    log_error "systemd服务停止失败"
                    return 1
                fi
            fi
        fi
    fi
    return 1
}

# 清理资源
cleanup_resources() {
    log_info "清理系统资源..."

    # 清理可能的临时文件
    if [ -f "$PROJECT_ROOT/.nfs*" ]; then
        rm -f "$PROJECT_ROOT/.nfs*" 2>/dev/null || true
    fi

    # 清理可能的锁文件
    if [ -f "$PROJECT_ROOT/log/process.pid" ]; then
        rm -f "$PROJECT_ROOT/log/process.pid" 2>/dev/null || true
    fi

    log_success "资源清理完成"
}

# 主函数
main() {
    # 解析命令行参数
    FORCE=false
    TIMEOUT=30
    VERBOSE=false
    SPECIFIC_PID=""
    MODE="all"

    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -f|--force)
                FORCE=true
                shift
                ;;
            -t|--timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            --pid)
                SPECIFIC_PID="$2"
                shift 2
                ;;
            --mode)
                MODE="$2"
                shift 2
                ;;
            *)
                log_error "未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # 设置环境变量
    export FORCE=$FORCE
    export TIMEOUT=$TIMEOUT
    export VERBOSE=$VERBOSE

    # 检查项目目录
    check_project_dir

    log_info "Quote System 优雅终止脚本"
    log_info "超时时间: ${TIMEOUT}秒"
    log_info "强制终止: $FORCE"

    # 如果指定了特定PID
    if [ -n "$SPECIFIC_PID" ]; then
        if [ -d "/proc/$SPECIFIC_PID" ]; then
            show_process_info $SPECIFIC_PID
            graceful_stop_process $SPECIFIC_PID $TIMEOUT $FORCE
            cleanup_resources
        else
            log_error "进程 $SPECIFIC_PID 不存在"
            exit 1
        fi
        exit 0
    fi

    # 检查systemd服务
    if check_systemd_service; then
        exit 0
    fi

    # 获取运行中的进程
    pids=($(get_running_processes $MODE))

    if [ ${#pids[@]} -eq 0 ]; then
        log_info "没有找到运行中的Quote System进程"
        exit 0
    fi

    # 显示进程信息
    if [ "$VERBOSE" = "true" ]; then
        show_process_info "${pids[@]}"
    fi

    # 确认终止（除非强制模式）
    if [ "$FORCE" = "false" ]; then
        echo ""
        read -p "确定要终止这 ${#pids[@]} 个进程吗? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "终止操作已取消"
            exit 0
        fi
    fi

    # 终止进程
    if stop_processes "${pids[@]}"; then
        cleanup_resources
        log_success "Quote System 已优雅终止"
    else
        log_error "终止过程中出现错误"
        exit 1
    fi
}

# 信号处理
trap 'log_info "收到中断信号，正在退出..."; exit 130' INT TERM

# 执行主函数
main "$@"