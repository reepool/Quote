#!/bin/bash

# 交易日行情库维护脚本
# 使用方法: ./scripts/daily_maintenance.sh [选项]

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
    echo "交易日行情库维护脚本"
    echo ""
    echo "使用方法:"
    echo "  $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help              显示此帮助信息"
    echo "  -s, --status            检查系统状态"
    echo "  -u, --update            更新最新交易日数据"
    echo "  -f, --force-update      强制更新今日数据"
    echo "  -c, --check             检查数据完整性"
    echo "  -l, --logs              查看最新日志"
    echo "  -r, --restart           重启调度器"
    echo "  --start-scheduler       启动调度器"
    echo "  --stop-scheduler        停止调度器"
    echo "  --api-status            检查API状态"
    echo "  --data-stats            显示数据统计"
    echo "  --backup                备份数据库"
    echo ""
    echo "示例:"
    echo "  $0 --status             # 检查系统状态"
    echo "  $0 --update             # 更新数据"
    echo "  $0 --check              # 检查数据完整性"
    echo "  $0 --restart            # 重启调度器"
}

# 检查系统状态
check_status() {
    log_info "检查系统状态..."

    cd "$PROJECT_ROOT"

    # 检查Python环境
    if ! python -c "import sys; print('Python version:', sys.version)" 2>/dev/null; then
        log_error "Python环境异常"
        return 1
    fi

    # 检查系统状态
    log_info "系统状态:"
    python main.py status

    # 检查进程
    log_info "运行中的进程:"
    if pgrep -f "python main.py scheduler" > /dev/null; then
        log_success "调度器正在运行"
    else
        log_warning "调度器未运行"
    fi

    if pgrep -f "python main.py api" > /dev/null; then
        log_success "API服务正在运行"
    else
        log_warning "API服务未运行"
    fi

    log_success "系统状态检查完成"
}

# 更新数据
update_data() {
    local force=$1
    cd "$PROJECT_ROOT"

    if [ "$force" = "true" ]; then
        log_info "强制更新今日数据..."
        # 获取今日日期
        today=$(date '+%Y-%m-%d')
        curl -s -X POST "http://localhost:8000/api/v1/data/update" \
          -H "Content-Type: application/json" \
          -d "{\"exchanges\": [\"SSE\", \"SZSE\"], \"start_date\": \"$today\", \"force_update\": true}" \
          || {
            log_warning "API服务不可用，使用命令行更新..."
            python main.py update --exchanges SSE SZSE
          }
    else
        log_info "更新最新交易日数据..."
        python main.py update --exchanges SSE SZSE
    fi

    if [ $? -eq 0 ]; then
        log_success "数据更新完成"
    else
        log_error "数据更新失败"
        return 1
    fi
}

# 检查数据完整性
check_data() {
    log_info "检查数据完整性..."
    cd "$PROJECT_ROOT"

    # 获取数据统计
    log_info "数据统计信息:"
    if curl -s "http://localhost:8000/api/v1/data/stats" 2>/dev/null; then
        log_success "API统计信息获取成功"
    else
        log_warning "API服务不可用，尝试命令行获取..."
        python -c "
import asyncio
import sys
sys.path.append('.')
from main import QuoteSystem

async def check_data():
    system = QuoteSystem()
    await system.initialize()
    status = await system.get_system_status()
    print(f'总股票数: {status[\"data_manager\"][\"download_progress\"][\"total_instruments\"]}')
    print(f'已处理: {status[\"data_manager\"][\"download_progress\"][\"processed_instruments\"]}')
    print(f'成功率: {status[\"data_manager\"][\"download_progress\"][\"success_rate\"]:.1f}%')

asyncio.run(check_data())
"
    fi

    # 检查最新数据日期
    log_info "最新数据日期检查..."
    latest_date=$(curl -s "http://localhost:8000/api/v1/data/latest?exchange=SSE" 2>/dev/null | python -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('latest_date', 'Unknown'))
except:
    print('API Error')
" 2>/dev/null) || echo "API不可用"

    if [ "$latest_date" != "API不可用" ] && [ "$latest_date" != "Unknown" ]; then
        log_success "最新数据日期: $latest_date"
    else
        log_warning "无法获取最新数据日期"
    fi

    log_success "数据完整性检查完成"
}

# 查看日志
show_logs() {
    local lines=${1:-50}
    log_info "显示最新 $lines 行日志..."

    if [ -f "$PROJECT_ROOT/log/sys.log" ]; then
        tail -n "$lines" "$PROJECT_ROOT/log/sys.log"
    else
        log_warning "日志文件不存在: $PROJECT_ROOT/log/sys.log"
    fi
}

# 重启调度器
restart_scheduler() {
    log_info "重启调度器..."

    # 停止现有调度器
    pkill -f "python main.py scheduler" || true
    sleep 2

    cd "$PROJECT_ROOT"

    # 启动调度器
    nohup python main.py scheduler > log/scheduler.log 2>&1 &

    sleep 3

    if pgrep -f "python main.py scheduler" > /dev/null; then
        log_success "调度器重启成功"
    else
        log_error "调度器重启失败"
        return 1
    fi
}

# 启动调度器
start_scheduler() {
    cd "$PROJECT_ROOT"

    if pgrep -f "python main.py scheduler" > /dev/null; then
        log_warning "调度器已在运行"
        return 0
    fi

    log_info "启动调度器..."
    nohup python main.py scheduler > log/scheduler.log 2>&1 &

    sleep 3

    if pgrep -f "python main.py scheduler" > /dev/null; then
        log_success "调度器启动成功"
    else
        log_error "调度器启动失败"
        return 1
    fi
}

# 停止调度器
stop_scheduler() {
    log_info "停止调度器..."

    if pgrep -f "python main.py scheduler" > /dev/null; then
        pkill -f "python main.py scheduler"
        sleep 2

        if pgrep -f "python main.py scheduler" > /dev/null; then
            log_warning "调度器仍在运行，强制停止..."
            pkill -9 -f "python main.py scheduler"
        fi

        log_success "调度器已停止"
    else
        log_warning "调度器未运行"
    fi
}

# 检查API状态
check_api_status() {
    log_info "检查API状态..."

    if curl -s "http://localhost:8000/api/v1/system/status" > /dev/null; then
        log_success "API服务正常"

        # 显示简要状态
        log_info "API简要状态:"
        curl -s "http://localhost:8000/api/v1/system/status" | python -c "
import sys, json
try:
    data = json.load(sys.stdin)
    dm = data['data_manager']
    print(f\"  数据管理器: {'运行中' if dm['is_running'] else '空闲'}\")
    print(f\"  处理进度: {dm['download_progress']['processed_instruments']}/{dm['download_progress']['total_instruments']}\")
    print(f\"  成功率: {dm['download_progress']['success_rate']:.1f}%\")
except Exception as e:
    print(f\"  解析错误: {e}\")
"
    else
        log_error "API服务不可用"
        return 1
    fi
}

# 显示数据统计
show_data_stats() {
    log_info "获取数据统计..."
    cd "$PROJECT_ROOT"

    if curl -s "http://localhost:8000/api/v1/data/stats" > /dev/null; then
        curl -s "http://localhost:8000/api/v1/data/stats" | python -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(\"=== 数据统计 ===\")
    print(f\"股票总数: {data['instruments_count']:,}\")
    print(f\"行情记录数: {data['quotes_count']:,}\")
    print(\"数据日期范围:\")
    for exchange, dates in data['quotes_date_range'].items():
        print(f\"  {exchange}: {dates.get('start_date', 'N/A')} ~ {dates.get('end_date', 'N/A')}\")
    print(\"按交易所分布:\")
    for exchange, count in data['instruments_by_exchange'].items():
        print(f\"  {exchange}: {count:,}\")
    print(\"最近更新:\")
    for update in data['recent_updates'][:3]:
        print(f\"  {update['exchange']}: {update['update_date']} ({update['new_records']}新, {update['updated_records']}更新)\")
except Exception as e:
    print(f\"获取统计失败: {e}\")
"
    else
        log_warning "API服务不可用，尝试命令行获取..."
        python main.py status
    fi
}

# 备份数据库
backup_database() {
    log_info "备份数据库..."
    cd "$PROJECT_ROOT"

    if [ -f "data/quotes.db" ]; then
        backup_name="data/quotes_backup_$(date '+%Y%m%d_%H%M%S').db"
        cp "data/quotes.db" "$backup_name"

        if [ $? -eq 0 ]; then
            log_success "数据库备份成功: $backup_name"

            # 清理旧备份（保留最近7天）
            find data/ -name "quotes_backup_*.db" -mtime +7 -delete
            log_info "已清理7天前的备份文件"
        else
            log_error "数据库备份失败"
            return 1
        fi
    else
        log_warning "数据库文件不存在: data/quotes.db"
    fi
}

# 主函数
main() {
    # 检查项目目录
    if [ ! -f "$PROJECT_ROOT/main.py" ]; then
        log_error "无法找到main.py，请确保在正确的项目目录中运行此脚本"
        exit 1
    fi

    # 解析命令行参数
    case "${1:-}" in
        -h|--help)
            show_help
            ;;
        -s|--status)
            check_status
            ;;
        -u|--update)
            update_data false
            ;;
        -f|--force-update)
            update_data true
            ;;
        -c|--check)
            check_data
            ;;
        -l|--logs)
            show_logs "${2:-50}"
            ;;
        -r|--restart)
            restart_scheduler
            ;;
        --start-scheduler)
            start_scheduler
            ;;
        --stop-scheduler)
            stop_scheduler
            ;;
        --api-status)
            check_api_status
            ;;
        --data-stats)
            show_data_stats
            ;;
        --backup)
            backup_database
            ;;
        "")
            # 默认执行状态检查
            check_status
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