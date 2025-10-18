#!/bin/bash
# Telegram连接健康检查脚本

LOG_FILE="log/sys.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$TIMESTAMP] 开始Telegram连接健康检查..."

# 检查最近的连接错误
RECENT_ERRORS=$(grep -c "Connection reset by peer\|Server closed the connection" "$LOG_FILE" | tail -1)

echo "最近1小时内的连接错误数量: $RECENT_ERRORS"

if [ "$RECENT_ERRORS" -gt 10 ]; then
    echo "⚠️  检测到频繁连接断开，建议检查网络连接或重启服务"
    echo "可以尝试: pkill -f 'main.py.*full' && python3 main.py full"
elif [ "$RECENT_ERRORS" -gt 5 ]; then
    echo "ℹ️  有一些连接不稳定，但还在可接受范围内"
else
    echo "✅ Telegram连接状态正常"
fi

# 检查重连日志
RECONNECT_COUNT=$(grep -c "Closing current connection to begin reconnect" "$LOG_FILE" | tail -1)
echo "重连次数: $RECONNECT_COUNT"

# 检查最近的Telegram活动
RECENT_ACTIVITY=$(grep "tgbot.*Message sent successfully" "$LOG_FILE" | tail -5)
if [ -n "$RECENT_ACTIVITY" ]; then
    echo "✅ Telegram消息发送正常"
    echo "最近活动:"
    echo "$RECENT_ACTIVITY"
else
    echo "ℹ️  最近没有Telegram消息活动"
fi

echo "[$TIMESTAMP] 健康检查完成"
echo ""
