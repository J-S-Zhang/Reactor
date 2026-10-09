#!/usr/bin/env bash
# 对应《Reactor网络服务器性能测试方案》预设场景（需先启动 echo_server）
set -e
BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)/build"
SERVER_HOST="${SERVER_HOST:-127.0.0.1}"
SERVER_PORT="${SERVER_PORT:-8080}"

if [[ ! -x "$BENCH_DIR/bench_echo" ]]; then
  echo "请先编译: cd Tast && mkdir -p build && cd build && cmake .. && make -j"
  exit 1
fi

echo "=== 4.1 短连接 1KB 10分钟 (示例改为 60s 冒烟) ==="
"$BENCH_DIR/bench_echo" --host "$SERVER_HOST" --port "$SERVER_PORT" \
  --threads 8 --conns 50 --duration 60 --size 1024 --short-conn

echo "=== 4.1 长连接 1KB 持续 QPS ==="
"$BENCH_DIR/bench_echo" --host "$SERVER_HOST" --port "$SERVER_PORT" \
  --threads 8 --conns 100 --duration 60 --size 1024

echo "=== 2.3 连接数 1000 保持 30s ==="
"$BENCH_DIR/bench_connect" --host "$SERVER_HOST" --port "$SERVER_PORT" \
  --conns 1000 --hold 30

echo "=== 4.3 消息 10KB ==="
"$BENCH_DIR/bench_echo" --host "$SERVER_HOST" --port "$SERVER_PORT" \
  --threads 4 --conns 50 --duration 30 --size 10240

echo "全部场景脚本执行完毕。正式 10min/30min 请增大 --duration / --hold。"
