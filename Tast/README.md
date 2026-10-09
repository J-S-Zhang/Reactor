# Tast — Reactor 性能压测工具

自研 TCP 压测客户端，配合 [Reactor网络服务器性能测试方案.md](../Reactor网络服务器性能测试方案.md)，对 `echo_server`（Codec 协议）进行 **QPS、P99 延迟、并发连接、长连接心跳** 等测试。

> 需在 **Linux / WSL2** 下编译运行（与服务端一致）。

---

## 1. 编译

```bash
# 终端 1：编译服务端
cd ReactorServer/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# 终端 2：编译压测工具
cd ../../Tast
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

生成可执行文件：

| 程序 | 对应方案章节 | 作用 |
|------|----------------|------|
| `bench_echo` | 4.1、4.3、2.1 QPS、2.2 延迟 | 发 Codec 帧，echo 校验，统计 QPS / P99 |
| `bench_connect` | 2.3、4.1 连接数 | 建立 N 条 TCP 并保持 |
| `bench_long_conn` | 4.2 长连接 + heartbeat | 大量长连接周期发包 |

---

## 2. 测试前准备

### 2.1 启动被测服务

```bash
cd ReactorServer/build
./echo_server
# 默认监听 8080；chat 压测改用 chat_server 9090 并改 --port
```

建议压测时：

- 日志级别调高：`AsyncLogger::instance().setLogLevel(WARN);`（或在 echo main 里改）
- 空闲超时关闭：`setConnectionIdleTimeout(0)`，避免长压被定时踢线
- 系统限制：`ulimit -n 1048576`

### 2.2 监控（另开终端，方案 2.4 / 2.5）

```bash
top -H -p $(pgrep echo_server)
pidstat -u -r -p $(pgrep echo_server) 1
ss -tan | grep 8080 | wc -l    # 当前连接数
vmstat 1
```

---

## 3. 操作示例

### 3.1 冒烟（10 秒）

```bash
cd Tast/build
./bench_echo --duration 10 --threads 2 --conns 5 --size 64
```

### 3.2 方案 4.1 — 短连接、1KB、持续压测

正式报告可用 `--duration 600`（10 分钟）：

```bash
./bench_echo --host 127.0.0.1 --port 8080 \
  --threads 16 --conns 50 --duration 600 --size 1024 --short-conn
```

输出含：`OK/FAIL`、`QPS`、**avg / p50 / p99 / max** 延迟（ms）。

### 3.3 方案 4.1 — 长连接持续 QPS

```bash
./bench_echo --threads 8 --conns 100 --duration 600 --size 1024
```

### 3.4 方案 2.3 — 并发连接档位

按方案尝试 1000 / 5000 / 10000（受 `ulimit -n` 限制）：

```bash
./bench_connect --conns 5000 --hold 60
```

观察：连接成功率、`ss` 连接数、服务端 RSS。

### 3.5 方案 4.2 — 长连接 + 周期心跳

```bash
# 10000 连接、30 分钟、每 5s 一轮心跳（可先缩小做试验）
./bench_long_conn --conns 1000 --duration 1800 --interval 5000
```

### 3.6 方案 4.3 — 大消息

```bash
./bench_echo --size 1024   --duration 60 --threads 4 --conns 30
./bench_echo --size 10240  --duration 60 --threads 4 --conns 30
./bench_echo --size 1048576 --duration 60 --threads 2 --conns 10
```

### 3.7 一键跑简化场景集

```bash
chmod +x ../scripts/run_scenarios.sh
../scripts/run_scenarios.sh
```

（脚本内 duration 为冒烟时长，写报告时请自行改为 600/1800 秒。）

---

## 4. 指标与报告（方案第 6 节）

记录到表格：

| 场景 | QPS | avg / P99 ms | 连接成功数 | CPU% | RSS MB | 备注 |
|------|-----|--------------|------------|------|--------|------|
| 短连接 1KB | | | | | | |
| 长连接 1KB | | | | | | |
| 5000 connect | | | | | | |

优化对比（方案第 5 节）：修改 `workerThreadNum`、日志级别、或代码分支后 **同一命令重跑** 对比即可。

---

## 5. 常见问题

1. **`connect failed` 过多**：提高 `ulimit -n`；降低 `--conns`；检查服务端是否已启动。  
2. **QPS 很低**：客户端线程/连接不够；或单机 CPU 饱和；用 `top` 区分 server/client。  
3. **不能用 wrk**：本服务为 **二进制 Codec**，需用本目录工具或自研协议客户端。  
4. **chat_server**：将 `--port 9090`，长连接心跳仍可用；广播场景需单独设计多客户端脚本。

---

## 6. 目录说明

```
Tast/
├── CMakeLists.txt      # 链接 ReactorServer 的 reactor 库与 Codec
├── README.md           # 本说明
├── scripts/run_scenarios.sh
└── src/
    ├── protocol_client.h  # 与 Codec 一致的收发帧
    ├── bench_stats.h      # QPS / 延迟统计
    ├── bench_echo.cpp
    ├── bench_connect.cpp
    └── bench_long_conn.cpp
```
