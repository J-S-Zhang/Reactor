# Reactor

基于 Reactor 模型的高性能 C++ 网络服务器框架，面向 Linux 高并发 TCP 通信场景。核心代码在 **`ReactorServer/`** 目录；仓库根目录的设计文档描述架构目标与优化方向。

---

## 仓库根目录

| 路径 | 说明 |
|------|------|
| `README.md` | 本文件：项目总览、目录说明、代码阅读顺序 |
| `Reactor高性能网络服务器技术方案.md` | 总体技术方案：架构、模块划分、协议与目录规划 |
| `Reactor高性能网络服务器优化总结.md` | 性能与实现层面的优化总结（可与代码对照阅读） |
| `ReactorServer/` | **可编译运行的框架实现**（CMake 工程） |

---

## ReactorServer 目录总览

```
ReactorServer/
├── CMakeLists.txt          # 构建：静态库 reactor + 示例 + 单元测试
├── README.md               # 构建与运行说明（Linux / epoll）
├── config/
├── include/                # 头文件（按模块分子目录）
├── src/                    # 与 include 对应的实现
├── example/                # 可运行的示例服务器
├── test/                   # 简单单元测试
└── build/                  # 本地编译输出（自行 mkdir，一般不提交）
```

---

## config/

| 文件 | 作用 |
|------|------|
| `server.conf` | 示例配置项（端口、工作线程数、空闲超时、日志等）。当前示例程序在代码里写死参数，此文件便于后续扩展「读配置启动」。 |

---

## include/ 与 src/（一一对应）

头文件在 `include/`，实现一般在 `src/` 下同路径的 `.cpp`。阅读时建议 **先看 .h 再看 .cpp**。

### base/ — 基础设施

| 文件 | 作用 |
|------|------|
| `NonCopyable.h` | 禁止拷贝的基类，用于 `EventLoop`、`ThreadPool` 等单例式对象 |
| `Timestamp.h` / `Timestamp.cpp` | 微秒级时间戳，供日志、定时器、IO 回调使用 |
| `Thread.h` / `Thread.cpp` | 对 `pthread` 的薄封装，统一线程入口与命名 |
| `Logger.h` / `Logger.cpp` | **异步日志**：业务线程入队，独立后台线程写控制台/文件 |

### buffer/ — 网络缓冲

| 文件 | 作用 |
|------|------|
| `Buffer.h` / `Buffer.cpp` | 动态缓冲区（read/write 索引、`readv` 读 socket、自动扩容），支撑粘包拆包 |

### thread/ — 并发与任务

| 文件 | 作用 |
|------|------|
| `TaskQueue.h` | 模板任务队列（mutex + condition_variable），生产者/消费者模型 |
| `TaskQueue.cpp` | 占位文件；队列逻辑在头文件模板中 |
| `ThreadPool.h` / `ThreadPool.cpp` | 固定数量工作线程，从队列取任务执行业务，**与 IO 线程解耦** |

### timer/ — 定时

| 文件 | 作用 |
|------|------|
| `Timer.h` / `Timer.cpp` | 单次/重复定时任务抽象（到期时间、间隔、回调） |
| `TimerQueue.h` / `TimerQueue.cpp` | 基于 **timerfd + epoll** 的定时器队列，挂到 `EventLoop` |
| `TimeWheel.h` | 简单时间轮接口（秒级粗粒度延迟，可扩展用） |

### net/ — Reactor 与 TCP 核心

| 文件 | 作用 |
|------|------|
| `InetAddress.h` / `InetAddress.cpp` | IPv4 地址封装（bind/connect 日志展示） |
| `Socket.h` / `Socket.cpp` | socket 生命周期、非阻塞、`accept4`、常用 TCP 选项 |
| `Channel.h` / `Channel.cpp` | **fd + 关注事件 + 回调**，Reactor 里「事件处理器」的抽象 |
| `Poller.h` / `Poller.cpp` | IO 多路复用抽象；`newDefaultPoller()` 创建 Linux 实现 |
| `EpollPoller.h` / `EpollPoller.cpp` | **epoll** 封装（含 ET 边缘触发） |
| `EventLoop.h` / `EventLoop.cpp` | **Reactor 主循环**：`poll` → 分发 `Channel` → 执行 pending  functor；`eventfd` 跨线程唤醒 |
| `EventLoopThread.h` / `EventLoopThread.cpp` | 独立线程跑一个 `EventLoop`（为多 IO 线程 / Main-Sub Reactor 预留） |
| `Acceptor.h` / `Acceptor.cpp` | 监听 socket：可读时 `accept` 新连接 |
| `TcpConnection.h` / `TcpConnection.cpp` | 单条 TCP 连接：读/写/关闭、输入输出 `Buffer`、可选投递线程池、空闲超时 |
| `TcpServer.h` / `TcpServer.cpp` | 对外服务器入口：持有 `Acceptor`、连接表、线程池配置 |

### protocol/ — 应用层帧

| 文件 | 作用 |
|------|------|
| `Protocol.h` | 帧格式约定：`Length(4) + Type(2) + Data`，及长度上限等常量 |
| `Codec.h` / `Codec.cpp` | 从 `Buffer` 拆包、组包发送，回调上层「完整一条消息」 |

---

## example/ — 示例程序

| 路径 | 作用 |
|------|------|
| `echo_server/main.cpp` | 端口 **8080**：收到协议帧后原样 echo（演示 `TcpServer` + `Codec` + 线程池） |
| `chat_server/main.cpp` | 端口 **9090**：简易聊天室，连接加入/广播（演示多连接与业务回调） |

入口模式一致：`EventLoop` → `TcpServer` → 设置回调 → `start()` → `loop()`。

---

## test/ — 单元测试

| 文件 | 作用 |
|------|------|
| `Buffer_test.cpp` | Buffer 追加、取字符串、整型读写 |
| `ThreadPool_test.cpp` | 线程池并发执行任务 |
| `Timer_test.cpp` | 定时器重复间隔与 `restart` |

由 `CMakeLists.txt` 注册为 `ctest` 目标，在 `build/` 下编译后直接运行可执行文件即可。

---

## 构建相关

| 文件 | 作用 |
|------|------|
| `ReactorServer/CMakeLists.txt` | 定义 `reactor` 静态库、`echo_server`、`chat_server` 及测试目标；依赖 **Linux epoll / pthread** |

详细编译命令见 `ReactorServer/README.md`。

---

## 推荐阅读与学习顺序

按「自底向上、先单线程 Reactor 再多线程与协议」阅读，便于把数据流一次走通。

### 第一阶段：基础工具（无网络）

1. `include/base/NonCopyable.h`
2. `Timestamp` → `Thread` → `TaskQueue.h` → `ThreadPool`
3. `Buffer` + 运行 `test/Buffer_test.cpp`
4. `Logger`（理解异步日志如何不阻塞 IO）

### 第二阶段：Reactor 核心（单线程事件循环）

5. `InetAddress`、`Socket`（非阻塞创建与选项）
6. **`Channel`**：事件与回调如何绑定到 fd
7. **`Poller` / `EpollPoller`**：`epoll_wait` 与 Channel 更新
8. **`EventLoop`**：`loop()`、`runInLoop` / `queueInLoop`、与 `eventfd` 唤醒
9. `Timer` + **`TimerQueue`**（timerfd 如何并入同一 epoll 循环）

建议在此阶段对照 `Reactor高性能网络服务器技术方案.md` 第 3、8 节。

### 第三阶段：TCP 服务器链路

10. **`Acceptor`**：监听 fd 如何注册读事件并接受连接
11. **`TcpConnection`**：连接读（ET 下读尽）→ `Buffer` → 写回/关闭；关注 `connectEstablished` / `connectDestroyed`
12. **`TcpServer`**：新连接创建、`connections_` 管理、线程池与空闲超时
13. （可选）`EventLoopThread`：理解「一个线程一个 EventLoop」的扩展方式

对照方案第 4、10 节（非阻塞 socket、连接生命周期）。

### 第四阶段：协议与业务

14. **`Protocol.h` + `Codec`**：粘包拆包与发包格式
15. **`example/echo_server/main.cpp`**：最小完整服务器
16. **`example/chat_server/main.cpp`**：多连接与共享状态（注意线程安全）

对照方案第 5、7 节（协议、生产者-消费者线程模型）。

### 第五阶段：测试与扩展

17. 跑通 `ThreadPool_test`、`Timer_test`，在 Linux 下 `cmake && make` 跑两个 example
18. 阅读 `Reactor高性能网络服务器优化总结.md`，思考 Main-Sub Reactor、内存池等后续扩展点（方案第 11 节）

---

## 运行时数据流（简图）

```
客户端 TCP
    ↓
Acceptor（listen fd 可读）→ 新 fd
    ↓
TcpConnection + Channel 注册到 EpollPoller
    ↓
EventLoop::loop → epoll_wait → Channel 回调
    ↓
read → Buffer → Codec 拆包 →（可选）ThreadPool 业务
    ↓
send → 写 Buffer / 直接 write → 客户端
```

---

## 环境说明

- **目标平台**：Linux（`epoll`、`eventfd`、`timerfd`、`accept4` 等）。
- Windows 上可阅读代码；编译与运行请在 Linux 或 WSL 中进行。

若你希望把 `config/server.conf` 接到示例启动参数，或补充测试客户端，可以在 issue/需求里说明，便于按同一目录规范扩展。
