# ReactorServer

基于 Reactor 模型的高性能 C++ 网络服务器框架（Linux / epoll）。

## 特性

- Reactor 事件循环（`EventLoop` + `Channel` + `EpollPoller`，ET + 非阻塞 IO）
- TCP 服务器（`Acceptor` / `TcpServer` / `TcpConnection`）
- 动态 `Buffer`，支持粘包拆包
- 应用层协议 `Length(4) + Type(2) + Data` 与 `Codec`
- 线程池解耦网络 IO 与业务处理
- `timerfd` 定时器（连接超时、心跳等）
- 异步日志

## 构建（Linux）

```bash
cd ReactorServer
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

## 运行示例

```bash
./echo_server    # 端口 8080
./chat_server    # 端口 9090
```

客户端需按 `Codec` 格式发包（可先写简单测试客户端）。

## 目录

与《Reactor高性能网络服务器技术方案》第 13 节一致：`include/`、`src/`、`example/`、`test/`。

## 说明

- 目标平台为 **Linux**（依赖 `epoll`、`eventfd`、`timerfd`、`accept4`、`readv` 等）。
- Windows 上可编辑代码，请在 Linux/WSL 中编译运行。
