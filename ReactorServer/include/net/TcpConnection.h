#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "base/NonCopyable.h"
#include "base/Timestamp.h"
#include "buffer/Buffer.h"
#include "net/InetAddress.h"
#include "timer/TimerQueue.h"

namespace reactor {

class Channel;
class EventLoop;
class Socket;
class ThreadPool;

/**
 * @class TcpConnection
 * 含义：一条已建立的 TCP 连接的状态、IO 缓冲与回调。
 * 项目角色：Reactor 上除 listen 外的主要 IO 对象；读数据→Buffer→Codec/业务；
 *           写数据→outputBuffer_；生命周期由 TcpServer::connections_ 管理。
 */
class TcpConnection : NonCopyable,
                      public std::enable_shared_from_this<TcpConnection> {
 public:
  using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
  using ConnectionCallback =
      std::function<void(const TcpConnectionPtr&)>;
  using MessageCallback =
      std::function<void(const TcpConnectionPtr&, Buffer*, Timestamp)>;
  using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
  using WriteCompleteCallback =
      std::function<void(const TcpConnectionPtr&)>;

  /// 含义：连接状态机。角色：控制 send/shutdown 是否合法。
  enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };

  TcpConnection(EventLoop* loop, const std::string& name, int sockfd,
                const InetAddress& localAddr, const InetAddress& peerAddr);
  ~TcpConnection();

  /// 含义：线程安全发送字符串。角色：业务/Codec 回包。
  void send(const std::string& message);
  void send(Buffer* buf);
  /// 含义：优雅关闭（写尽后 shutdown WR）。角色：协议错误或客户端 quit。
  void shutdown();
  /// 含义：立即触发关闭流程。角色：空闲超时 forceClose。
  void forceClose();

  void setConnectionCallback(ConnectionCallback cb) {
    connectionCallback_ = std::move(cb);
  }
  void setMessageCallback(MessageCallback cb) {
    messageCallback_ = std::move(cb);
  }
  void setCloseCallback(CloseCallback cb) { closeCallback_ = std::move(cb); }
  void setWriteCompleteCallback(WriteCompleteCallback cb) {
    writeCompleteCallback_ = std::move(cb);
  }

  /// 含义：注册读事件并回调 connectionCallback(true)。角色：newConnection 最后一步。
  void connectEstablished();
  /// 含义：注销 channel、取消定时器。角色：从 TcpServer map 移除时。
  void connectDestroyed();

  EventLoop* getLoop() const { return loop_; }
  const std::string& name() const { return name_; }
  const InetAddress& localAddress() const { return localAddr_; }
  const InetAddress& peerAddress() const { return peerAddr_; }
  bool connected() const { return state_ == kConnected; }

  /// 含义：设置业务线程池。角色：非空则 messageCallback 在 worker 执行。
  void setThreadPool(ThreadPool* pool) { threadPool_ = pool; }
  /// 含义：无数据 N 秒后 forceClose。角色：方案连接超时管理。
  void setIdleTimeout(int seconds);

 private:
  void handleRead(Timestamp receiveTime);   ///< 含义：读 socket 到 input。角色：Channel 读回调。
  void handleWrite();                       ///< 含义：写 outputBuffer。角色：Channel 写回调。
  void handleClose();                       ///< 含义：对端 EOF。角色：通知 TcpServer 移除。
  void handleError();                       ///< 含义：IO 错误。角色：LOG_ERROR。
  void sendInLoop(const std::string& message);  ///< 含义：IO 线程发 string。角色：send 内部。
  void sendInLoop(const void* data, size_t len); ///< 含义：IO 线程发原始字节。角色：实际 write。
  void shutdownInLoop();                    ///< 含义：shutdown WR。角色：shutdown 内部。
  void forceCloseInLoop();                  ///< 含义：触发 closeCallback。角色：forceClose 内部。
  void setState(StateE s) { state_ = s; }  ///< 含义：改状态机。角色：连接生命周期控制。

  EventLoop* loop_;              ///< 含义：固定 IO 线程 loop。角色：所有 channel 操作在此线程。
  const std::string name_;       ///< 含义：唯一连接名 Server#id。角色：map 键、聊天室标识。
  std::atomic<StateE> state_;    ///< 含义：连接状态。角色：并发 send 与 IO 回调同步。
  bool reading_;                 ///< 含义：是否读开启。角色：预留扩展半关闭读。

  std::unique_ptr<Socket> socket_;     ///< 含义：连接 socket RAII。角色：fd 与选项。
  std::unique_ptr<Channel> channel_;   ///< 含义：连接 fd 的 Reactor 通道。角色：epoll 与回调。
  const InetAddress localAddr_;        ///< 含义：本端地址。角色：日志。
  const InetAddress peerAddr_;         ///< 含义：对端地址。角色：日志与安全。

  Buffer inputBuffer_;           ///< 含义：读缓冲。角色：粘包拆包输入。
  Buffer outputBuffer_;          ///< 含义：写缓冲。角色：写不完时排队。

  ConnectionCallback connectionCallback_;      ///< 含义：建立/断开通知。角色：TcpServer 用户回调。
  MessageCallback messageCallback_;            ///< 含义：收到数据。角色：Codec::onMessage。
  CloseCallback closeCallback_;                ///< 含义：连接关闭。角色：通知 TcpServer 移除。
  WriteCompleteCallback writeCompleteCallback_; ///< 含义：写缓冲区清空。角色：高级用法。

  ThreadPool* threadPool_;       ///< 含义：业务池指针（不拥有）。角色：可选 offload。
  TimerId idleTimer_;            ///< 含义：空闲定时器句柄。角色：cancel 于 connectDestroyed。
};

}  // namespace reactor
