/**
 * 聊天室示例：监听 9090，广播聊天消息（kMsgChat=1，加入提示 kMsgJoin=2）。
 */
#include <memory>
#include <mutex>
#include <set>

#include "base/Logger.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TcpServer.h"
#include "protocol/Codec.h"

namespace {

constexpr uint16_t kMsgChat = 1;
constexpr uint16_t kMsgJoin = 2;

/// 维护在线连接并广播协议包
class ChatRoom {
 public:
  void add(const reactor::TcpConnectionPtr& conn) {
    std::lock_guard<std::mutex> lock(mutex_);
    conns_.insert(conn);
    broadcast(reactor::Codec::encode(kMsgJoin, conn->name() + " joined"), conn);
  }

  void remove(const reactor::TcpConnectionPtr& conn) {
    std::lock_guard<std::mutex> lock(mutex_);
    conns_.erase(conn);
  }

  void onChat(const reactor::TcpConnectionPtr& from, const std::string& text) {
    std::string payload = from->name() + ": " + text;
    broadcast(reactor::Codec::encode(kMsgChat, payload), nullptr);
  }

 private:
  void broadcast(const std::string& packet,
                 const reactor::TcpConnectionPtr& except) {
    for (const auto& conn : conns_) {
      if (conn != except && conn->connected()) {
        conn->send(packet);
      }
    }
  }

  std::mutex mutex_;                              ///< 保护 conns_
  std::set<reactor::TcpConnectionPtr> conns_;   ///< 当前在线连接
};

}  // namespace

int main() {
  reactor::AsyncLogger::instance().setLogLevel(reactor::INFO);
  auto room = std::make_shared<ChatRoom>();

  reactor::EventLoop loop;
  reactor::InetAddress listenAddr(9090);
  reactor::TcpServer server(&loop, listenAddr, "ChatServer");
  server.setWorkerThreadNum(4);

  auto codec = std::make_shared<reactor::Codec>(
      [room](const reactor::TcpConnectionPtr& conn, uint16_t type,
             const std::string& body, reactor::Timestamp) {
        if (type == kMsgChat) {
          room->onChat(conn, body);
        }
      });

  server.setConnectionCallback([room](const reactor::TcpConnectionPtr& conn) {
    if (conn->connected()) {
      room->add(conn);
      LOG_INFO("Chat - %s connected", conn->peerAddress().toIpPort().c_str());
    } else {
      room->remove(conn);
      LOG_INFO("Chat - %s disconnected", conn->peerAddress().toIpPort().c_str());
    }
  });

  server.setMessageCallback(
      [codec](const reactor::TcpConnectionPtr& conn, reactor::Buffer* buf,
              reactor::Timestamp t) { codec->onMessage(conn, buf, t); });

  server.start();
  loop.loop();
  return 0;
}
