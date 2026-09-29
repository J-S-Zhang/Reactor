#include <memory>

#include "base/Logger.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TcpServer.h"
#include "protocol/Codec.h"

int main() {
  reactor::AsyncLogger::instance().setLogLevel(reactor::INFO);
  reactor::EventLoop loop;
  reactor::InetAddress listenAddr(8080);
  reactor::TcpServer server(&loop, listenAddr, "EchoServer");
  server.setWorkerThreadNum(4);
  server.setConnectionIdleTimeout(300);

  auto codec = std::make_shared<reactor::Codec>(
      [](const reactor::TcpConnectionPtr& conn, uint16_t type,
         const std::string& body, reactor::Timestamp) {
        (void)type;
        conn->send(reactor::Codec::encode(1, body));
      });

  server.setMessageCallback(
      [codec](const reactor::TcpConnectionPtr& conn, reactor::Buffer* buf,
              reactor::Timestamp t) { codec->onMessage(conn, buf, t); });

  server.setConnectionCallback([](const reactor::TcpConnectionPtr& conn) {
    if (conn->connected()) {
      LOG_INFO("EchoServer - %s -> %s UP", conn->peerAddress().toIpPort().c_str(),
               conn->localAddress().toIpPort().c_str());
    } else {
      LOG_INFO("EchoServer - %s -> %s DOWN",
               conn->peerAddress().toIpPort().c_str(),
               conn->localAddress().toIpPort().c_str());
    }
  });

  server.start();
  loop.loop();
  return 0;
}
