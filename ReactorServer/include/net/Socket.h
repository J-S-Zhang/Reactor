#pragma once

#include "base/NonCopyable.h"

namespace reactor {

class InetAddress;

class Socket : NonCopyable {
 public:
  explicit Socket(int sockfd) : sockfd_(sockfd) {}
  ~Socket();

  int fd() const { return sockfd_; }

  void bindAddress(const InetAddress& addr);
  void listen();
  int accept(InetAddress* peeraddr);
  void shutdownWrite();

  void setTcpNoDelay(bool on);
  void setReuseAddr(bool on);
  void setReusePort(bool on);
  void setKeepAlive(bool on);
  void setNonBlocking();

  static int createNonblockingTcp();

 private:
  const int sockfd_;
};

}  // namespace reactor
