#include "net/Socket.h"

#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "base/Logger.h"
#include "net/InetAddress.h"

namespace reactor {

Socket::~Socket() {
  if (sockfd_ >= 0) {
    ::close(sockfd_);
  }
}

void Socket::bindAddress(const InetAddress& addr) {
  if (::bind(sockfd_, addr.getSockAddr(), sizeof(struct sockaddr_in)) < 0) {
    LOG_FATAL("bind failed");
  }
}

void Socket::listen() {
  if (::listen(sockfd_, SOMAXCONN) < 0) {
    LOG_FATAL("listen failed");
  }
}

int Socket::accept(InetAddress* peeraddr) {
  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof addr);
  socklen_t addrlen = sizeof addr;
  int connfd = ::accept4(sockfd_, reinterpret_cast<struct sockaddr*>(&addr),
                         &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (connfd >= 0 && peeraddr) {
    peeraddr->setSockAddrInet(addr);
  }
  return connfd;
}

void Socket::shutdownWrite() {
  if (::shutdown(sockfd_, SHUT_WR) < 0) {
    LOG_ERROR("shutdownWrite failed");
  }
}

void Socket::setTcpNoDelay(bool on) {
  int optval = on ? 1 : 0;
  ::setsockopt(sockfd_, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof optval);
}

void Socket::setReuseAddr(bool on) {
  int optval = on ? 1 : 0;
  ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);
}

void Socket::setReusePort(bool on) {
#ifdef SO_REUSEPORT
  int optval = on ? 1 : 0;
  ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof optval);
#else
  (void)on;
#endif
}

void Socket::setKeepAlive(bool on) {
  int optval = on ? 1 : 0;
  ::setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof optval);
}

void Socket::setNonBlocking() {
  int flags = ::fcntl(sockfd_, F_GETFL, 0);
  flags |= O_NONBLOCK;
  ::fcntl(sockfd_, F_SETFL, flags);
}

int Socket::createNonblockingTcp() {
  int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                        IPPROTO_TCP);
  if (sockfd < 0) {
    LOG_FATAL("socket failed");
  }
  return sockfd;
}

}  // namespace reactor
