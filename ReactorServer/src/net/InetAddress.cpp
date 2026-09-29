#include "net/InetAddress.h"

#include <arpa/inet.h>
#include <cstdio>
#include <cstring>

namespace reactor {

InetAddress::InetAddress(uint16_t port, bool loopbackOnly) {
  std::memset(&addr_, 0, sizeof addr_);
  addr_.sin_family = AF_INET;
  in_addr_t addr = loopbackOnly ? INADDR_LOOPBACK : INADDR_ANY;
  addr_.sin_addr.s_addr = htonl(addr);
  addr_.sin_port = htons(port);
}

InetAddress::InetAddress(const std::string& ip, uint16_t port) {
  std::memset(&addr_, 0, sizeof addr_);
  addr_.sin_family = AF_INET;
  ::inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr);
  addr_.sin_port = htons(port);
}

std::string InetAddress::toIp() const {
  char buf[64];
  ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof buf);
  return buf;
}

std::string InetAddress::toIpPort() const {
  char buf[64];
  ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof buf);
  size_t end = std::strlen(buf);
  uint16_t port = ntohs(addr_.sin_port);
  std::snprintf(buf + end, sizeof buf - end, ":%u", port);
  return buf;
}

uint16_t InetAddress::port() const { return ntohs(addr_.sin_port); }

}  // namespace reactor
