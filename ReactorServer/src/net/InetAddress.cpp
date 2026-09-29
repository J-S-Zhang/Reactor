#include "net/InetAddress.h"

#include <arpa/inet.h>
#include <cstdio>
#include <cstring>

namespace reactor {

/// 构造监听地址：0.0.0.0 或 127.0.0.1 + 端口
InetAddress::InetAddress(uint16_t port, bool loopbackOnly) {
  std::memset(&addr_, 0, sizeof addr_);
  addr_.sin_family = AF_INET;
  in_addr_t addr = loopbackOnly ? INADDR_LOOPBACK : INADDR_ANY;
  addr_.sin_addr.s_addr = htonl(addr);
  addr_.sin_port = htons(port);
}

/// 从点分十进制 IP 字符串构造
InetAddress::InetAddress(const std::string& ip, uint16_t port) {
  std::memset(&addr_, 0, sizeof addr_);
  addr_.sin_family = AF_INET;
  ::inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr);
  addr_.sin_port = htons(port);
}

/// 仅 IP 字符串
std::string InetAddress::toIp() const {
  char buf[64];
  ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof buf);
  return buf;
}

/// "ip:port" 形式
std::string InetAddress::toIpPort() const {
  char buf[64];
  ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof buf);
  size_t end = std::strlen(buf);
  uint16_t port = ntohs(addr_.sin_port);
  std::snprintf(buf + end, sizeof buf - end, ":%u", port);
  return buf;
}

/// 主机序端口号
uint16_t InetAddress::port() const { return ntohs(addr_.sin_port); }

}  // namespace reactor
