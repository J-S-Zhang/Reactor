#include "net/InetAddress.h"

#include <arpa/inet.h>
#include <cstdio>
#include <cstring>

namespace reactor {

/// 做什么：构造 INADDR_ANY 或 LOOPBACK + port 的 sockaddr_in。
/// 项目角色：TcpServer 监听地址 InetAddress(8080)。
InetAddress::InetAddress(uint16_t port, bool loopbackOnly) {
  std::memset(&addr_, 0, sizeof addr_);
  addr_.sin_family = AF_INET;
  in_addr_t addr = loopbackOnly ? INADDR_LOOPBACK : INADDR_ANY;
  addr_.sin_addr.s_addr = htonl(addr);
  addr_.sin_port = htons(port);
}

/// 做什么：inet_pton 解析 IP 字符串。
/// 项目角色：连接指定远程主机。
InetAddress::InetAddress(const std::string& ip, uint16_t port) {
  std::memset(&addr_, 0, sizeof addr_);
  addr_.sin_family = AF_INET;
  ::inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr);
  addr_.sin_port = htons(port);
}

/// 做什么：inet_ntop 输出 IP。
/// 项目角色：日志与调试。
std::string InetAddress::toIp() const {
  char buf[64];
  ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof buf);
  return buf;
}

/// 做什么：IP + ":" + port 字符串。
/// 项目角色：LOG_INFO 连接 UP/DOWN。
std::string InetAddress::toIpPort() const {
  char buf[64];
  ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof buf);
  size_t end = std::strlen(buf);
  uint16_t port = ntohs(addr_.sin_port);
  std::snprintf(buf + end, sizeof buf - end, ":%u", port);
  return buf;
}

/// 做什么：ntohs 返回主机序端口。
/// 项目角色：展示与配置。
uint16_t InetAddress::port() const { return ntohs(addr_.sin_port); }

}  // namespace reactor
