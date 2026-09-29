#pragma once

#include <netinet/in.h>
#include <string>

namespace reactor {

/**
 * @class InetAddress
 * 含义：IPv4 sockaddr_in 的值类型封装。
 * 项目角色：Acceptor bind、TcpConnection 记录 local/peer、日志打印 ip:port。
 */
class InetAddress {
 public:
  /// 含义：ANY 或 loopback + 端口。角色：TcpServer 监听地址 listenAddr(8080)。
  explicit InetAddress(uint16_t port = 0, bool loopbackOnly = false);
  /// 含义：指定 IP 字符串与端口。角色：连接指定主机。
  InetAddress(const std::string& ip, uint16_t port);
  /// 含义：从系统 sockaddr_in 拷贝。角色：accept/getsockname 结果包装。
  explicit InetAddress(const struct sockaddr_in& addr) : addr_(addr) {}

  /// 含义：地址族 AF_INET 等。角色：socket API 参数。
  sa_family_t family() const { return addr_.sin_family; }
  /// 含义：点分十进制 IP。角色：日志。
  std::string toIp() const;
  /// 含义：ip:port 字符串。角色：连接 UP/DOWN 日志。
  std::string toIpPort() const;
  /// 含义：主机序端口。角色：配置展示。
  uint16_t port() const;

  /// 含义：转为 bind/connect 用的 sockaddr*。角色：Socket::bindAddress。
  const struct sockaddr* getSockAddr() const {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }
  /// 含义：用 accept 得到的地址覆盖。角色：Socket::accept 填充 peer。
  void setSockAddrInet(const struct sockaddr_in& addr) { addr_ = addr; }

 private:
  struct sockaddr_in addr_;  ///< 含义：内核 IPv4 地址结构。角色：唯一地址状态。
};

}  // namespace reactor
