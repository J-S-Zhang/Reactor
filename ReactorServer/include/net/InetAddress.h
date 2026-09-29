#pragma once

#include <netinet/in.h>
#include <string>

namespace reactor {

class InetAddress {
 public:
  explicit InetAddress(uint16_t port = 0, bool loopbackOnly = false);
  InetAddress(const std::string& ip, uint16_t port);
  explicit InetAddress(const struct sockaddr_in& addr) : addr_(addr) {}

  sa_family_t family() const { return addr_.sin_family; }
  std::string toIp() const;
  std::string toIpPort() const;
  uint16_t port() const;

  const struct sockaddr* getSockAddr() const {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }
  void setSockAddrInet(const struct sockaddr_in& addr) { addr_ = addr; }

 private:
  struct sockaddr_in addr_;
};

}  // namespace reactor
