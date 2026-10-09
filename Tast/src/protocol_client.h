#pragma once

#include <arpa/inet.h>
#include <cstring>
#include <string>
#include <unistd.h>

#include "protocol/Codec.h"

namespace tast {

/// 从 fd 读取一帧完整 Codec 包；成功返回 true，对端关闭返回 false
inline bool recvFrame(int fd, uint16_t* type, std::string* body) {
  char lenBuf[4];
  ssize_t n = 0;
  size_t got = 0;
  while (got < 4) {
    n = ::read(fd, lenBuf + got, 4 - got);
    if (n <= 0) return false;
    got += static_cast<size_t>(n);
  }
  int32_t be32 = 0;
  std::memcpy(&be32, lenBuf, 4);
  int32_t len = ntohl(be32);
  if (len < 2 || len > static_cast<int32_t>(reactor::Protocol::kMaxBodyLen)) {
    return false;
  }
  std::string payload;
  payload.resize(static_cast<size_t>(len));
  got = 0;
  while (got < payload.size()) {
    n = ::read(fd, &payload[got], payload.size() - got);
    if (n <= 0) return false;
    got += static_cast<size_t>(n);
  }
  uint16_t be16 = 0;
  std::memcpy(&be16, payload.data(), 2);
  *type = ntohs(be16);
  *body = payload.substr(2);
  return true;
}

inline bool sendFrame(int fd, uint16_t type, const std::string& body) {
  std::string packet = reactor::Codec::encode(type, body);
  const char* p = packet.data();
  size_t left = packet.size();
  while (left > 0) {
    ssize_t n = ::write(fd, p, left);
    if (n <= 0) return false;
    p += n;
    left -= static_cast<size_t>(n);
  }
  return true;
}

}  // namespace tast
