#include "protocol/Codec.h"

#include <arpa/inet.h>
#include <cstring>

#include "protocol/Protocol.h"

namespace reactor {

namespace {
/// 做什么：缓冲区前 4 字节转主机序 int32。
/// 项目角色：解析 Length 字段。
int32_t asInt32(const char* buf) {
  int32_t be32 = 0;
  std::memcpy(&be32, buf, sizeof be32);
  return ntohl(be32);
}

/// 做什么：缓冲区 2 字节转主机序 uint16。
/// 项目角色：解析 Type 字段。
uint16_t asInt16(const char* buf) {
  uint16_t be16 = 0;
  std::memcpy(&be16, buf, sizeof be16);
  return ntohs(be16);
}
}  // namespace

uint16_t Protocol::hostToNetType(uint16_t type) { return htons(type); }

uint16_t Protocol::netToHostType(uint16_t type) { return ntohs(type); }

/// 做什么：循环从 buf 切完整帧并 messageCallback_。
/// 项目角色：TcpServer messageCallback 内入口，解决 TCP 粘包。
void Codec::onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp t) {
  while (buf->readableBytes() >= Protocol::kHeaderLen) {
    int32_t len = asInt32(buf->peek());
    if (len < static_cast<int32_t>(Protocol::kTypeFieldLen) ||
        len > static_cast<int32_t>(Protocol::kMaxBodyLen)) {
      conn->shutdown();
      break;
    }
    if (buf->readableBytes() >= Protocol::kLengthFieldLen + len) {
      buf->retrieve(Protocol::kLengthFieldLen);
      uint16_t type = asInt16(buf->peek());
      buf->retrieve(Protocol::kTypeFieldLen);
      std::string body(buf->peek(), len - Protocol::kTypeFieldLen);
      buf->retrieve(len - Protocol::kTypeFieldLen);
      if (messageCallback_) {
        messageCallback_(conn, type, body, t);
      }
    } else {
      break;
    }
  }
}

/// 做什么：组 Length+Type+Data 二进制串。
/// 项目角色：echo/chat 发包与广播。
std::string Codec::encode(uint16_t type, const std::string& body) {
  int32_t len = static_cast<int32_t>(Protocol::kTypeFieldLen + body.size());
  int32_t be32 = htonl(len);
  uint16_t be16 = htons(type);
  std::string result;
  result.append(reinterpret_cast<const char*>(&be32), sizeof be32);
  result.append(reinterpret_cast<const char*>(&be16), sizeof be16);
  result.append(body);
  return result;
}

/// 做什么：encode 后 conn->send。
/// 项目角色：业务层一行发送协议消息。
void Codec::send(const TcpConnectionPtr& conn, uint16_t type,
                 const std::string& body) {
  conn->send(encode(type, body));
}

}  // namespace reactor
