#pragma once

#include <cstdint>
#include <string>

namespace reactor {

/**
 * @struct Protocol
 * 含义：应用层报文格式常量与类型字节序转换。
 * 项目角色：方案 5.1 粘包协议 | Length(4) + Type(2) + Data | 的代码化；
 *           Codec 编解码均依赖这些常量。
 */
struct Protocol {
  static const size_t kLengthFieldLen = 4;   ///< 含义：长度域字节数。角色：Codec 先读 4 字节。
  static const size_t kTypeFieldLen = 2;     ///< 含义：类型域字节数。角色：区分 echo/chat 等。
  static const size_t kHeaderLen = kLengthFieldLen + kTypeFieldLen;  ///< 含义：长度+类型头。角色：最小可读判断。
  static const size_t kMaxBodyLen = 64 * 1024 * 1024;  ///< 含义：单帧最大 payload。角色：防攻击超长包。

  /// 含义：主机序 type → 网络序。角色：encode 发送。
  static uint16_t hostToNetType(uint16_t type);
  /// 含义：网络序 type → 主机序。角色：onMessage 解析。
  static uint16_t netToHostType(uint16_t type);
};

}  // namespace reactor
