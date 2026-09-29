#pragma once

#include <cstdint>
#include <string>

namespace reactor {

/// 应用层帧: | Length(4) | Type(2) | Data |
/// Length 为 Type+Data 的字节数（网络字节序）
struct Protocol {
  static const size_t kLengthFieldLen = 4;   ///< 长度字段字节数
  static const size_t kTypeFieldLen = 2;     ///< 类型字段字节数
  static const size_t kHeaderLen = kLengthFieldLen + kTypeFieldLen;  ///< 最小头长度
  static const size_t kMaxBodyLen = 64 * 1024 * 1024;  ///< 单帧 payload 上限

  static uint16_t hostToNetType(uint16_t type);
  static uint16_t netToHostType(uint16_t type);
};

}  // namespace reactor
