#pragma once

#include <functional>
#include <string>

#include "base/Timestamp.h"
#include "buffer/Buffer.h"
#include "net/TcpConnection.h"

namespace reactor {

/**
 * @class Codec
 * 含义：在 TcpConnection 的 Buffer 流上拆包/组包 Protocol 帧。
 * 项目角色：应用层与 TCP 字节流之间的适配器；echo/chat 示例的核心协议层，
 *           演示方案「Buffer + 长度头」解决粘包。
 */
class Codec {
 public:
  using MessageCallback =
      std::function<void(const TcpConnectionPtr&, uint16_t type,
                         const std::string& body, Timestamp)>;

  /// 含义：绑定完整消息回调。角色：构造时传入 echo/chat 逻辑。
  explicit Codec(MessageCallback cb) : messageCallback_(std::move(cb)) {}

  /// 含义：从 buf 循环解析完整帧并回调。角色：TcpServer messageCallback 内调用。
  void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp t);
  /// 含义：编码并 send。角色：业务发包的便捷方法。
  void send(const TcpConnectionPtr& conn, uint16_t type, const std::string& body);

  /// 含义：仅组包不发送。角色：echo 回显、chat broadcast。
  static std::string encode(uint16_t type, const std::string& body);

 private:
  MessageCallback messageCallback_;  ///< 含义：一帧就绪后的用户处理。角色：业务逻辑入口。
};

}  // namespace reactor
