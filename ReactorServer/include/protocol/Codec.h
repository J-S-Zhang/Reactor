#pragma once

#include <functional>
#include <string>

#include "base/Timestamp.h"
#include "buffer/Buffer.h"
#include "net/TcpConnection.h"

namespace reactor {

class Codec {
 public:
  using MessageCallback =
      std::function<void(const TcpConnectionPtr&, uint16_t type,
                         const std::string& body, Timestamp)>;

  explicit Codec(MessageCallback cb) : messageCallback_(std::move(cb)) {}

  void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp t);
  void send(const TcpConnectionPtr& conn, uint16_t type, const std::string& body);

  static std::string encode(uint16_t type, const std::string& body);

 private:
  MessageCallback messageCallback_;
};

}  // namespace reactor
