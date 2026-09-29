#pragma once

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

namespace reactor {

/// 网络读写动态缓冲区：读/写索引 + 可选前预留区，支持粘包拆包
class Buffer {
 public:
  static const size_t kInitialSize = 1024;   ///< 默认可写区初始大小
  static const size_t kCheapPrepend = 8;     ///< 默认前预留字节（便于追加长度头）
  static const size_t kMaxPrepend = 16;      ///< 前预留上限（预留扩展用）

  explicit Buffer(size_t initialSize = kInitialSize)
      : buffer_(kCheapPrepend + initialSize),
        readerIndex_(kCheapPrepend),
        writerIndex_(kCheapPrepend) {}

  size_t readableBytes() const { return writerIndex_ - readerIndex_; }
  size_t writableBytes() const { return buffer_.size() - writerIndex_; }
  size_t prependableBytes() const { return readerIndex_; }

  const char* peek() const { return begin() + readerIndex_; }

  void retrieve(size_t len);
  void retrieveAll();

  std::string retrieveAsString(size_t len);
  std::string retrieveAllAsString();

  void append(const char* data, size_t len);
  void append(const std::string& str) { append(str.data(), str.size()); }

  /// 从 fd 读入数据（readv 优化），失败时 *savedErrno 为 errno
  ssize_t readFd(int fd, int* savedErrno);

  char* beginWrite() { return begin() + writerIndex_; }
  const char* beginWrite() const { return begin() + writerIndex_; }

  void hasWritten(size_t len) { writerIndex_ += len; }

  void ensureWritableBytes(size_t len);

  /// 窥视网络序 int32 并转主机序
  int32_t peekInt32() const;
  void appendInt32(int32_t value);

 private:
  char* begin() { return &*buffer_.begin(); }
  const char* begin() const { return &*buffer_.begin(); }

  /// 扩容或前移可读数据以腾出 writable 空间
  void makeSpace(size_t len);

  std::vector<char> buffer_;  ///< 底层字节存储
  size_t readerIndex_;        ///< 可读数据起始下标
  size_t writerIndex_;        ///< 可写数据起始下标
};

}  // namespace reactor
