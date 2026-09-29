#pragma once

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

namespace reactor {

/**
 * @class Buffer
 * 含义：带 read/write 索引的动态字节缓冲区（类似 Netty/Muduo Buffer）。
 * 项目角色：TcpConnection 的 input/output 载体；Codec 在此做粘包拆包；
 *           readFd 用 readv 减少 syscall，是网络数据路径的核心组件。
 */
class Buffer {
 public:
  static const size_t kInitialSize = 1024;   ///< 含义：初始可写区大小。角色：默认构造容量。
  static const size_t kCheapPrepend = 8;     ///< 含义：头部预留。角色：前移数据时保留空间。
  static const size_t kMaxPrepend = 16;      ///< 含义：预留上限常量。角色：扩展协议头预留。

  /// 含义：分配内部 vector 并初始化读写指针。角色：每连接 input/output 创建。
  explicit Buffer(size_t initialSize = kInitialSize)
      : buffer_(kCheapPrepend + initialSize),
        readerIndex_(kCheapPrepend),
        writerIndex_(kCheapPrepend) {}

  /// 含义：可读字节数。角色：Codec 判断是否够一帧。
  size_t readableBytes() const { return writerIndex_ - readerIndex_; }
  /// 含义：可写字节数（尾部连续空间）。角色：readFd 前判断空间。
  size_t writableBytes() const { return buffer_.size() - writerIndex_; }
  /// 含义：read 指针前可复用空间。角色：makeSpace 前移数据。
  size_t prependableBytes() const { return readerIndex_; }

  /// 含义：指向第一个可读字节。角色：peek 长度头、append 源。
  const char* peek() const { return begin() + readerIndex_; }

  /// 含义：消费 len 字节（移动 read 索引）。角色：解析完一帧后 retrieve。
  void retrieve(size_t len);
  /// 含义：清空可读区（索引复位）。角色：线程池拷贝走数据后清空 input。
  void retrieveAll();

  /// 含义：读出 len 字节为 string 并 retrieve。角色：取 body 字符串。
  std::string retrieveAsString(size_t len);
  /// 含义：读出全部可读为 string。角色：便捷 API。
  std::string retrieveAllAsString();

  /// 含义：追加原始字节。角色：组包、线程池 buf 拷贝。
  void append(const char* data, size_t len);
  void append(const std::string& str) { append(str.data(), str.size()); }

  /// 含义：从 socket 非阻塞读入（readv）。角色：TcpConnection::handleRead 核心。
  ssize_t readFd(int fd, int* savedErrno);

  /// 含义：可写区起始指针。角色：手动写入或 readv 目标。
  char* beginWrite() { return begin() + writerIndex_; }
  const char* beginWrite() const { return begin() + writerIndex_; }

  /// 含义：告知已写入 len 字节（推进 write 索引）。角色：readFd/append 后更新。
  void hasWritten(size_t len) { writerIndex_ += len; }

  /// 含义：保证尾部至少有 len 可写空间。角色：append 前扩容或前移。
  void ensureWritableBytes(size_t len);

  /// 含义：窥视网络序 int32 并转主机序。角色：解析 Length 字段。
  int32_t peekInt32() const;
  /// 含义：追加网络序 int32。角色：协议编码辅助。
  void appendInt32(int32_t value);

 private:
  char* begin() { return &*buffer_.begin(); }
  const char* begin() const { return &*buffer_.begin(); }

  /// 含义：扩容或 compact。角色：空间不足时内部整理。
  void makeSpace(size_t len);

  std::vector<char> buffer_;  ///< 含义：底层存储。角色：所有数据的物理容器。
  size_t readerIndex_;        ///< 含义：读游标。角色：界定可读区间起点。
  size_t writerIndex_;        ///< 含义：写游标。角色：界定可读区间终点/可写起点。
};

}  // namespace reactor
