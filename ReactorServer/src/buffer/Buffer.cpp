#include "buffer/Buffer.h"

#include <arpa/inet.h>
#include <errno.h>
#include <sys/uio.h>
#include <unistd.h>

namespace reactor {

/// 做什么：前移 read 索引，消费已处理字节。
/// 项目角色：Codec 解析完一帧后移动读指针。
void Buffer::retrieve(size_t len) {
  if (len < readableBytes()) {
    readerIndex_ += len;
  } else {
    retrieveAll();
  }
}

/// 做什么：读写索引复位到 kCheapPrepend，逻辑清空。
/// 项目角色：连接上数据已全部交给业务后清空 input。
void Buffer::retrieveAll() {
  readerIndex_ = kCheapPrepend;
  writerIndex_ = kCheapPrepend;
}

/// 做什么：拷贝 len 字节到 string 并 retrieve。
/// 项目角色：取协议 body 文本。
std::string Buffer::retrieveAsString(size_t len) {
  std::string result(peek(), len);
  retrieve(len);
  return result;
}

/// 做什么：retrieve 全部可读字节为 string。
/// 项目角色：便捷 API。
std::string Buffer::retrieveAllAsString() {
  return retrieveAsString(readableBytes());
}

/// 做什么：确保空间后 memcpy 到写区。
/// 项目角色：组包、线程池拷贝 input 到临时 Buffer。
void Buffer::append(const char* data, size_t len) {
  ensureWritableBytes(len);
  std::memcpy(beginWrite(), data, len);
  hasWritten(len);
}

/// 做什么：空间不足时 makeSpace。
/// 项目角色：append/readFd 前置条件。
void Buffer::ensureWritableBytes(size_t len) {
  if (writableBytes() < len) {
    makeSpace(len);
  }
}

/// 做什么：扩容或 compact 已有数据到 kCheapPrepend。
/// 项目角色：长时间连接避免频繁 realloc。
void Buffer::makeSpace(size_t len) {
  if (writableBytes() + prependableBytes() < kCheapPrepend + len) {
    buffer_.resize(writerIndex_ + len);
  } else {
    size_t readable = readableBytes();
    std::copy(begin() + readerIndex_, begin() + writerIndex_,
              begin() + kCheapPrepend);
    readerIndex_ = kCheapPrepend;
    writerIndex_ = readerIndex_ + readable;
  }
}

/// 做什么：readv 读 socket，优先填满内部 buffer。
/// 项目角色：TcpConnection ET 模式下一次读尽可能多的数据。
ssize_t Buffer::readFd(int fd, int* savedErrno) {
  char extrabuf[65536];
  struct iovec vec[2];
  const size_t writable = writableBytes();
  vec[0].iov_base = beginWrite();
  vec[0].iov_len = writable;
  vec[1].iov_base = extrabuf;
  vec[1].iov_len = sizeof extrabuf;

  const int iovcnt = (writable < sizeof extrabuf) ? 2 : 1;
  const ssize_t n = readv(fd, vec, iovcnt);
  if (n < 0) {
    *savedErrno = errno;
  } else if (static_cast<size_t>(n) <= writable) {
    writerIndex_ += n;
  } else {
    writerIndex_ = buffer_.size();
    append(extrabuf, n - writable);
  }
  return n;
}

/// 做什么：从 peek 读 4 字节 big-endian int32。
/// 项目角色：Codec 读 Length 字段。
int32_t Buffer::peekInt32() const {
  int32_t be32 = 0;
  std::memcpy(&be32, peek(), sizeof be32);
  return ntohl(be32);
}

/// 做什么：追加网络序 int32。
/// 项目角色：协议编码辅助。
void Buffer::appendInt32(int32_t value) {
  int32_t be32 = htonl(value);
  append(reinterpret_cast<const char*>(&be32), sizeof be32);
}

}  // namespace reactor
