#include "buffer/Buffer.h"

#include <arpa/inet.h>
#include <errno.h>
#include <sys/uio.h>
#include <unistd.h>

namespace reactor {

void Buffer::retrieve(size_t len) {
  if (len < readableBytes()) {
    readerIndex_ += len;
  } else {
    retrieveAll();
  }
}

void Buffer::retrieveAll() {
  readerIndex_ = kCheapPrepend;
  writerIndex_ = kCheapPrepend;
}

std::string Buffer::retrieveAsString(size_t len) {
  std::string result(peek(), len);
  retrieve(len);
  return result;
}

std::string Buffer::retrieveAllAsString() {
  return retrieveAsString(readableBytes());
}

void Buffer::append(const char* data, size_t len) {
  ensureWritableBytes(len);
  std::memcpy(beginWrite(), data, len);
  hasWritten(len);
}

void Buffer::ensureWritableBytes(size_t len) {
  if (writableBytes() < len) {
    makeSpace(len);
  }
}

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

/// 使用 readv：先写满内部 buffer，剩余读入栈上 extrabuf 再 append
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

int32_t Buffer::peekInt32() const {
  int32_t be32 = 0;
  std::memcpy(&be32, peek(), sizeof be32);
  return ntohl(be32);
}

void Buffer::appendInt32(int32_t value) {
  int32_t be32 = htonl(value);
  append(reinterpret_cast<const char*>(&be32), sizeof be32);
}

}  // namespace reactor
