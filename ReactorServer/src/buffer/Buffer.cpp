#include "buffer/Buffer.h"

#include <arpa/inet.h>
#include <errno.h>
#include <sys/uio.h>
#include <unistd.h>

namespace reactor {

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
