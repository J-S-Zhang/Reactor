#pragma once

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

namespace reactor {

class Buffer {
 public:
  static const size_t kInitialSize = 1024;
  static const size_t kCheapPrepend = 8;
  static const size_t kMaxPrepend = 16;

  explicit Buffer(size_t initialSize = kInitialSize)
      : buffer_(kCheapPrepend + initialSize),
        readerIndex_(kCheapPrepend),
        writerIndex_(kCheapPrepend) {}

  size_t readableBytes() const { return writerIndex_ - readerIndex_; }
  size_t writableBytes() const { return buffer_.size() - writerIndex_; }
  size_t prependableBytes() const { return readerIndex_; }

  const char* peek() const { return begin() + readerIndex_; }

  void retrieve(size_t len) {
    if (len < readableBytes()) {
      readerIndex_ += len;
    } else {
      retrieveAll();
    }
  }

  void retrieveAll() {
    readerIndex_ = kCheapPrepend;
    writerIndex_ = kCheapPrepend;
  }

  std::string retrieveAsString(size_t len) {
    std::string result(peek(), len);
    retrieve(len);
    return result;
  }

  std::string retrieveAllAsString() {
    return retrieveAsString(readableBytes());
  }

  void append(const char* data, size_t len) {
    ensureWritableBytes(len);
    std::memcpy(beginWrite(), data, len);
    hasWritten(len);
  }

  void append(const std::string& str) { append(str.data(), str.size()); }

  ssize_t readFd(int fd, int* savedErrno);

  char* beginWrite() { return begin() + writerIndex_; }
  const char* beginWrite() const { return begin() + writerIndex_; }

  void hasWritten(size_t len) { writerIndex_ += len; }

  void ensureWritableBytes(size_t len) {
    if (writableBytes() < len) {
      makeSpace(len);
    }
  }

  int32_t peekInt32() const;
  void appendInt32(int32_t value);

 private:
  char* begin() { return &*buffer_.begin(); }
  const char* begin() const { return &*buffer_.begin(); }

  void makeSpace(size_t len) {
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

  std::vector<char> buffer_;
  size_t readerIndex_;
  size_t writerIndex_;
};

}  // namespace reactor
