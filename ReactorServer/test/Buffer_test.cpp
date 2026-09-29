/// Buffer 基本读写与整型字段测试
#include <cassert>
#include <iostream>

#include "buffer/Buffer.h"

int main() {
  reactor::Buffer buf;
  buf.append("hello", 5);
  assert(buf.readableBytes() == 5);
  assert(buf.retrieveAsString(5) == "hello");

  buf.appendInt32(42);
  assert(buf.readableBytes() == 4);
  assert(buf.peekInt32() == 42);
  buf.retrieve(4);

  std::cout << "Buffer_test passed\n";
  return 0;
}
