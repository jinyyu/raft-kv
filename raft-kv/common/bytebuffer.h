#pragma once
#include <vector>
#include <stdint.h>
#include <string.h>
#include <raft-kv/common/slice.h>

namespace kv {

class ByteBuffer {
 public:
  explicit ByteBuffer();

  void put(const uint8_t* data, uint32_t len);

  void read_bytes(uint32_t bytes);

  bool readable() const {
    return writer_ > reader_;
  }

  uint32_t readable_bytes() const;

  uint32_t capacity() const {
    return static_cast<uint32_t>(buff_.capacity());
  }

  const uint8_t* reader() const {
    return buff_.data() + reader_;
  }

  Slice slice() const {
    return Slice((const char*) reader(), readable_bytes());
  }

  void reset();
 private:
  void may_shrink_to_fit();

  uint32_t reader_;
  uint32_t writer_;
  std::vector<uint8_t> buff_;
};

}
