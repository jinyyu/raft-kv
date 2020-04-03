#pragma once
#include <raft-kv/raft/proto.h>

namespace kv {

class WAL {
 public:
  explicit WAL(const std::string& dir, const std::vector<uint8_t>& metadata) {}

  ~WAL() = default;

 public:

};

}
