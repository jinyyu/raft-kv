#pragma once
#include <raft-kv/raft/proto.h>
#include <memory>
#include <raft-kv/common/status.h>

namespace kv {

class WAL;
typedef std::shared_ptr<WAL> WAL_ptr;
class WAL {
 public:
  explicit WAL(const std::string& dir, const std::vector<uint8_t>& metadata) {}

  ~WAL() = default;

  Status read_all(std::vector<uint8_t>& metadata, proto::HardState& hs, std::vector<proto::EntryPtr>& ents) {
    return Status::ok();
  }

 public:

};

}
