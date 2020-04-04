#pragma once
#include <raft-kv/raft/proto.h>
#include <memory>
#include <raft-kv/common/status.h>

namespace kv {

struct WAL_Snapshot {
  uint64_t index;
  uint64_t term;
};

class WAL;
typedef std::shared_ptr<WAL> WAL_ptr;
class WAL {
 public:
  static WAL_ptr create(const std::string& dir);

  static WAL_ptr open(const std::string& dir, const WAL_Snapshot& snap);

  explicit WAL(const std::string& dir) {}

  ~WAL() = default;

  Status read_all(proto::HardState& hs, std::vector<proto::EntryPtr>& ents);

  Status save(proto::HardState hs, const std::vector<proto::EntryPtr>& ents);

  Status save_snapshot(const WAL_Snapshot& snap);

 private:

};

}
