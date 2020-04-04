#include <raft-kv/wal/wal.h>
#include <raft-kv/common/log.h>

namespace kv {

WAL_ptr WAL::create(const std::string& dir) {
  return std::make_shared<WAL>(dir);
}

WAL_ptr WAL::open(const std::string& dir, const WAL_Snapshot& snap) {
  return std::make_shared<WAL>(dir);
}

Status WAL::read_all(proto::HardState& hs, std::vector<proto::EntryPtr>& ents) {
  return Status::ok();
}

Status WAL::save(proto::HardState hs, const std::vector<proto::EntryPtr>& ents) {
  return Status::ok();
}

Status WAL::save_snapshot(const WAL_Snapshot& snap) {
  return Status::ok();
}

}
