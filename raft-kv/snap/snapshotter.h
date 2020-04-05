#pragma once
#include <string>
#include <memory>
#include <raft-kv/raft/proto.h>
#include <raft-kv/common/status.h>

namespace kv {

class Snapshotter {
 public:
  explicit Snapshotter(const std::string& dir)
      : dir_(dir) {
  }

  ~Snapshotter() = default;

  Status load(proto::Snapshot& snapshot);

  Status save_snap(const proto::Snapshot& snapshot);

  static std::string snap_name(uint64_t term, uint64_t index);

 private:
  void get_snap_names(std::vector<std::string>& names);

  Status load_snap(const std::string& filename, proto::Snapshot& snapshot);

 private:
  std::string dir_;
};

}