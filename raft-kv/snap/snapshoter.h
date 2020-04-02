#pragma once
#include <string>
#include <memory>
#include <raft-kv/raft/proto.h>
#include <raft-kv/common/status.h>

namespace kv {

class Snapshoter {
 public:
  explicit Snapshoter(const std::string& dir)
      : dir_(dir) {
  };

  ~Snapshoter() {
  }

  Status load(proto::Snapshot& snapshot);

  Status save_snap(const proto::Snapshot& snapshot);

 private:
  void get_snap_names(std::vector<std::string>& names);

  Status load_snap(const std::string& filename, proto::Snapshot& snapshot);

 private:
  std::string dir_;
};

}