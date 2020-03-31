#pragma once
#include <memory>
#include <raft-kv/raft/proto.h>
#include <raft-kv/raft/node.h>
#include <raft-kv/common/status.h>

namespace kv {

class RaftServer {
 public:
  virtual ~RaftServer() = default;

  virtual void process(proto::MessagePtr msg, const std::function<void(const Status&)>& callback) = 0;

  virtual void is_id_removed(uint64_t id, const std::function<void(bool)>& callback) = 0;

  virtual void report_unreachable(uint64_t id) = 0;

  virtual void report_snapshot(uint64_t id, SnapshotStatus status) = 0;
};
typedef std::shared_ptr<RaftServer> RaftServerPtr;

class IoServer {
 public:
  virtual void start() = 0;
  virtual void stop() = 0;

  static std::shared_ptr<IoServer> create(void* io_service,
                                          const std::string& host,
                                          RaftServer* raft);
};
typedef std::shared_ptr<IoServer> IoServerPtr;

}