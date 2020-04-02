#pragma once
#include <string>
#include <stdint.h>
#include <memory>
#include <vector>
#include <raft-kv/transport/transport.h>
#include <raft-kv/raft/node.h>
#include <raft-kv/server/redis_store.h>
#include <raft-kv/wal/WAL.h>
#include <raft-kv/snap/snapshoter.h>

namespace kv {

typedef std::function<void(const Status&)> StatusCallback;

class RaftNode : public RaftServer {
 public:
  static void main(uint64_t id, const std::string& cluster, uint16_t port);

  explicit RaftNode(uint64_t id, const std::string& cluster, uint16_t port);

  ~RaftNode() final;

  void stop();

  void propose(std::shared_ptr<std::vector<uint8_t>> data, const StatusCallback& callback);

  void process(proto::MessagePtr msg, const StatusCallback& callback) final;

  void is_id_removed(uint64_t id, const std::function<void(bool)>& callback) final;

  void report_unreachable(uint64_t id) final;

  void report_snapshot(uint64_t id, SnapshotStatus status) final;

  bool publish_entries(const std::vector<proto::EntryPtr>& entries);
  void entries_to_apply(const std::vector<proto::EntryPtr>& entries, std::vector<proto::EntryPtr>& ents);
  void maybe_trigger_snapshot();

 private:
  void start_timer();
  void pull_ready_events();
  void save_snap(const proto::Snapshot& snap);
  void publish_snapshot(const proto::Snapshot& snap);

  // recover from snapshot and WAL
  void recovery();

  // replay_WAL replays WAL entries into the raft instance.
  void replay_WAL();
  void schedule();

  uint16_t port_;
  pthread_t pthread_id_;
  boost::asio::io_service io_service_;
  boost::asio::deadline_timer timer_;
  uint64_t id_;
  std::vector<std::string> peers_;
  uint64_t last_index_;
  proto::ConfStatePtr conf_state_;
  uint64_t snapshot_index_;
  uint64_t applied_index_;

  RawNodePtr node_;
  TransporterPtr transport_;
  MemoryStoragePtr storage_;
  std::shared_ptr<RedisStore> redis_server_;
  std::unique_ptr<Snapshoter> snapshoter_;
};
typedef std::shared_ptr<RaftNode> RaftNodePtr;

}