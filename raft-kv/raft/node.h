#pragma once
#include <raft-kv/raft/raft.h>
#include <raft-kv/raft/proto.h>
#include <raft-kv/raft/config.h>
#include <raft-kv/raft/raft_status.h>

namespace kv {

typedef uint8_t SnapshotStatus;

static const SnapshotStatus SnapshotFinish = 1;
static const SnapshotStatus SnapshotFailure = 2;

struct PeerContext {
  uint64_t id;
  std::vector<uint8_t> context;
};

class Node {
 public:
  ~Node() = default;

  // tick increments the internal logical clock for the Node by a single tick. Election
  // timeouts and heartbeat timeouts are in units of ticks.
  virtual void tick() = 0;

  // campaign causes the Node to transition to candidate state and start campaigning to become leader.
  virtual Status campaign() = 0;

  // propose proposes that data be appended to the log. Note that proposals can be lost without
  // notice, therefore it is user's job to ensure proposal retries.
  virtual Status propose(std::vector<uint8_t> data) = 0;

  // propose_conf_change proposes config change.
  // At most one ConfChange can be in the process of going through consensus.
  // Application needs to call apply_conf_change when applying EntryConfChange type entry.
  virtual Status propose_conf_change(const proto::ConfChange& cc) = 0;

  // step advances the state machine using the given message. ctx.Err() will be returned, if any.
  virtual Status step(proto::MessagePtr msg) = 0;

  // ready returns the current point-in-time state of this RawNode.
  virtual ReadyPtr ready() = 0;

  // has_ready called when RawNode user need to check if any Ready pending.
  // Checking logic in this method should be consistent with Ready.containsUpdates().
  virtual bool has_ready() = 0;

  // advance notifies the Node that the application has saved progress up to the last ready.
  // It prepares the node to return the next available ready.
  //
  // The application should generally call advance after it applies the entries in last ready.
  //
  // However, as an optimization, the application may call advance while it is applying the
  // commands. For example. when the last ready contains a snapshot, the application might take
  // a long time to apply the snapshot data. To continue receiving ready without blocking raft
  // progress, it can call advance before finishing applying the last ready.
  virtual void advance(ReadyPtr ready) = 0;

  // apply_conf_change applies config change to the local node.
  // Returns an opaque ConfState protobuf which must be recorded
  // in snapshots. Will never return nil; it returns a pointer only
  // to match MemoryStorage.Compact.
  virtual proto::ConfStatePtr apply_conf_change(const proto::ConfChange& cc) = 0;

  // transfer_leadership attempts to transfer leadership to the given transferee.
  virtual void transfer_leadership(uint64_t lead, ino64_t transferee) = 0;

  // read_index request a read state. The read state will be set in the ready.
  // Read state has a read index. Once the application advances further than the read
  // index, any linearizable read requests issued before the read request can be
  // processed safely. The read state will have the same rctx attached.
  virtual Status read_index(std::vector<uint8_t> rctx) = 0;

  // raft_status returns the current status of the raft state machine.
  virtual RaftStatusPtr raft_status() = 0;

  // report_unreachable reports the given node is not reachable for the last send.
  virtual void report_unreachable(uint64_t id) = 0;

  // report_snapshot reports the status of the sent snapshot. The id is the raft ID of the follower
  // who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
  // Calling report_snapshot with SnapshotFinish is a no-op. But, any failure in applying a
  // snapshot (for e.g., while streaming it from leader to follower), should be reported to the
  // leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
  // log probes until the follower can apply the snapshot and advance its state. If the follower
  // can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
  // updates from the leader. Therefore, it is crucial that the application ensures that any
  // failure in snapshot sending is caught and reported back to the leader; so it can resume raft
  // log probing in the follower.
  virtual void report_snapshot(uint64_t id, SnapshotStatus status) = 0;

  // stop performs any necessary termination of the Node.
  virtual void stop() = 0;

  // start_node returns a new Node given configuration and a list of raft peers.
  // It appends a ConfChangeAddNode entry for each given peer to the initial log.
  static Node* start_node(const Config& conf, const std::vector<PeerContext>& peers);

  // restart_node is similar to start_node but does not take a list of peers.
  // The current membership of the cluster will be restored from the Storage.
  // If the caller has an existing state machine, pass in the last log index that
  // has been applied to it; otherwise use zero.
  static Node* restart_node(const Config& conf);
};

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
class RawNode : public Node {
 public:
  explicit RawNode(const Config& conf, const std::vector<PeerContext>& peers);
  explicit RawNode(const Config& conf);

  ~RawNode() = default;

  void tick() final;
  Status campaign() final;
  Status propose(std::vector<uint8_t> data) final;
  Status propose_conf_change(const proto::ConfChange& cc) final;
  Status step(proto::MessagePtr msg) final;
  ReadyPtr ready() final;
  bool has_ready() final;
  void advance(ReadyPtr rd) final;
  proto::ConfStatePtr apply_conf_change(const proto::ConfChange& cc) final;
  void transfer_leadership(uint64_t lead, ino64_t transferee) final;
  Status read_index(std::vector<uint8_t> rctx) final;
  RaftStatusPtr raft_status() final;
  void report_unreachable(uint64_t id) final;
  void report_snapshot(uint64_t id, SnapshotStatus status) final;
  void stop() final;
 public:
  RaftPtr raft_;
  SoftStatePtr prev_soft_state_;
  proto::HardState prev_hard_state_;
};

}