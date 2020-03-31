#pragma once
#include <stdint.h>
#include <vector>
#include <raft-kv/raft/storage.h>

namespace kv {

enum ReadOnlyOption {
  // ReadOnlySafe guarantees the linearizability of the read only request by
  // communicating with the quorum. It is the default and suggested option.
  ReadOnlySafe = 0,

  // ReadOnlyLeaseBased ensures linearizability of the read only request by
  // relying on the leader lease. It can be affected by clock drift.
  // If the clock drift is unbounded, leader might keep the lease longer than it
  // should (clock can move backward/pause without any bound). ReadIndex is not safe
  // in that case.
  ReadOnlyLeaseBased = 1,
};

// Config contains the parameters to start a raft.
struct Config {
  explicit Config()
      : id(0),
        election_tick(0),
        heartbeat_tick(0),
        applied(0),
        max_size_per_msg(0),
        max_committed_size_per_ready(0),
        max_uncommitted_entries_size(0),
        max_inflight_msgs(0),
        check_quorum(false),
        pre_vote(false),
        read_only_option(ReadOnlySafe),
        disable_proposal_forwarding(false) {}

  // id is the identity of the local raft. ID cannot be 0.
  uint64_t id;

  // peers contains the IDs of all nodes (including self) in the raft cluster. It
  // should only be set when starting a new raft cluster. Restarting raft from
  // previous configuration will panic if peers is set. peer is private and only
  // used for testing right now.
  std::vector<uint64_t> peers;

  // learners contains the IDs of all learner nodes (including self if the
  // local node is a learner) in the raft cluster. learners only receives
  // entries from the leader node. It does not vote or promote itself.
  std::vector<uint64_t> learners;

  // election_tick is the number of Node.tick invocations that must pass between
  // elections. That is, if a follower does not receive any message from the
  // leader of current term before election_tick has elapsed, it will become
  // candidate and start an election. election_tick must be greater than
  // heartbeat_tick. We suggest election_tick = 10 * heartbeat_tick to avoid
  // unnecessary leader switching.
  uint32_t election_tick;
  // heartbeat_tick is the number of Node.tick invocations that must pass between
  // heartbeats. That is, a leader sends heartbeat messages to maintain its
  // leadership every heartbeat_tick ticks.
  uint32_t heartbeat_tick;

  // storage is the storage for raft. raft generates entries and states to be
  // stored in storage. raft reads the persisted entries and states out of
  // Storage when it needs. raft reads out the previous state and configuration
  // out of storage when restarting.
  StoragePtr storage;
  // applied is the last applied index. It should only be set when restarting
  // raft. raft will not return entries to the application smaller or equal to
  // Applied. If Applied is unset when restarting, raft might return previous
  // applied entries. This is a very application dependent configuration.
  uint64_t applied;

  // max_size_per_msg limits the max byte size of each append message. Smaller
  // value lowers the raft recovery cost(initial probing and message lost
  // during normal operation). On the other side, it might affect the
  // throughput during normal replication. Note: math.MaxUint64 for unlimited,
  // 0 for at most one entry per message.
  uint64_t max_size_per_msg;
  // max_committed_size_per_ready limits the size of the committed entries which
  // can be applied.
  uint64_t max_committed_size_per_ready;
  // max_uncommitted_entries_size limits the aggregate byte size of the
  // uncommitted entries that may be appended to a leader's log. Once this
  // limit is exceeded, proposals will begin to return ErrProposalDropped
  // errors. Note: 0 for no limit.
  uint64_t max_uncommitted_entries_size;
  // max_inflight_msgs limits the max number of in-flight append messages during
  // optimistic replication phase. The application transportation layer usually
  // has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
  // overflowing that sending buffer.
  uint64_t max_inflight_msgs;

  // check_quorum specifies if the leader should check quorum activity. Leader
  // steps down when quorum is not active for an election_timeout.
  bool check_quorum;

  // pre_vote enables the Pre-Vote algorithm described in raft thesis section
  // 9.6. This prevents disruption when a node that has been partitioned away
  // rejoins the cluster.
  bool pre_vote;

  // read_only_option specifies how the read only request is processed.
  //
  // ReadOnlySafe guarantees the linearizability of the read only request by
  // communicating with the quorum. It is the default and suggested option.
  //
  // ReadOnlyLeaseBased ensures linearizability of the read only request by
  // relying on the leader lease. It can be affected by clock drift.
  // If the clock drift is unbounded, leader might keep the lease longer than it
  // should (clock can move backward/pause without any bound). read_index is not safe
  // in that case.
  // CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
  ReadOnlyOption read_only_option;

  // disable_proposal_forwarding set to true means that followers will drop
  // proposals, rather than forwarding them to the leader. One use case for
  // this feature would be in a situation where the Raft leader is used to
  // compute the data of a proposal, for example, adding a timestamp from a
  // hybrid logical clock to data in a monotonically increasing way. Forwarding
  // should be disabled to prevent a follower with an inaccurate hybrid
  // logical clock from assigning the timestamp and then forwarding the data
  // to the leader.
  bool disable_proposal_forwarding;

  Status validate();
};

}