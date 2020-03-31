#pragma once
#include <functional>
#include <stdint.h>
#include <raft-kv/raft/raft_log.h>
#include <raft-kv/raft/progress.h>
#include <raft-kv/raft/proto.h>
#include <raft-kv/raft/readonly.h>
#include <raft-kv/raft/ready.h>
#include <raft-kv/common/random_device.h>

namespace kv {

class Raft {
 public:
  explicit Raft(const Config& c);

  virtual ~Raft();

  void tick();

  void become_follower(uint64_t term, uint64_t lead);

  void become_candidate();

  void become_pre_candidate();

  void become_leader();

  // campaign_type represents the type of campaigning
  // the reason we use the type of string instead of uint64
  // is because it's simpler to compare and fill in raft entries
  void campaign(const std::string& campaign_type);

  uint32_t poll(uint64_t id, proto::MessageType type, bool v);

  virtual Status step(proto::MessagePtr msg);

  Status step_leader(proto::MessagePtr msg);

  // step_candidate is shared by StateCandidate and StatePreCandidate; the difference is
  // whether they respond to MsgVoteResp or MsgPreVoteResp.
  Status step_candidate(proto::MessagePtr msg);

  Status step_follower(proto::MessagePtr msg);

  void handle_append_entries(proto::MessagePtr msg);

  void handle_heartbeat(proto::MessagePtr msg);

  void handle_snapshot(proto::MessagePtr msg);

  bool restore(const proto::Snapshot& snapshot);

  void send(proto::MessagePtr msg);

  void restore_node(const std::vector<uint64_t>& nodes, bool is_learner);

  // promotable indicates whether state machine can be promoted to leader,
  // which is true when its own id is in progress list.
  bool promotable() const;

  void add_node(uint64_t id);

  void add_node_or_learner(uint64_t id, bool is_learner);

  void remove_node(uint64_t id);

  uint32_t quorum() const {
    return static_cast<uint32_t>(prs_.size() / 2 + 1);
  }

  SoftStatePtr soft_state() const;

  proto::HardState hard_state() const;

  void load_state(const proto::HardState& state);

  void nodes(std::vector<uint64_t>& node) const;

  void learner_nodes(std::vector<uint64_t>& learner) const;

  ProgressPtr get_progress(uint64_t id);

  void set_progress(uint64_t id, uint64_t match, uint64_t next, bool is_learner);

  void del_progress(uint64_t id);

  // sendAppend sends an append RPC with new entries (if any) and the
  // current commit index to the given peer.
  void send_append(uint64_t to);

  // maybe_send_append sends an append RPC with new entries to the given peer,
  // if necessary. Returns true if a message was sent. The sendIfEmpty
  // argument controls whether messages with no entries will be sent
  // ("empty" messages are useful to convey updated Commit indexes, but
  // are undesirable when we're sending multiple messages in a batch).
  bool maybe_send_append(uint64_t to, bool send_if_empty);

  // send_heartbeat sends a heartbeat RPC to the given peer.
  void send_heartbeat(uint64_t to, std::vector<uint8_t> ctx);

  void for_each_progress(const std::function<void(uint64_t, ProgressPtr&)>& callback);

  // bcast_append sends RPC, with entries to all peers that are not up-to-date
  // according to the progress recorded in prs_.
  void bcast_append();

  void bcast_heartbeat();

  void bcast_heartbeat_with_ctx(const std::vector<uint8_t>& ctx);

  // maybe_commit attempts to advance the commit index. Returns true if
  // the commit index changed (in which case the caller should call
  // bcast_append).
  bool maybe_commit();

  void reset(uint64_t term);

  bool append_entry(const std::vector<proto::Entry>& entries);

  // tick_election is run by followers and candidates after ElectionTimeout.
  void tick_election();

  void tick_heartbeat();

  // past_election_timeout returns true if r.electionElapsed is greater
  // than or equal to the randomized election timeout in
  // [electiontimeout, 2 * electiontimeout - 1].
  bool past_election_timeout();

  void reset_randomized_election_timeout();

  bool check_quorum_active();

  void send_timeout_now(uint64_t to);

  void abort_leader_transfer();

  // increase_uncommitted_size computes the size of the proposed entries and
  // determines whether they would push leader over its maxUncommittedSize limit.
  // If the new entries would exceed the limit, the method returns false. If not,
  // the increase in uncommitted entry size is recorded and the method returns
  // true.
  bool increase_uncommitted_size(const std::vector<proto::EntryPtr>& entries);

  // reduce_uncommitted_size accounts for the newly committed entries by decreasing
  // the uncommitted entry size limit.
  void reduce_uncommitted_size(const std::vector<proto::EntryPtr>& entries);

  virtual std::vector<proto::MessagePtr> read_messages() {
    std::vector<proto::MessagePtr> ret;
    ret.swap(msgs_);
    msgs_.clear();
    return ret;
  }

 public:
  uint64_t id_;

  uint64_t term_;
  uint64_t vote_;

  std::vector<ReadState> read_states_;

  // the log
  RaftLogPtr raft_log_;

  uint64_t max_msg_size_;
  uint64_t max_uncommitted_size_;
  uint64_t max_inflight_;
  std::unordered_map<uint64_t, ProgressPtr> prs_;
  std::unordered_map<uint64_t, ProgressPtr> learner_prs_;
  std::vector<uint64_t> match_buf_;

  RaftState state_;

  // is_learner_ is true if the local raft node is a learner.
  bool is_learner_;

  std::unordered_map<uint64_t, bool> votes_;

  std::vector<proto::MessagePtr> msgs_;

  // the leader id
  uint64_t lead_;

  // lead_transferee_ is id of the leader transfer target when its value is not zero.
  // Follow the procedure defined in raft thesis 3.10.
  uint64_t lead_transferee_;
  // Only one conf change may be pending (in the log, but not yet
  // applied) at a time. This is enforced via pending_conf_index_, which
  // is set to a value >= the log index of the latest pending
  // configuration change (if any). Config changes are only allowed to
  // be proposed if the leader's applied index is greater than this
  // value.
  uint64_t pending_conf_index_;
  // an estimate of the size of the uncommitted tail of the Raft log. Used to
  // prevent unbounded log growth. Only maintained by the leader. Reset on
  // term changes.
  uint64_t uncommitted_size_;

  ReadOnlyPtr read_only_;

  // number of ticks since it reached last election_elapsed_ when it is leader
  // or candidate.
  // number of ticks since it reached last electionTimeout or received a
  // valid message from current leader when it is a follower.
  uint32_t election_elapsed_;

  // number of ticks since it reached last heartbeat_elapsed_.
  // only leader keeps heartbeatElapsed.
  uint32_t heartbeat_elapsed_;

  bool check_quorum_;
  bool pre_vote_;

  uint32_t heartbeat_timeout_;
  uint32_t election_timeout_;
  // randomized_election_timeout_ is a random number between
  // [randomized_election_timeout_, 2 * randomized_election_timeout_ - 1]. It gets reset
  // when raft changes its state to follower or candidate.
  uint32_t randomized_election_timeout_;

  bool disable_proposal_forwarding_;

  std::function<void()> tick_;
  std::function<Status(proto::MessagePtr)> step_;
  RandomDevice random_device_;
};
typedef std::shared_ptr<Raft> RaftPtr;

}
