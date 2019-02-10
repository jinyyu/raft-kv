#pragma once

#include <stdint.h>
#include <kvd/raft/RaftLog.h>
#include <kvd/raft/Progress.h>
#include <kvd/raft/proto.h>
#include <kvd/raft/ReadOnly.h>

namespace kvd
{

enum RaftState
{
    Follower = 0,
    Candidate = 1,
    Leader = 2,
    PreCandidate = 3,
};

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
struct SoftState
{
    explicit SoftState(uint64_t lead, RaftState state)
        : lead(lead), state(state)
    {

    }

    uint64_t lead;       // must use atomic operations to access; keep 64-bit aligned.
    RaftState state;
};
typedef std::shared_ptr<SoftState> SoftStatePtr;

class Raft
{
public:
    explicit Raft(const Config& c);

    ~Raft();

    void tick();

    void become_follower(uint64_t term, uint64_t lead);

    RaftLogPtr& raft_log()
    {
        return raft_log_;
    }

    SoftStatePtr soft_state() const;
    proto::HardState hard_state() const;


private:
    uint64_t id_;

    uint64_t term_;
    uint64_t vote_;

    std::vector<ReadState> read_states_;

    // the log
    RaftLogPtr raft_log_;

    uint64_t max_bsg_size_;
    uint64_t max_uncommitted_size_;
    uint64_t max_inflight_;
    std::unordered_map<uint64_t, ProgressPtr> prs_;
    std::unordered_map<uint64_t, ProgressPtr> learner_prs_;
    std::vector<uint64_t> match_buf;

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

    uint64_t check_quorum_;
    bool pre_vote_;

    uint32_t heartbeat_timeout_;
    uint32_t election_timeout_;
    // randomized_election_timeout_ is a random number between
    // [randomized_election_timeout_, 2 * randomized_election_timeout_ - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    uint32_t randomized_election_timeout_;

    bool disable_proposal_forwarding_;

    std::function<void()> tick_;
    std::function<void(proto::Message msg)> step_;
};
typedef std::shared_ptr<Raft> RaftPtr;

}