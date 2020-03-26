#pragma once
#include <raft-kv/raft/proto.h>
#include <raft-kv/raft/ReadOnly.h>

namespace kv
{

enum RaftState
{
    Follower = 0,
    Candidate = 1,
    Leader = 2,
    PreCandidate = 3,
};

// soft_state provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
struct SoftState
{
    explicit SoftState(uint64_t lead, RaftState state)
        : lead(lead), state(state)
    {

    }

    bool equal(const SoftState& ss) const
    {
        return lead == ss.lead && state == ss.state;
    }

    uint64_t lead;       // must use atomic operations to access; keep 64-bit aligned.
    RaftState state;
};
typedef std::shared_ptr<SoftState> SoftStatePtr;
class Raft;

struct Ready
{
    Ready()
        : must_sync(false)
    {}

    explicit Ready(std::shared_ptr<Raft> raft, SoftStatePtr pre_soft_state, const proto::HardState& pre_hard_state);

    bool contains_updates() const;

    bool equal(const Ready& rd) const;

    // applied_cursor extracts from the Ready the highest index the client has
    // applied (once the Ready is confirmed via Advance). If no information is
    // contained in the Ready, returns zero.
    uint64_t applied_cursor() const;

    // The current volatile state of a Node.
    // soft_state will be nil if there is no update.
    // It is not required to consume or store soft_state.
    SoftStatePtr soft_state;

    // The current state of a Node to be saved to stable storage BEFORE
    // messages are sent.
    // hard_state will be equal to empty state if there is no update.
    proto::HardState hard_state;

    // read_states can be used for node to serve linearizable read requests locally
    // when its applied index is greater than the index in ReadState.
    // Note that the readState will be returned when raft receives msgReadIndex.
    // The returned is only valid for the request that requested to read.
    std::vector<ReadState> read_states;

    // entries specifies entries to be saved to stable storage BEFORE
    // messages are sent
    std::vector<proto::EntryPtr> entries;

    // Snapshot specifies the snapshot to be saved to stable storage.
    proto::Snapshot snapshot;

    // committed_entries specifies entries to be committed to a
    // store/state-machine. These have previously been committed to stable
    // store.
    std::vector<proto::EntryPtr> committed_entries;

    // messages specifies outbound messages to be sent AFTER entries are
    // committed to stable storage.
    // If it contains a MsgSnap message, the application MUST report back to raft
    // when the snapshot has been received or has failed by calling ReportSnapshot.
    std::vector<proto::MessagePtr> messages;

    // must_sync indicates whether the hard_state and entries must be synchronously
    // written to disk or if an asynchronous write is permissible.
    bool must_sync;
};
typedef std::shared_ptr<Ready> ReadyPtr;

}
