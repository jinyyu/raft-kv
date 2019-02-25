#include <kvd/raft/Ready.h>
#include <kvd/raft/Raft.h>

namespace kvd
{


// must_sync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
static bool is_must_sync(const proto::HardState& st, const proto::HardState& prevst, size_t entsnum)
{
    // Persistent state on all servers:
    // (Updated on stable storage before responding to RPCs)
    // currentTerm
    // votedFor
    // log entries[]
    return entsnum != 0 || st.vote != prevst.vote || st.vote != prevst.vote;
}

Ready::Ready(std::shared_ptr<Raft> raft, SoftStatePtr pre_soft_state, const proto::HardState& pre_hard_state)
    : entries(raft->raft_log_->unstable_entries())
{
    std::swap(this->messages, raft->msgs_);

    raft->raft_log_->next_entries(committed_entries);

    SoftStatePtr st = raft->soft_state();
    if (!st->equal(*pre_soft_state)) {
        this->soft_state = st;
    }

    proto::HardState hs = raft->hard_state();
    if (!hs.equal(pre_hard_state)) {
        this->hard_state = hs;
    }


    proto::SnapshotPtr snapshot = raft->raft_log_->unstable_->snapshot_;
    if (snapshot) {
        //copy
        this->snapshot = *snapshot;
    }
    if (!raft->read_states_.empty()) {
        this->read_states = raft->read_states_;
    }

    this->must_sync = is_must_sync(hs, hard_state, entries.size());
}

bool Ready::contains_updates() const
{
    return soft_state != nullptr || !hard_state.is_empty_state() ||
        !snapshot.is_empty() || !entries.empty() ||
        !committed_entries.empty() || !messages.empty() || read_states.empty();
}

uint64_t Ready::applied_cursor() const
{
    if (!committed_entries.empty()) {
        return committed_entries.back()->index;
    }
    uint64_t index = snapshot.metadata.index;
    if (index > 0) {
        return index;
    }
    return 0;
}

}

