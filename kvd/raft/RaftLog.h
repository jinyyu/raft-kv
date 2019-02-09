#pragma once
#include <kvd/raft/Storage.h>
#include <kvd/raft/Unstable.h>

namespace kvd
{

class RaftLog
{
public:
    explicit RaftLog(StoragePtr storage, uint64_t max_next_ents_size);

    ~RaftLog();

    std::string status_string() const
    {
        char buffer[64];
        int n = snprintf(buffer,
                         sizeof(buffer),
                         "committed=%lu, applied=%lu, unstable.offset=%lu, unstable.entries=%lu",
                         committed_,
                         applied_,
                         unstable_->offset(),
                         unstable_->ref_entries().size());
        return std::string(buffer, n);
    }

    bool match_term(uint64_t index, uint64_t t);

    uint64_t last_term() const;

    Status term(uint64_t index, uint64_t& t) const;

    uint64_t first_index() const;

    uint64_t last_index() const;

private:
    // storage contains all stable entries since the last snapshot.
    StoragePtr storage_;

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    UnstablePtr unstable_;

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    uint64_t committed_;
    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    uint64_t applied_;

    // max_next_ents_size is the maximum number aggregate byte size of the messages
    // returned from calls to nextEnts.
    uint64_t max_next_ents_size_;
};

}
