#pragma once
#include <kvd/raft/Storage.h>
#include <kvd/raft/Unstable.h>

namespace kvd
{

class RaftLog
{
public:

private:
    // storage contains all stable entries since the last snapshot.
    StoragePtr storage_;

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    Unstable unstable_;

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    uint64_t committed;
    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    uint64_t applied_t;

    // max_next_ents_size is the maximum number aggregate byte size of the messages
    // returned from calls to nextEnts.
    uint64_t max_next_ents_size;
};

}
