#pragma once
#include <kvd/raft/proto.h>

namespace kvd
{


// Unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
class Unstable
{
public:
    explicit Unstable(uint64_t offset)
        : offset_(offset)
    {

    }

private:
    // the incoming unstable snapshot, if any.
    proto::SnapshotPtr snapshot_;
    // all entries that have not yet been written to storage.

    std::vector<proto::EntryPtr> entries_;
    uint64_t offset_;
};

}
