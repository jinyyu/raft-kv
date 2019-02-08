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

    // maybeFirstIndex returns the index of the first possible entry in entries
    // if it has a snapshot.
    void maybe_first_index(uint64_t& index, bool& maybe)
    {
        if (snapshot_) {
            maybe = true;
            index = snapshot_->metadata.index + 1;
        }
        else {
            maybe = false;
            index = 0;
        }
    }

    // maybeLastIndex returns the last index if it has at least one
    // unstable entry or snapshot.
    void maybe_last_index(uint64_t& index, bool& maybe)
    {
        if (!entries_.empty()) {
            maybe = true;
            index = offset_ + entries_.size() - 1;
            return;
        }
        if (snapshot_) {
            maybe = true;
            index = snapshot_->metadata.index;
            return;
        }
        index = 0;
        maybe = false;
    }



private:
    // the incoming unstable snapshot, if any.
    proto::SnapshotPtr snapshot_;
    // all entries that have not yet been written to storage.

    std::vector<proto::EntryPtr> entries_;
    uint64_t offset_;
};

}
