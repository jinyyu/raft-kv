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
    void maybe_first_index(uint64_t& index, bool& ok);

    // maybeLastIndex returns the last index if it has at least one
    // unstable entry or snapshot.
    void maybe_last_index(uint64_t& index, bool& ok);

    // maybeTerm returns the term of the entry at index i, if there
    // is any.
    void maybe_term(uint64_t index, uint64_t& term, bool& ok);

    void stable_to(uint64_t index, uint64_t term);

    void stable_snap_to(uint64_t index);

    void restore(proto::SnapshotPtr snapshot);

    void truncate_and_append(std::vector<proto::EntryPtr> entries);

    // getter && setter
    proto::SnapshotPtr& ref_snapshot()
    {
        return snapshot_;
    }
    std::vector<proto::EntryPtr>& ref_entries()
    {
        return entries_;
    }

    uint64_t offset() const
    {
        return offset_;
    }

private:
    // the incoming unstable snapshot, if any.
    proto::SnapshotPtr snapshot_;
    // all entries that have not yet been written to storage.

    std::vector<proto::EntryPtr> entries_;
    uint64_t offset_;
};
typedef std::shared_ptr<Unstable> UnstablePtr;

}
