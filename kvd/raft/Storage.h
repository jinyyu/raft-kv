#pragma once
#include <kvd/common/Status.h>
#include <kvd/raft/proto.h>
#include <memory>

namespace kvd
{

class Storage
{
public:
    ~Storage() = default;

    // initial_state returns the saved HardState and ConfState information.
    virtual Status initial_state(proto::HardState& hard_state, proto::ConfState& conf_state) = 0;

    // entries returns a slice of log entries in the range [lo,hi).
    // MaxSize limits the total size of the log entries returned, but
    // Entries returns at least one entry if any.
    virtual Status entries(uint32_t low,
                           uint32_t high,
                           uint64_t max_size,
                           std::vector<proto::EntryPtr>& entries) = 0;

    // Term returns the term of entry i, which must be in the range
    // [FirstIndex()-1, LastIndex()]. The term of the entry before
    // FirstIndex is retained for matching purposes even though the
    // rest of that entry may not be available.
    virtual Status term(uint64_t i, uint64_t& term) = 0;

    // LastIndex returns the index of the last entry in the log.
    virtual Status last_index(uint64_t& index) = 0;

    // firstIndex returns the index of the first log entry that is
    // possibly available via Entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    virtual Status first_index(uint64_t& index) = 0;

    // Snapshot returns the most recent snapshot.
    // If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
    // so raft state machine could know that Storage needs some time to prepare
    // snapshot and call Snapshot later.
    virtual Status snapshot(proto::SnapshotPtr& snapshot) = 0;
};
typedef std::shared_ptr<Storage> StoragePtr;

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
class MemoryStorage : public Storage
{

public:
    virtual Status initial_state(proto::HardState& hard_state, proto::ConfState& conf_state);

    virtual Status entries(uint32_t low,
                           uint32_t high,
                           uint64_t max_size,
                           std::vector<proto::EntryPtr>& entries);

    virtual Status term(uint64_t i, uint64_t& term);

    virtual Status last_index(uint64_t& index);

    virtual Status first_index(uint64_t& index);

    virtual Status snapshot(proto::SnapshotPtr& snapshot);
private:
    proto::HardState hard_state_;
    proto::SnapshotPtr snapshot_;
    // ents[i] has raft log position i+snapshot.Metadata.Index
    std::vector<proto::EntryPtr> ents_;
};

}
