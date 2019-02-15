#pragma once
#include <memory>
#include <mutex>
#include <kvd/common/Status.h>
#include <kvd/raft/proto.h>

namespace kvd
{

class Storage
{
public:
    ~Storage() = default;

    // initial_state returns the saved hard_state and ConfState information.
    virtual Status initial_state(proto::HardState& hard_state, proto::ConfState& conf_state) = 0;

    // entries returns a slice of log entries in the range [low,high).
    // MaxSize limits the total size of the log entries returned, but
    // entries returns at least one entry if any.
    virtual Status entries(uint64_t low,
                           uint64_t high,
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
    // possibly available via entries (older entries have been incorporated
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
class MemoryStorage: public Storage
{
public:

    // creates an empty MemoryStorage
    explicit MemoryStorage()
        : snapshot_(new proto::Snapshot())
    {
        // When starting from scratch populate the list with a dummy entry at term zero.
        proto::EntryPtr entry(new proto::Entry());
        entries_.emplace_back(std::move(entry));
    }

    virtual Status initial_state(proto::HardState& hard_state, proto::ConfState& conf_state);

    virtual Status entries(uint64_t low,
                           uint64_t high,
                           uint64_t max_size,
                           std::vector<proto::EntryPtr>& entries);

    virtual Status term(uint64_t i, uint64_t& term);

    virtual Status last_index(uint64_t& index);

    virtual Status first_index(uint64_t& index);

    virtual Status snapshot(proto::SnapshotPtr& snapshot);

    // compact discards all log entries prior to compact_index.
    // It is the application's responsibility to not attempt to compact an index
    // greater than raftLog.applied.
    Status compact(uint64_t compact_index);

    // append the new entries to storage.
    Status append(std::vector<proto::EntryPtr> entries);

    // create_snapshot makes a snapshot which can be retrieved with Snapshot() and
    // can be used to reconstruct the state at that point.
    // If any configuration changes have been made since the last compaction,
    // the result of the last apply_conf_change must be passed in.
    Status create_snapshot(uint64_t index,
                           proto::ConfStatePtr cs,
                           std::vector<uint8_t> data,
                           proto::SnapshotPtr& snapshot);

    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    Status apply_snapshot(proto::SnapshotPtr snapshot);

    // getter && setter
    std::vector<proto::EntryPtr>& ref_entries()
    {
        return entries_;
    }
    proto::SnapshotPtr& ref_snapshot()
    {
        return snapshot_;
    }

private:
    Status last_index_impl(uint64_t& index);
    Status first_index_impl(uint64_t& index);

    std::mutex mutex_;
    proto::HardState hard_state_;
    proto::SnapshotPtr snapshot_;
    // entries_[i] has raft log position i+snapshot.Metadata.Index
    std::vector<proto::EntryPtr> entries_;
};
typedef std::shared_ptr<MemoryStorage> MemoryStoragePtr;

}
