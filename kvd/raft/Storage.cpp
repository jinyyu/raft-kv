#include "kvd/raft/Storage.h"
#include "kvd/common/log.h"
#include "kvd/raft/util.h"
namespace kvd
{


Status MemoryStorage::initial_state(proto::HardState& hard_state, proto::ConfState& conf_state)
{
    hard_state = hard_state_;

    // copy
    conf_state = snapshot_->metadata.conf_state;
    return Status::ok();
}

Status MemoryStorage::entries(uint64_t low,
                              uint64_t high,
                              uint64_t max_size,
                              std::vector<proto::EntryPtr>& entries)
{
    assert(low < high);
    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t offset = entries_[0]->index;
    if (low <= offset) {
        return Status::invalid_argument("requested index is unavailable due to compaction");
    }
    uint64_t last = 0;
    this->last_index_impl(last);

    if (high > last + 1) {
        LOG_FATAL("entries' hi(%lu) is out of bound lastindex(%lu)", high, last);
    }
    // only contains dummy entries.
    if (entries_.size() == 1) {
        return Status::invalid_argument("requested entry at index is unavailable");
    }


    for (uint64_t i = low - offset; i < high - offset; ++i) {
        entries.push_back(entries_[i]);
    }
    entry_limit_size(max_size, entries);
    return Status::ok();
}

Status MemoryStorage::term(uint64_t i, uint64_t& term)
{
    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t offset = entries_[0]->index;

    if (i < offset) {
        return Status::invalid_argument("requested index is unavailable due to compaction");
    }

    if (i - offset >= entries_.size()) {
        return Status::invalid_argument("requested entry at index is unavailable");
    }
    term = entries_[i - offset]->term;
    return Status::ok();
}

Status MemoryStorage::last_index(uint64_t& index)
{
    std::lock_guard<std::mutex> guard(mutex_);
    return last_index_impl(index);
}

Status MemoryStorage::first_index(uint64_t& index)
{
    std::lock_guard<std::mutex> guard(mutex_);
    return first_index_impl(index);
}

Status MemoryStorage::snapshot(proto::SnapshotPtr& snapshot)
{
    std::lock_guard<std::mutex> guard(mutex_);
    snapshot = snapshot_;
    return Status::ok();
}

Status MemoryStorage::compact(uint64_t compact_index)
{
    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t offset = entries_[0]->index;

    if (compact_index <= offset) {
        return Status::invalid_argument("requested index is unavailable due to compaction");
    }

    uint64_t last_idx;
    this->last_index_impl(last_idx);
    if (compact_index > last_idx) {
        LOG_FATAL("compact %lu is out of bound lastindex(%lu)", compact_index, last_idx);
    }

    uint64_t i = compact_index - offset;
    entries_[0]->index = entries_[i]->index;
    entries_[0]->term = entries_[i]->term;

    entries_.erase(entries_.begin() + 1, entries_.begin() + i + 1);
    return Status::ok();
}

Status MemoryStorage::append(std::vector<proto::EntryPtr> entries)
{
    if (entries.empty()) {
        return Status::ok();
    }

    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t first = 0;
    first_index_impl(first);
    uint64_t last = entries[0]->index + entries.size() - 1;

    // shortcut if there is no new entry.
    if (last < first) {
        return Status::ok();
    }

    // truncate compacted entries
    if (first > entries[0]->index) {
        uint64_t n = first - entries[0]->index;
        // first 之前的 entry 已经进入 snapshot, 丢弃
        entries.erase(entries.begin(), entries.begin() + n);
    }

    uint64_t offset = entries[0]->index - entries_[0]->index;

    if (entries_.size() > offset) {
        //MemoryStorage [first, offset] 被保留, offset 之后的丢弃
        entries_.erase(entries_.begin() + offset, entries_.end());
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    }
    else if (entries_.size() == offset) {
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    }
    else {
        uint64_t last_idx;
        last_index_impl(last_idx);
        LOG_FATAL("missing log entry [last: %lu, append at: %lu", last_idx, entries[0]->index);
    }
    return Status::ok();
}

Status MemoryStorage::create_snapshot(uint64_t index,
                                      proto::ConfStatePtr cs,
                                      std::vector<uint8_t> data,
                                      proto::SnapshotPtr& snapshot)
{
    std::lock_guard<std::mutex> guard(mutex_);

    if (index <= snapshot_->metadata.index) {
        snapshot = std::make_shared<proto::Snapshot>();
        return Status::invalid_argument("requested index is older than the existing snapshot");
    }

    uint64_t offset = entries_[0]->index;
    uint64_t last = 0;
    last_index_impl(last);
    if (index > last) {
        LOG_FATAL("snapshot %lu is out of bound lastindex(%lu)", index, last);
    }

    snapshot_->metadata.index = index;
    snapshot_->metadata.term = entries_[index - offset]->term;
    if (cs) {
        snapshot_->metadata.conf_state = *cs;
    }
    snapshot_->data = std::make_shared<std::vector<uint8_t>>(std::move(data));
    snapshot = snapshot_;
    return Status::ok();

}

Status MemoryStorage::apply_snapshot(proto::SnapshotPtr snapshot)
{
    assert(snapshot);

    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t index = snapshot_->metadata.index;
    uint64_t snap_index = snapshot->metadata.index;

    if (index >= snap_index) {
        return Status::invalid_argument("requested index is older than the existing snapshot");
    }

    snapshot_ = std::move(snapshot);

    entries_.resize(1);
    proto::EntryPtr entry(new proto::Entry());
    entry->term = snapshot_->metadata.term;
    entry->index = snapshot_->metadata.index;
    entries_[0] = std::move(entry);
    return Status::ok();
}

Status MemoryStorage::last_index_impl(uint64_t& index)
{
    index = entries_[0]->index + entries_.size() - 1;
    return Status::ok();
}

Status MemoryStorage::first_index_impl(uint64_t& index)
{
    index = entries_[0]->index + 1;
    return Status::ok();
}

}