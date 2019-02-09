#include "kvd/raft/RaftLog.h"
#include "kvd/common/log.h"
#include "kvd/raft/util.h"
namespace kvd
{

RaftLog::RaftLog(StoragePtr storage, uint64_t max_next_ents_size)
    : storage_(std::move(storage)),
      max_next_ents_size_(max_next_ents_size)
{
    assert(storage_);
    uint64_t first;
    auto status = storage_->first_index(first);
    assert(status.is_ok());

    uint64_t last;
    status = storage_->last_index(last);
    assert(status.is_ok());

    unstable_ = std::make_shared<Unstable>(last + 1);

    // Initialize our committed and applied pointers to the time of the last compaction.
    applied_ = committed_ = first - 1;
}
RaftLog::~RaftLog()
{

}

void RaftLog::maybe_append(uint64_t index,
                           uint64_t log_term,
                           uint64_t committed,
                           std::vector<proto::Entry> entries,
                           uint64_t& last_new_index,
                           bool& ok)
{
    if (match_term(index, log_term)) {
        uint64_t lastnewi = index + entries.size();
        uint64_t ci = find_conflict(entries);
        if (ci <= committed_) {
            LOG_ERROR("entry %lu conflict with committed entry [committed(%lu)]", ci, committed_);
            assert(false);
        }
        else if (ci > 0) {
            uint64_t offset = index + 1;
            uint64_t n = ci - offset;
            entries.erase(entries.begin(), entries.begin() + n);
            append(std::move(entries));
        }

        commit_to(std::min(committed, lastnewi));

        last_new_index = lastnewi;
        ok = true;

        return;
    }
    else {
        last_new_index = 0;
        ok = false;
    }
}

uint64_t RaftLog::append(std::vector<proto::Entry> entries)
{
    if (entries.empty()) {
        return last_index();
    }

    uint64_t after = entries[0].index - 1;
    if (after < committed_) {
        LOG_ERROR("after(%lu) is out of range [committed(%lu)]\", after, committed_", after, committed_);
        assert(false);
    }

    std::vector<proto::EntryPtr> ents;
    for (auto it = entries.begin(); it != entries.end(); ++it) {
        proto::EntryPtr entry(new proto::Entry(std::move(*it)));
        ents.push_back(std::move(entry));
    }

    unstable_->truncate_and_append(std::move(ents));
    return last_index();

}

uint64_t RaftLog::find_conflict(const std::vector<proto::Entry>& entries)
{
    for (const proto::Entry& entry : entries) {
        if (!match_term(entry.index, entry.term)) {
            if (entry.index < last_index()) {
                uint64_t t;
                Status status = this->term(entry.index, t);
                LOG_INFO("found conflict at index %lu [existing term: %lu, conflicting term: %lu], %s",
                         entry.index,
                         t,
                         entry.term,
                         status.to_string().c_str());
            }
            return entry.index;
        }
    }
    return 0;
}

void RaftLog::next_entries(std::vector<proto::Entry>& entries) const
{
    uint64_t off = std::max(applied_ + 1, first_index());
    if (committed_ + 1 > off) {
        Status status = slice(off, committed_ + 1, max_next_ents_size_, entries);
        if (!status.is_ok()) {
            LOG_ERROR("unexpected error when getting unapplied entries");
            assert(false);
        }
    }
}

bool RaftLog::has_next_entries() const
{
    uint64_t off = std::max(applied_ + 1, first_index());
    return committed_ + 1 > off;
}

bool RaftLog::maybe_commit(uint64_t max_index, uint64_t term)
{
    if (max_index > committed_) {
        uint64_t t;
        this->term(max_index, t);
        if (t == term) {
            commit_to(max_index);
            return true;
        }
    }
    return false;
}

void RaftLog::restore(proto::SnapshotPtr snapshot)
{
    LOG_INFO("log starts to restore snapshot [index: %lu, term: %lu]", snapshot->metadata.index, snapshot->metadata.term);
    committed_ = snapshot->metadata.index;
    unstable_->restore(std::move(snapshot));
}

Status RaftLog::snapshot(proto::Snapshot& snap) const
{
    if (unstable_->ref_snapshot()) {
        //copy
        snap = (*unstable_->ref_snapshot());
    }

    proto::SnapshotPtr s;
    Status status = storage_->snapshot(s);
    if (s) {
        //copy
        snap = *s;
    }
    return status;
}

void RaftLog::applied_to(uint64_t index)
{
    if (index == 0) {
        return;
    }
    if (committed_ < index || index < applied_) {
        LOG_ERROR("applied(%lu) is out of range [prevApplied(%lu), committed(%lu)]", index, applied_, committed_);
    }
    applied_ = index;
}

Status RaftLog::slice(uint64_t low, uint64_t high, uint64_t max_size, std::vector<proto::Entry>& entries) const
{
    Status status = must_check_out_of_bounds(low, high);
    if (!status.is_ok()) {
        return status;
    }
    if (low == high) {
        return Status::ok();
    }

    std::vector<proto::EntryPtr> entry_ptr;

    if (low < unstable_->offset()) {
        status = storage_->entries(low, std::min(high, unstable_->offset()), max_size, entry_ptr);
        if (!status.is_ok()) {
            return status;
        }

        // check if ents has reached the size limitation
        if (entry_ptr.size() < std::min(high, unstable_->offset()) - low) {
            for (auto it = entry_ptr.begin(); it != entry_ptr.end(); ++it) {
                //copy
                entries.push_back(**it);
            }
            return Status::ok();
        }

    }
    if (high > unstable_->offset()) {
        std::vector<proto::EntryPtr> unstable;
        unstable_->slice(std::max(low, unstable_->offset()), high, unstable);
    }
    entry_limit_size(max_size, entry_ptr);

    for (auto it = entry_ptr.begin(); it != entry_ptr.end(); ++it) {
        //copy
        entries.push_back(**it);
    }
    return Status::ok();
}

void RaftLog::commit_to(uint64_t to_commit)
{
    // never decrease commit
    if (committed_ < to_commit) {
        if (last_index() < to_commit) {
            LOG_ERROR("tocommit(%lu) is out of range [lastIndex(%lu)]. Was the raft log corrupted, truncated, or lost?",
                      to_commit,
                      last_index());
            assert(false);
        }
        committed_ = to_commit;
    }
}

bool RaftLog::match_term(uint64_t index, uint64_t t)
{
    uint64_t term_out;
    Status status = this->term(index, term_out);
    if (!status.is_ok()) {
        return false;
    }
    return t == term_out;
}

uint64_t RaftLog::last_term() const
{
    uint64_t t;
    Status status = term(last_index(), t);
    assert(status.is_ok());
    return t;
}

Status RaftLog::term(uint64_t index, uint64_t& t) const
{
    uint64_t dummy_index = first_index() - 1;
    if (index < dummy_index || index > last_index()) {
        // TODO: return an error instead?
        LOG_ERROR("invalid index, %lu, %lu, %lu", dummy_index, index, last_index());
        t = 0;
        return Status::ok();
    }


    uint64_t term_index;
    bool ok;

    unstable_->maybe_term(index, term_index, ok);
    if (ok) {
        t = term_index;
        return Status::ok();
    }


    Status status = storage_->term(index, term_index);
    if (status.is_ok()) {
        t = term_index;
    }
    return status;
}

uint64_t RaftLog::first_index() const
{
    uint64_t index;
    bool ok;
    unstable_->maybe_first_index(index, ok);
    if (ok) {
        return index;
    }

    Status status = storage_->first_index(index);
    assert(status.is_ok());

    return index;
}

uint64_t RaftLog::last_index() const
{
    uint64_t index;
    bool ok;
    unstable_->maybe_last_index(index, ok);
    if (ok) {
        return index;
    }

    Status status = storage_->last_index(index);
    assert(status.is_ok());

    return index;
}

Status RaftLog::must_check_out_of_bounds(uint64_t low, uint64_t high) const
{
    assert(high > low);

    uint64_t first = first_index();

    if (low < first) {
        return Status::invalid_argument("requested index is unavailable due to compaction");
    }

    uint64_t length = last_index() + 1 - first;
    if (low < first || high > first + length) {
        LOG_ERROR("slice[%lu,%lu) out of bound [%lu,%lu]", low, high, first, last_index());
    }
    return Status::ok();

}

}

