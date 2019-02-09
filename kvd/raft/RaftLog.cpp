#include "kvd/raft/RaftLog.h"
#include <kvd/common/log.h>

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

RaftLog::~RaftLog()
{

}

}

