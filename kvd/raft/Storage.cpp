#include "kvd/raft/Storage.h"
#include <kvd/common/log.h>

namespace kvd
{



Status MemoryStorage::initial_state(proto::HardState& hard_state, proto::ConfState& conf_state)
{
    LOG_ERROR("not impl yet");
    return Status::ok();

}

Status MemoryStorage::entries(uint32_t low,
                       uint32_t high,
                       uint64_t max_size,
                       std::vector<proto::EntryPtr>& entries)
{
    LOG_ERROR("not impl yet");
    return Status::ok();
}

Status MemoryStorage::term(uint64_t i, uint64_t& term)
{
    LOG_ERROR("not impl yet");
    return Status::ok();
}

Status MemoryStorage::last_index(uint64_t& index)
{
    LOG_ERROR("not impl yet");
    return Status::ok();
}

Status MemoryStorage::first_index(uint64_t& index)
{
    LOG_ERROR("not impl yet");
    return Status::ok();
}

Status MemoryStorage::snapshot(proto::SnapshotPtr& snapshot)
{
    LOG_ERROR("not impl yet");
    return Status::ok();
}


}