#include "kvd/raft/Storage.h"


namespace kvd
{



Status MemoryStorage::initial_state(proto::HardState& hard_state, proto::ConfState& conf_state)
{

}

Status MemoryStorage::entries(uint32_t low,
                       uint32_t high,
                       uint64_t max_size,
                       std::vector<proto::EntryPtr>& entries)
{

}

Status MemoryStorage::term(uint64_t i, uint64_t& term)
{

}

Status MemoryStorage::last_index(uint64_t& index)
{

}

Status MemoryStorage::first_index(uint64_t& index)
{

}

Status MemoryStorage::snapshot(proto::SnapshotPtr& snapshot)
{

}


}