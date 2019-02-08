#pragma once
#include <kvd/raft/proto.h>

namespace kvd
{


void entry_limit_size(uint64_t max_size, std::vector<proto::EntryPtr>& entries);

}
