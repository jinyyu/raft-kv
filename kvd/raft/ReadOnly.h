#pragma once

#include <vector>
#include <kvd/raft/proto.h>
#include <kvd/raft/Config.h>

namespace kvd
{


// ReadState provides state for read only query.
// It's caller's responsibility to call read_index first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// request_ctx
struct ReadState
{
    uint64_t index;
    std::vector<uint8_t> request_ctx;
};

struct ReadIndexStatus
{
    proto::Message req;
    uint64_t index;
    std::unordered_set<uint64_t> acks;
};
typedef std::shared_ptr<ReadIndexStatus> ReadIndexStatusPtr;

struct ReadOnly
{
    ReadOnlyOption option;
    std::unordered_map<std::string, ReadIndexStatusPtr> pending_read_index;
    std::vector<std::string> read_index_queue;
};
typedef std::shared_ptr<ReadOnly> ReadOnlyPtr;

}
