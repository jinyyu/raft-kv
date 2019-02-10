#pragma once

#include <vector>
#include <kvd/raft/proto.h>

namespace kvd
{


// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
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

enum ReadOnlyOption
{
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
        ReadOnlySafe = 0,

    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
        ReadOnlyLeaseBased = 1,
};

struct ReadOnly
{
    ReadOnlyOption option;
    std::unordered_map<std::string, ReadIndexStatusPtr> pending_read_index;
    std::vector<std::string> read_index_queue;
};
typedef std::shared_ptr<ReadOnly> ReadOnlyPtr;

}
