#include "kvd/raft/proto.h"
#include <boost/assert.hpp>
#include "kvd/common/log.h"
#include <msgpack.hpp>

namespace kvd
{
namespace proto
{

const char* msg_type_to_string(MessageType type)
{
    switch (type) {
    case MsgHup: {
        return "MsgHup";
    }
    case MsgBeat: {
        return "MsgBeat";
    }
    case MsgProp: {
        return "MsgProp";
    }
    case MsgApp: {
        return "MsgApp";
    }
    case MsgAppResp: {
        return "MsgAppResp";
    }
    case MsgVote: {
        return "MsgVote";
    }
    case MsgVoteResp: {
        return "MsgVoteResp";
    }
    case MsgSnap: {
        return "MsgSnap";
    }
    case MsgHeartbeat: {
        return "MsgHeartbeat";
    }
    case MsgHeartbeatResp: {
        return "MsgHeartbeatResp";
    }
    case MsgUnreachable: {
        return "MsgUnreachable";
    }
    case MsgSnapStatus: {
        return "MsgSnapStatus";
    }
    case MsgCheckQuorum: {
        return "MsgCheckQuorum";
    }
    case MsgTransferLeader: {
        return "MsgTransferLeader";
    }
    case MsgTimeoutNow: {
        return "MsgTimeoutNow";
    }
    case MsgReadIndex: {
        return "MsgReadIndex";
    }
    case MsgReadIndexResp: {
        return "MsgReadIndexResp";
    }
    case MsgPreVote: {
        return "MsgPreVote";
    }
    case MsgPreVoteResp: {
        return "MsgPreVoteResp";
    }
    default: {
        LOG_FATAL("invalid msg type %d", type);
    }
    }
}

const char* entry_type_to_string(EntryType type)
{
    switch (type) {
    case EntryNormal: {
        return "EntryNormal";
    }
    case EntryConfChange: {
        return "EntryConfChange";
    }
    default: {
        LOG_FATAL("invalid entry type %d", type);
    }
    }
}

// detail: https://github.com/msgpack/msgpack/blob/master/spec.md#str-format-family
static uint32_t u8_serialize_size(uint8_t d)
{
    if (d < (1 << 7)) {
        /* fixnum */
        return 1;
    }
    else {
        /* unsigned 8 */
        return 2;
    }
}

static uint32_t u64_serialize_size(uint64_t d)
{
    if (d < (1ULL << 8)) {
        if (d < (1ULL << 7)) {
            /* fixnum */
            return 1;
        }
        else {
            /* unsigned 8 */
            return 2;
        }
    }
    else {
        if (d < (1ULL << 16)) {
            /* unsigned 16 */
            return 3;
        }
        else if (d < (1ULL << 32)) {
            /* unsigned 32 */
            return 5;
        }
        else {
            /* unsigned 64 */
            return 9;
        }
    }
}

static uint32_t data_serialize_size(uint32_t len)
{

    if (len <= std::numeric_limits<uint8_t>::max()) {
        // (2^8)-1
        return 2 + len;
    }
    if (len <= std::numeric_limits<uint16_t>::max()) {
        //(2^16)-1
        return 3 + len;
    }
    if (len <= std::numeric_limits<uint32_t>::max()) {
        return 5 + len;
    }
    assert(false);
}

uint32_t Entry::serialize_size() const
{
    return 1 + u8_serialize_size(type)
        + u64_serialize_size(term)
        + u64_serialize_size(index)
        + data_serialize_size(static_cast<uint32_t>(data.size()));
}

}
}