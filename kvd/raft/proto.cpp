#include "kvd/raft/proto.h"
#include <boost/assert.hpp>
#include "kvd/common/log.h"

namespace kvd
{
namespace proto
{

const char *msg_type_to_string(MessageType type)
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
        LOG_DEBUG("invalid msg type %d", type);
        assert(false);
        return "unknown";
    }
    }
}

const char *entry_type_to_string(EntryType type)
{
    switch (type) {
    case EntryNormal: {
        return "EntryNormal";
    }
    case EntryConfChange: {
        return "EntryConfChange";
    }
    default:LOG_DEBUG("invalid entry type %d", type);
        assert(false);
        return "unknown";
    }
}

}
}