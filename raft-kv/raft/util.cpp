#include <raft-kv/raft/util.h>
#include <raft-kv/common/log.h>

namespace kv {

void entry_limit_size(uint64_t max_size, std::vector<proto::EntryPtr>& entries) {
  if (entries.empty()) {
    return;
  }

  uint64_t size = entries[0]->serialize_size();
  for (size_t limit = 1; limit < entries.size(); ++limit) {
    size += entries[limit]->serialize_size();
    if (size > max_size) {
      entries.resize(limit);
      break;
    }
  }
}

proto::MessageType vote_resp_msg_type(proto::MessageType type) {
  switch (type) {
    case proto::MsgVote: {
      return proto::MsgVoteResp;
    }
    case proto::MsgPreVote: {
      return proto::MsgPreVoteResp;
    }
    default: {
      LOG_FATAL("not a vote message: %s", proto::msg_type_to_string(type));
    }
  }
}

bool is_local_msg(proto::MessageType type) {
  return type == proto::MsgHup || type == proto::MsgBeat || type == proto::MsgUnreachable ||
      type == proto::MsgSnapStatus || type == proto::MsgCheckQuorum;
}

}

