#include <raft-kv/raft/readonly.h>
#include <raft-kv/common/log.h>

namespace kv {

void ReadOnly::last_pending_request_ctx(std::vector<uint8_t>& ctx) {
  if (read_index_queue.empty()) {
    return;
  }

  ctx.insert(ctx.end(), read_index_queue.back().begin(), read_index_queue.back().end());
}

uint32_t ReadOnly::recv_ack(const proto::Message& msg) {
  std::string str(msg.context.begin(), msg.context.end());

  auto it = pending_read_index.find(str);
  if (it == pending_read_index.end()) {
    return 0;
  }

  it->second->acks.insert(msg.from);
  // add one to include an ack from local node
  return it->second->acks.size() + 1;
}

std::vector<ReadIndexStatusPtr> ReadOnly::advance(const proto::Message& msg) {
  std::vector<ReadIndexStatusPtr> rss;

  std::string ctx(msg.context.begin(), msg.context.end());
  bool found = false;
  uint32_t i = 0;
  for (std::string& okctx: read_index_queue) {
    i++;
    auto it = pending_read_index.find(okctx);
    if (it == pending_read_index.end()) {
      LOG_FATAL("cannot find corresponding read state from pending map");
    }

    rss.push_back(it->second);
    if (okctx == ctx) {
      found = true;
      break;
    }

  }

  if (found) {
    read_index_queue.erase(read_index_queue.begin(), read_index_queue.begin() + i);
    for (ReadIndexStatusPtr& rs : rss) {
      std::string str(rs->req.entries[0].data.begin(), rs->req.entries[0].data.end());
      pending_read_index.erase(str);
    }
  }
  return rss;
}

void ReadOnly::add_request(uint64_t index, proto::MessagePtr msg) {
  std::string ctx(msg->entries[0].data.begin(), msg->entries[0].data.end());
  auto it = pending_read_index.find(ctx);
  if (it != pending_read_index.end()) {
    return;
  }
  ReadIndexStatusPtr status(new ReadIndexStatus());
  status->index = index;
  status->req = *msg;
  pending_read_index[ctx] = status;
  read_index_queue.push_back(ctx);
}

}
