#pragma once
#include <memory>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <raft-kv/transport/peer.h>
#include <raft-kv/transport/raft_server.h>
#include <raft-kv/transport/transport.h>
#include <raft-kv/common/status.h>
#include <raft-kv/raft/proto.h>
#include <raft-kv/raft/node.h>

namespace kv {

class Transport {
 public:
  virtual ~Transport() = default;

  virtual void start(const std::string& host) = 0;

  virtual void stop() = 0;

  // sends out the given messages to the remote peers.
  // Each message has a To field, which is an id that maps
  // to an existing peer in the transport.
  // If the id cannot be found in the transport, the message
  // will be ignored.
  virtual void send(std::vector<proto::MessagePtr> msgs) = 0;

  virtual void add_peer(uint64_t id, const std::string& peer) = 0;

  virtual void remove_peer(uint64_t id) = 0;

  static std::shared_ptr<Transport> create(RaftServer* raft, uint64_t id);
};
typedef std::shared_ptr<Transport> TransporterPtr;

}
