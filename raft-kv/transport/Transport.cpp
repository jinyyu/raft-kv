#include <boost/algorithm/string.hpp>
#include <raft-kv/transport/Transport.h>
#include <raft-kv/common/log.h>

namespace kv {

AsioTransport::AsioTransport(std::weak_ptr<RaftServer> raft, uint64_t id)
    : raft_(std::move(raft)),
      id_(id) {

}

AsioTransport::~AsioTransport() {
  if (io_thread_.joinable()) {
    io_thread_.join();
    LOG_DEBUG("asio transport stopped");
  }

}

void AsioTransport::start(const std::string& host) {
  std::shared_ptr<AsioServer> server(new AsioServer(io_service_, host, raft_));
  server_ = server;
  server_->start();

  io_thread_ = std::thread([this]() {
    this->io_service_.run();
  });
}

void AsioTransport::add_peer(uint64_t id, const std::string& peer) {
  LOG_DEBUG("node:%lu, peer:%lu, addr:%s", id_, id, peer.c_str());
  std::lock_guard<std::mutex> guard(mutex_);

  auto it = peers_.find(id);
  if (it != peers_.end()) {
    LOG_DEBUG("peer already exists %lu", id);
    return;
  }

  std::shared_ptr<AsioPeer> asio_peer(new AsioPeer(io_service_, id, peer));
  PeerPtr p = asio_peer;
  p->start();
  peers_[id] = p;
}

void AsioTransport::remove_peer(uint64_t id) {
  LOG_WARN("no impl yet");
}

void AsioTransport::send(std::vector<proto::MessagePtr> msgs) {
  auto callback = [this](std::vector<proto::MessagePtr> msgs) {
    for (proto::MessagePtr& msg : msgs) {
      if (msg->to == 0) {
        // ignore intentionally dropped message
        continue;
      }

      auto it = peers_.find(msg->to);
      if (it != peers_.end()) {
        it->second->send(msg);
        continue;
      }
      LOG_DEBUG("ignored message %d (sent to unknown peer %lu)", msg->type, msg->to);
    }
  };
  io_service_.post(std::bind(callback, std::move(msgs)));
}

void AsioTransport::stop() {
  io_service_.stop();
}

}
