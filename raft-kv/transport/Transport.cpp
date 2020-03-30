#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <raft-kv/transport/Transport.h>
#include <raft-kv/common/log.h>

namespace kv {

class TransportImpl : public Transport {

 public:
  explicit TransportImpl(RaftServer* raft, uint64_t id)
      : raft_(raft),
        id_(id) {
  }

  ~TransportImpl() final {
    if (io_thread_.joinable()) {
      io_thread_.join();
      LOG_DEBUG("transport stopped");
    }
  }

  void start(const std::string& host) final {
    server_ = IoServer::create((void*)&io_service_, host, raft_);
    server_->start();

    io_thread_ = std::thread([this]() {
      this->io_service_.run();
    });
  }

  void add_peer(uint64_t id, const std::string& peer) final {
    LOG_DEBUG("node:%lu, peer:%lu, addr:%s", id_, id, peer.c_str());
    std::lock_guard<std::mutex> guard(mutex_);

    auto it = peers_.find(id);
    if (it != peers_.end()) {
      LOG_DEBUG("peer already exists %lu", id);
      return;
    }

    PeerPtr p = Peer::creat(id, peer, (void*)&io_service_);
    p->start();
    peers_[id] = p;
  }

  void remove_peer(uint64_t id) final {
    LOG_WARN("no impl yet");
  }

  void send(std::vector<proto::MessagePtr> msgs) final {
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

  void stop() final {
    io_service_.stop();
  }

 private:
  RaftServer* raft_;
  uint64_t id_;

  std::thread io_thread_;
  boost::asio::io_service io_service_;

  std::mutex mutex_;
  std::unordered_map<uint64_t, PeerPtr> peers_;

  IoServerPtr server_;
};

std::shared_ptr<Transport> Transport::create(RaftServer* raft, uint64_t id) {
  std::shared_ptr<TransportImpl> impl(new TransportImpl(raft, id));
  return impl;
}

}
