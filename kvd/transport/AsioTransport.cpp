#include "kvd/transport/AsioTransport.h"
#include "kvd/common/log.h"
#include "kvd/transport/AsioPeer.h"

namespace kvd
{


AsioTransport::AsioTransport(std::weak_ptr<RaftServer> raft, uint64_t id)
    : raft_(std::move(raft)),
      id_(id)
{

}

AsioTransport::~AsioTransport()
{
    if (io_thread_.joinable()) {
        io_thread_.join();
        LOG_DEBUG("asio transport stopped");
    }

}

void AsioTransport::start()
{
    io_thread_ = std::thread([this]() {
        this->io_service_.run();
    });
}

void AsioTransport::add_peer(uint64_t id, const std::string& peer)
{
    LOG_DEBUG("node:%lu, peer:%lu, addr:%s", id_, id, peer.c_str());
    std::lock_guard<std::mutex> guard(mutex_);

    auto it = peers_.find(id);
    if (it != peers_.end()) {
        LOG_DEBUG("peer already exists %lu", id);
        return;
    }

    AsioPeer* asio_peer = new AsioPeer(io_service_, id, peer);
    PeerPtr p((Peer*) asio_peer);
    p->start();
    peers_[id] = p;
}

void AsioTransport::send(std::vector<proto::MessagePtr> msgs)
{

}

void AsioTransport::stop()
{
    io_service_.stop();
}

}
