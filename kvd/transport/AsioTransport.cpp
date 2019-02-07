#include "kvd/transport/AsioTransport.h"
#include "kvd/common/log.h"

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

}

void AsioTransport::send(std::vector<proto::Message> msgs)
{

}

void AsioTransport::stop()
{
    io_service_.stop();
}

}
