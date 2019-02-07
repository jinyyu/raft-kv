#pragma once
#include <kvd/transport/Transporter.h>
#include <boost/asio/io_service.hpp>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <kvd/transport/Peer.h>

namespace kvd
{

class AsioTransport: public Transporter
{

public:
    explicit AsioTransport(std::weak_ptr<RaftServer> raft, uint64_t id);

    ~AsioTransport();

    virtual void start();

    virtual void add_peer(uint64_t id, const std::string& peer);

    virtual void send(std::vector<proto::MessagePtr> msgs);

    virtual void stop();

private:
    std::weak_ptr<RaftServer> raft_;
    uint64_t id_;

    std::thread io_thread_;
    boost::asio::io_service io_service_;

    std::mutex mutex_;
    std::unordered_map<uint64_t, PeerPtr> peers_;
};

}