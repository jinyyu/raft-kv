#pragma once
#include <kvd/transport/Transporter.h>
#include <boost/asio/io_service.hpp>
#include <thread>

namespace kvd
{


class AsioTransport: public Transporter
{

public:
    explicit AsioTransport(std::weak_ptr<RaftServer> raft, uint64_t id);

    ~AsioTransport();

    virtual void start();

    virtual void stop();

private:
    std::weak_ptr<RaftServer> raft_;
    uint64_t id_;

    std::thread io_thread_;
    boost::asio::io_service io_service_;
};

}