#pragma once
#include <memory>
#include <boost/asio.hpp>
#include <kvd/raft/proto.h>

namespace kvd
{

class RaftServer;
class AsioServer: public std::enable_shared_from_this<AsioServer>
{
public:
    explicit AsioServer(boost::asio::io_service& io_service,
                        const std::string& host,
                        std::weak_ptr<RaftServer> raft);

    ~AsioServer();

    void start();

    void stop();

    void on_message(proto::MessagePtr msg);

private:
    boost::asio::io_service& io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    std::weak_ptr<RaftServer> raft_;
};

}