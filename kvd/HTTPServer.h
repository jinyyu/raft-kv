#pragma once
#include <boost/asio.hpp>
namespace kvd
{

class KvdServer;
class HTTPServer
{
public:
    explicit HTTPServer(std::weak_ptr<KvdServer> server, boost::asio::io_service& io_service, uint16_t port);

    void start();

private:
    std::weak_ptr<KvdServer> server_;
    boost::asio::io_service& io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
};

}
