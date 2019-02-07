#include <kvd/transport/Server.h>
#include <boost/algorithm/string.hpp>
#include <kvd/common/log.h>
#include <kvd/common/ByteBuffer.h>

namespace kvd
{


class ServerSession: public std::enable_shared_from_this<ServerSession>
{
public:
    explicit ServerSession(boost::asio::io_service& io_service, std::weak_ptr<AsioServer> server)
        : socket(io_service),
          server_ptr(std::move(server))
    {

    }

    void start()
    {
        LOG_DEBUG("new session ");
        auto server = server_ptr.lock();
        if (server) {
            LOG_DEBUG("LOCK OK");
        }
    }

    boost::asio::ip::tcp::socket socket;
    std::weak_ptr<AsioServer> server_ptr;
};

AsioServer::AsioServer(boost::asio::io_service& io_service, const std::string& host)
    : io_service_(io_service),
      acceptor_(io_service)
{
    std::vector<std::string> strs;
    boost::split(strs, host, boost::is_any_of(":"));
    if (strs.size() != 2) {
        LOG_DEBUG("invalid host %s", host.c_str());
        exit(0);
    }
    auto address = boost::asio::ip::address::from_string(strs[0]);
    int port = std::atoi(strs[1].c_str());
    auto endpoint = boost::asio::ip::tcp::endpoint(address, port);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(1));
    acceptor_.bind(endpoint);
    acceptor_.listen();
    LOG_DEBUG("listen at %s:%d", address.to_string().c_str(), port);
}

AsioServer::~AsioServer()
{

}

void AsioServer::start()
{
    ServerSessionPtr session(new ServerSession(io_service_, shared_from_this()));
    acceptor_.async_accept(session->socket, [this, session](const boost::system::error_code& error) {
        if (error) {
            LOG_DEBUG("accept error %s", error.message().c_str());
            return;
        }

        this->start();
        session->start();
    });
}

void AsioServer::stop()
{

}

}
