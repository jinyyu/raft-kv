#include <kvd/transport/Server.h>
#include <boost/algorithm/string.hpp>
#include <kvd/common/log.h>

namespace kvd
{

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

}

void AsioServer::stop()
{

}

}
