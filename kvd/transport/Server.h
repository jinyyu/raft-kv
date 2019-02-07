#pragma once
#include <memory>
#include <boost/asio.hpp>

namespace kvd
{

class ServerSession;
typedef std::shared_ptr<ServerSession> ServerSessionPtr;

class Server
{
public:

    virtual ~Server() = default;

    virtual void start() = 0;

    virtual void stop() = 0;
};
typedef std::shared_ptr<Server> ServerPtr;

class AsioServer: public Server
{
public:
    explicit AsioServer(boost::asio::io_service& io_service, const std::string& host);

    ~AsioServer();

    virtual void start();

    virtual void stop();

private:
    void handle_accept(ServerSessionPtr session);

    boost::asio::io_service& io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
};

}