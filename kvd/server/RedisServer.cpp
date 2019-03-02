#include <msgpack.hpp>
#include <kvd/server/RedisServer.h>
#include <kvd/server/KvdServer.h>
#include <kvd/common/log.h>
#include <kvd/server/RedisSession.h>

namespace kvd
{


RedisServer::RedisServer(std::weak_ptr<KvdServer> server, uint16_t port)
    : server_(std::move(server)),
      acceptor_(io_service_)
{
    auto address = boost::asio::ip::address::from_string("0.0.0.0");
    auto endpoint = boost::asio::ip::tcp::endpoint(address, port);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(1));
    acceptor_.bind(endpoint);
    acceptor_.listen();
}

RedisServer::~RedisServer()
{
    if (worker_.joinable()) {
        worker_.join();
    }
}

void RedisServer::start(std::promise<pthread_t>& promise)
{
    start_accept();

    auto self = shared_from_this();
    worker_ = std::thread([self, &promise]() {
        promise.set_value(pthread_self());
        self->io_service_.run();
    });
}

void RedisServer::start_accept()
{
    RedisSessionPtr session(new RedisSession(shared_from_this(), io_service_));

    acceptor_.async_accept(session->socket_, [this, session](const boost::system::error_code& error) {
        if (error) {
            LOG_DEBUG("accept error %s", error.message().c_str());
            return;
        }
        this->start_accept();
        session->start();
    });
}

void RedisServer::put(std::string key, std::string value, const std::function<void(const Status&)>& callback)
{

    msgpack::sbuffer sbuf;
    std::shared_ptr<std::vector<uint8_t>> data(new std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size()));

    auto on_propose = [this, callback](const Status& status) {
        io_service_.post([callback, status]() {
            callback(status);
        });
    };
    server_.lock()->propose(std::move(data), std::move(on_propose));
}

void RedisServer::read_commit(proto::EntryPtr entry)
{
    auto cb = [this, entry] {

    };

    io_service_.post(cb);
}

}

