#include <kvd/transport/Server.h>
#include <boost/algorithm/string.hpp>
#include <kvd/common/log.h>
#include <kvd/common/ByteBuffer.h>
#include <kvd/transport/proto.h>
namespace kvd
{


class ServerSession: public std::enable_shared_from_this<ServerSession>
{
public:
    explicit ServerSession(boost::asio::io_service& io_service, std::weak_ptr<AsioServer> server)
        : socket(io_service),
          server_(std::move(server))
    {

    }

    void start_read_meta()
    {
        auto self = shared_from_this();
        auto buffer = boost::asio::buffer(&meta_, sizeof(meta_));
        auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
            if (error || bytes == 0) {
                LOG_DEBUG("read error %s", error.message().c_str());
                return;
            }

            if (bytes != sizeof(meta_)) {
                LOG_DEBUG("invalid data len %lu", bytes);
                return;
            }
            self->start_read_message();
        };

        boost::asio::async_read(socket, buffer, boost::asio::transfer_exactly(sizeof(meta_)), handler);
    }

    void start_read_message()
    {
        auto self = shared_from_this();
        uint32_t len = ntohl(meta_.len);
        if (buffer_.capacity() < len) {
            buffer_.resize(len);
        }

        auto buffer = boost::asio::buffer(buffer_.data(), len);
        auto handler = [self, len](const boost::system::error_code& error, std::size_t bytes) {
            assert(len == ntohl(self->meta_.len));
            if (error || bytes == 0) {
                LOG_DEBUG("read error %s", error.message().c_str());
                return;
            }

            if (bytes != len) {
                LOG_DEBUG("invalid data len %lu, %u", bytes, len);
                return;
            }
            LOG_ERROR("------------------%d", len);
            self->decode_message(len);
        };
        boost::asio::async_read(socket, buffer, boost::asio::transfer_exactly(len), handler);
    }

    void decode_message(uint32_t len)
    {
        switch (meta_.type) {
        case TransportTypeDebug: {

            LOG_DEBUG("debug msg:%s", std::string((const char*)buffer_.data(), len).c_str());
            break;
        }
        default: {
            LOG_DEBUG("unknown msg type %d, len = %d", meta_.type, ntohl(meta_.len));
            return;
        }
        }

        start_read_message();
    }

    boost::asio::ip::tcp::socket socket;
private:
    std::weak_ptr<AsioServer> server_;
    TransportMeta meta_;
    std::vector<uint8_t> buffer_;
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
        session->start_read_meta();
    });
}

void AsioServer::stop()
{

}

}
