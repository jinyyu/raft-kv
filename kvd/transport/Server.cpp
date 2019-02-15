#include <boost/algorithm/string.hpp>
#include <kvd/transport/Server.h>
#include <kvd/common/log.h>
#include <kvd/common/ByteBuffer.h>
#include <kvd/transport/proto.h>
#include <kvd/raft/proto.h>
#include <kvd/transport/Transport.h>

namespace kvd
{

class ServerSession;
typedef std::shared_ptr<ServerSession> ServerSessionPtr;

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
        assert(sizeof(meta_) == 5);
        meta_.type = 0;
        meta_.len = 0;
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
            self->decode_message(len);
        };
        boost::asio::async_read(socket, buffer, boost::asio::transfer_exactly(len), handler);
    }

    void decode_message(uint32_t len)
    {
        switch (meta_.type) {
        case TransportTypeDebug: {
            assert(len == sizeof(DebugMessage));
            DebugMessage* dbg = (DebugMessage*) buffer_.data();
            assert(dbg->a + 1 == dbg->b);
            //LOG_DEBUG("tick ok");
            break;
        }
        case TransportTypeStream: {
            proto::MessagePtr msg(new proto::Message());
            msgpack::object_handle oh = msgpack::unpack((const char*) buffer_.data(), buffer_.size());
            oh.get().convert(&*msg);
            on_receive_stream_message(std::move(msg));
            break;
        }
        default: {
            LOG_DEBUG("unknown msg type %d, len = %d", meta_.type, ntohl(meta_.len));
            return;
        }
        }

        start_read_meta();
    }

    void on_receive_stream_message(proto::MessagePtr msg)
    {
        auto server = server_.lock();
        if (server) {
            server->on_message(std::move(msg));
        }
    }

    boost::asio::ip::tcp::socket socket;
private:
    std::weak_ptr<AsioServer> server_;
    TransportMeta meta_;
    std::vector<uint8_t> buffer_;
};

AsioServer::AsioServer(boost::asio::io_service& io_service,
                       const std::string& host,
                       std::weak_ptr<RaftServer> raft)
    : io_service_(io_service),
      acceptor_(io_service),
      raft_(std::move(raft))
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

void AsioServer::on_message(proto::MessagePtr msg)
{
    auto raft = raft_.lock();
    if (raft) {
        Status status = raft->process(std::move(msg));
        LOG_DEBUG("process %s", status.to_string().c_str());

    }
}

void AsioServer::stop()
{

}

}
