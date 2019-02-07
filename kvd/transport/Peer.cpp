#include "kvd/transport/Peer.h"
#include "kvd/common/log.h"
#include "kvd/common/ByteBuffer.h"
#include "kvd/transport/proto.h"
#include <boost/algorithm/string.hpp>

namespace kvd
{


class ClientSession: public std::enable_shared_from_this<ClientSession>
{
public:
    explicit ClientSession(boost::asio::io_service& io_serivce, std::weak_ptr<AsioPeer> peer)
        : socket_(io_serivce),
          endpoint_(peer.lock()->endpoint_),
          peer_(std::move(peer))
    {
    }

    ~ClientSession()
    {
        LOG_DEBUG("remove session");
    }

    void send(uint8_t transport_type, const uint8_t* data, uint32_t len)
    {
        bool remain = buffer_.remain();
        uint32_t remaining = buffer_.remaining();

        TransportMeta meta;
        meta.type = transport_type;
        meta.len = htonl(len);
        assert(sizeof(TransportMeta) == 5);
        buffer_.put((const uint8_t*) &meta, sizeof(TransportMeta));
        buffer_.put(data, len);
        assert(remaining + sizeof(TransportMeta) + len == buffer_.remaining());

        if (remaining == 0) {
            start_write();
        }
    }

    void on_disconnected()
    {
        auto peer = peer_.lock();
        if (peer) {
            peer->session_ = nullptr;
        }
    }

    void start_connect()
    {
        auto self = shared_from_this();
        socket_.async_connect(endpoint_, [self](const boost::system::error_code& err) {
            if (err) {
                LOG_DEBUG("connect error %s", err.message().c_str());
                self->on_disconnected();
                return;
            }
        });
    }

    void start_write()
    {
        if (!buffer_.remain()) {
            LOG_ERROR("not data to send");
            return;
        }

        auto self = shared_from_this();
        uint32_t remaining = buffer_.remaining();
        auto buffer = boost::asio::buffer(buffer_.reader(), remaining);
        auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
            if (error || bytes == 0) {
                LOG_DEBUG("send error %s", error.message().c_str());
                self->on_disconnected();
                return;
            }
            LOG_ERROR("------------------------------------send bytes%d", bytes);
            self->buffer_.skip_bytes(bytes);
            self->start_write();
        };
        LOG_ERROR("SEND data len = %d", remaining);
        boost::asio::async_write(socket_, buffer, handler);
    }

private:
    boost::asio::ip::tcp::socket socket_;
    boost::asio::ip::tcp::endpoint endpoint_;
    std::weak_ptr<AsioPeer> peer_;
    ByteBuffer buffer_;
};

AsioPeer::AsioPeer(boost::asio::io_service& io_service, uint64_t peer, const std::string& peer_str)
    : io_service_(io_service),
      timer_(io_service)
{
    std::vector<std::string> strs;
    boost::split(strs, peer_str, boost::is_any_of(":"));
    if (strs.size() != 2) {
        LOG_DEBUG("invalid host %s", peer_str.c_str());
        exit(0);
    }
    auto address = boost::asio::ip::address::from_string(strs[0]);
    int port = std::atoi(strs[1].c_str());
    endpoint_ = boost::asio::ip::tcp::endpoint(address, port);

}

AsioPeer::~AsioPeer()
{

}

void AsioPeer::start()
{
    start_timer();
}

void AsioPeer::send(proto::MessagePtr msg)
{

}

void AsioPeer::send_snap(proto::SnapshotPtr snap)
{

}

void AsioPeer::update(const std::string& peer)
{

}

uint64_t AsioPeer::active_since()
{
    LOG_DEBUG("no impl yet");
    return 0;
}

void AsioPeer::stop()
{

}

void AsioPeer::do_send_data(uint8_t type, const uint8_t* data, uint32_t len)
{
    if (!session_) {
        session_ = std::make_shared<ClientSession>(io_service_, shared_from_this());
        session_->start_connect();
    }
    session_->send(type, data, len);
}

void AsioPeer::start_timer()
{
    auto self = shared_from_this();
    timer_.expires_from_now(boost::posix_time::seconds(3));
    timer_.async_wait([self](const boost::system::error_code& err) {
        if (err) {
            LOG_ERROR("timer waiter error %s", err.message().c_str());
            return;
        }


        self->start_timer();
    });
    for (int i = 99; i < 101; ++i) {
        auto str = std::to_string(i);
        do_send_data(TransportTypeDebug, (const uint8_t*) str.data(), str.size());
    }

}

}
