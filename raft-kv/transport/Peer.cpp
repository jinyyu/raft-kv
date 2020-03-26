#include <boost/algorithm/string.hpp>
#include <raft-kv/transport/Peer.h>
#include <raft-kv/common/log.h>
#include <raft-kv/common/ByteBuffer.h>
#include <raft-kv/transport/proto.h>

namespace kv
{


class ClientSession: public std::enable_shared_from_this<ClientSession>
{
public:
    explicit ClientSession(boost::asio::io_service& io_service, std::weak_ptr<AsioPeer> peer)
        : socket_(io_service),
          endpoint_(peer.lock()->endpoint_),
          peer_(std::move(peer)),
          peer_id_(peer_.lock()->peer_),
          connected_(false)
    {

    }

    ~ClientSession()
    {

    }

    void send(uint8_t transport_type, const uint8_t* data, uint32_t len)
    {
        uint32_t remaining = buffer_.readable_bytes();

        TransportMeta meta;
        meta.type = transport_type;
        meta.len = htonl(len);
        assert(sizeof(TransportMeta) == 5);
        buffer_.put((const uint8_t*) &meta, sizeof(TransportMeta));
        buffer_.put(data, len);
        assert(remaining + sizeof(TransportMeta) + len == buffer_.readable_bytes());

        if (connected_ && remaining == 0) {
            start_write();
        }
    }

    void close_session()
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
                LOG_DEBUG("connect [%lu] error %s", self->peer_id_, err.message().c_str());
                self->close_session();
                return;
            }
            self->connected_ = true;
            LOG_INFO("connected to [%lu]", self->peer_id_);

            if (self->buffer_.readable()) {
                self->start_write();
            }
        });
    }

    void start_write()
    {
        if (!buffer_.readable()) {
            return;
        }

        auto self = shared_from_this();
        uint32_t remaining = buffer_.readable_bytes();
        auto buffer = boost::asio::buffer(buffer_.reader(), remaining);
        auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
            if (error || bytes == 0) {
                LOG_DEBUG("send [%lu] error %s", self->peer_id_, error.message().c_str());
                self->close_session();
                return;
            }
            self->buffer_.read_bytes(bytes);
            self->start_write();
        };
        boost::asio::async_write(socket_, buffer, handler);
    }

private:
    boost::asio::ip::tcp::socket socket_;
    boost::asio::ip::tcp::endpoint endpoint_;
    std::weak_ptr<AsioPeer> peer_;
    uint64_t peer_id_;
    ByteBuffer buffer_;
    bool connected_;
};

AsioPeer::AsioPeer(boost::asio::io_service& io_service, uint64_t peer, const std::string& peer_str)
    : peer_(peer),
      io_service_(io_service),
      timer_(io_service),
      paused(false)
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
    if (msg->type == proto::MsgSnap) {
        LOG_DEBUG("send snap not impl yet");
        return;
    }

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, *msg);

    do_send_data(TransportTypeStream, (const uint8_t*) sbuf.data(), (uint32_t) sbuf.size());

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
        session_->send(type, data, len);
        session_->start_connect();
    }
    else {
        session_->send(type, data, len);
    }

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

    static std::atomic<uint32_t> tick;
    DebugMessage dbg;
    dbg.a = tick++;
    dbg.b = tick++;
    do_send_data(TransportTypeDebug, (const uint8_t*) &dbg, sizeof(dbg));

}

}
