#include <boost/algorithm/string.hpp>
#include <raft-kv/transport/peer.h>
#include <raft-kv/common/log.h>
#include <raft-kv/common/bytebuffer.h>
#include <raft-kv/transport/proto.h>
#include <boost/asio.hpp>

namespace kv {

class PeerImpl;
class ClientSession {
 public:
  explicit ClientSession(boost::asio::io_service& io_service, PeerImpl* peer);

  ~ClientSession() {

  }

  void send(uint8_t transport_type, const uint8_t* data, uint32_t len) {
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

  void close_session();

  void start_connect() {
    socket_.async_connect(endpoint_, [this](const boost::system::error_code& err) {
      if (err) {
        LOG_DEBUG("connect [%lu] error %s", this->peer_id_, err.message().c_str());
        this->close_session();
        return;
      }
      this->connected_ = true;
      LOG_INFO("connected to [%lu]", this->peer_id_);

      if (this->buffer_.readable()) {
        this->start_write();
      }
    });
  }

  void start_write() {
    if (!buffer_.readable()) {
      return;
    }

    uint32_t remaining = buffer_.readable_bytes();
    auto buffer = boost::asio::buffer(buffer_.reader(), remaining);
    auto handler = [this](const boost::system::error_code& error, std::size_t bytes) {
      if (error || bytes == 0) {
        LOG_DEBUG("send [%lu] error %s", this->peer_id_, error.message().c_str());
        this->close_session();
        return;
      }
      this->buffer_.read_bytes(bytes);
      this->start_write();
    };
    boost::asio::async_write(socket_, buffer, handler);
  }

 private:
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::endpoint endpoint_;
  PeerImpl* peer_;
  uint64_t peer_id_;
  ByteBuffer buffer_;
  bool connected_;
};

class PeerImpl : public Peer {
 public:
  explicit PeerImpl(boost::asio::io_service& io_service, uint64_t peer, const std::string& peer_str)
      : peer_(peer),
        io_service_(io_service),
        timer_(io_service) {
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

  ~PeerImpl() final {
  }

  void start() final {
    start_timer();
  };

  void send(proto::MessagePtr msg) final {
    if (msg->type == proto::MsgSnap) {
      LOG_DEBUG("send snap not impl yet");
      return;
    }

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, *msg);

    do_send_data(TransportTypeStream, (const uint8_t*) sbuf.data(), (uint32_t) sbuf.size());
  }

  void send_snap(proto::SnapshotPtr snap) final {
    LOG_DEBUG("no impl yet");
  }

  void update(const std::string& peer) final {
    LOG_DEBUG("no impl yet");
  }

  uint64_t active_since() final {
    LOG_DEBUG("no impl yet");
    return 0;
  }

  void stop() final {

  }

 private:
  void do_send_data(uint8_t type, const uint8_t* data, uint32_t len) {
    if (!session_) {
      session_ = std::make_shared<ClientSession>(io_service_, this);
      session_->send(type, data, len);
      session_->start_connect();
    } else {
      session_->send(type, data, len);
    }
  }

  void start_timer() {
    timer_.expires_from_now(boost::posix_time::seconds(3));
    timer_.async_wait([this](const boost::system::error_code& err) {
      if (err) {
        LOG_ERROR("timer waiter error %s", err.message().c_str());
        return;
      }
      this->start_timer();
    });

    static std::atomic<uint32_t> tick;
    DebugMessage dbg;
    dbg.a = tick++;
    dbg.b = tick++;
    do_send_data(TransportTypeDebug, (const uint8_t*) &dbg, sizeof(dbg));
  }

  uint64_t peer_;
  boost::asio::io_service& io_service_;
  friend class ClientSession;
  std::shared_ptr<ClientSession> session_;
  boost::asio::ip::tcp::endpoint endpoint_;
  boost::asio::deadline_timer timer_;
};

ClientSession::ClientSession(boost::asio::io_service& io_service, PeerImpl* peer)
    : socket_(io_service),
      endpoint_(peer->endpoint_),
      peer_(peer),
      peer_id_(peer_->peer_),
      connected_(false) {

}

void ClientSession::close_session() {
  peer_->session_ = nullptr;
}

std::shared_ptr<Peer> Peer::creat(uint64_t peer, const std::string& peer_str, void* io_service) {
  std::shared_ptr<PeerImpl> peer_ptr(new PeerImpl(*(boost::asio::io_service*) io_service, peer, peer_str));
  return peer_ptr;
}

}
