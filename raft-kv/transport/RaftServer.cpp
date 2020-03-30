#include <boost/algorithm/string.hpp>
#include <raft-kv/transport/RaftServer.h>
#include <raft-kv/common/log.h>
#include <raft-kv/transport/proto.h>
#include <raft-kv/transport/Transport.h>
#include <boost/asio.hpp>

namespace kv {

class AsioServer;
class ServerSession : public std::enable_shared_from_this<ServerSession> {
 public:
  explicit ServerSession(boost::asio::io_service& io_service, AsioServer* server)
      : socket(io_service),
        server_(server) {

  }

  void start_read_meta() {
    assert(sizeof(meta_) == 5);
    meta_.type = 0;
    meta_.len = 0;
    auto self = shared_from_this();
    auto buffer = boost::asio::buffer(&meta_, sizeof(meta_));
    auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
      if (bytes == 0) {
        return;;
      }
      if (error) {
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

  void start_read_message() {
    uint32_t len = ntohl(meta_.len);
    if (buffer_.capacity() < len) {
      buffer_.resize(len);
    }

    auto self = shared_from_this();
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

  void decode_message(uint32_t len) {
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
        try {
          msgpack::object_handle oh = msgpack::unpack((const char*) buffer_.data(), len);
          oh.get().convert(*msg);
        }
        catch (std::exception& e) {
          LOG_ERROR("bad message %s, size = %lu, type %s",
                    e.what(),
                    buffer_.size(),
                    proto::msg_type_to_string(msg->type));
          return;
        }
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

  void on_receive_stream_message(proto::MessagePtr msg);

  boost::asio::ip::tcp::socket socket;
 private:
  AsioServer* server_;
  TransportMeta meta_;
  std::vector<uint8_t> buffer_;
};
typedef std::shared_ptr<ServerSession> ServerSessionPtr;

class AsioServer : public IoServer {
 public:
  explicit AsioServer(boost::asio::io_service& io_service,
                      const std::string& host,
                      RaftServer* raft)
      : io_service_(io_service),
        acceptor_(io_service),
        raft_(raft) {
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

  ~AsioServer() {

  }

  void start() final {
    ServerSessionPtr session(new ServerSession(io_service_, this));
    acceptor_.async_accept(session->socket, [this, session](const boost::system::error_code& error) {
      if (error) {
        LOG_DEBUG("accept error %s", error.message().c_str());
        return;
      }

      this->start();
      session->start_read_meta();
    });
  }

  void stop() final {

  }

  void on_message(proto::MessagePtr msg) {
    raft_->process(std::move(msg), [](const Status& status) {
      if (!status.is_ok()) {
        LOG_ERROR("process error %s", status.to_string().c_str());
      }
    });
  }

 private:
  boost::asio::io_service& io_service_;
  boost::asio::ip::tcp::acceptor acceptor_;
  RaftServer* raft_;
};

void ServerSession::on_receive_stream_message(proto::MessagePtr msg) {
  server_->on_message(std::move(msg));
}

std::shared_ptr<IoServer> IoServer::create(void* io_service,
                                           const std::string& host,
                                           RaftServer* raft) {
  std::shared_ptr<AsioServer> server(new AsioServer(*(boost::asio::io_service*) io_service, host, raft));
  return server;
}

}
