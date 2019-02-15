#pragma once
#include <boost/asio.hpp>
#include <kvd/raft/proto.h>

namespace kvd
{

class Peer
{
public:
    virtual ~Peer() = default;

    virtual void start() = 0;

    // send sends the message to the remote peer. The function is non-blocking
    // and has no promise that the message will be received by the remote.
    // When it fails to send message out, it will report the status to underlying
    // raft.
    virtual void send(proto::MessagePtr msg) = 0;

    // sendSnap sends the merged snapshot message to the remote peer. Its behavior
    // is similar to send.
    virtual void send_snap(proto::SnapshotPtr snap) = 0;

    // update updates the urls of remote peer.
    virtual void update(const std::string& peer) = 0;

    // activeSince returns the time that the connection with the
    // peer becomes active.
    virtual uint64_t active_since() = 0;

    // stop performs any necessary finalization and terminates the peer
    // elegantly
    virtual void stop() = 0;
};
typedef std::shared_ptr<Peer> PeerPtr;

class ClientSession;
typedef std::shared_ptr<ClientSession> ClientSessionPtr;

class AsioPeer: public Peer, public std::enable_shared_from_this<AsioPeer>
{
public:
    explicit AsioPeer(boost::asio::io_service& io_service, uint64_t peer, const std::string& peer_str);

    virtual ~AsioPeer();

    virtual void start();

    virtual void send(proto::MessagePtr msg);

    virtual void send_snap(proto::SnapshotPtr snap);

    virtual void update(const std::string& peer);

    virtual uint64_t active_since();

    virtual void stop();
private:
    void do_send_data(uint8_t type, const uint8_t* data, uint32_t len);
    void start_timer();

    boost::asio::io_service& io_service_;

    friend class ClientSession;
    ClientSessionPtr session_;
    boost::asio::ip::tcp::endpoint endpoint_;
    boost::asio::deadline_timer timer_;
    bool paused;
};

}