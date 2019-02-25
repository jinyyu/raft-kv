#pragma once
#include <memory>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <boost/asio.hpp>
#include <kvd/transport/Peer.h>
#include <kvd/transport/Server.h>
#include <kvd/transport/Transport.h>
#include <kvd/common/Status.h>
#include <kvd/raft/proto.h>
#include <kvd/raft/Node.h>

namespace kvd
{

class Transport
{
public:
    virtual ~Transport() = default;

    virtual void start(const std::string& host) = 0;

    virtual void stop() = 0;

    // sends out the given messages to the remote peers.
    // Each message has a To field, which is an id that maps
    // to an existing peer in the transport.
    // If the id cannot be found in the transport, the message
    // will be ignored.
    virtual void send(std::vector<proto::MessagePtr> msgs) = 0;

    virtual void add_peer(uint64_t id, const std::string& peer) = 0;

    virtual void remove_peer(uint64_t id) = 0;

};

typedef std::shared_ptr<Transport> TransporterPtr;

class RaftServer
{
public:
    virtual ~RaftServer() = default;

    virtual void process(proto::MessagePtr msg, const std::function<void(const Status&)>& callback) = 0;

    virtual void is_id_removed(uint64_t id, const std::function<void(bool)>& callback) = 0;

    virtual void report_unreachable(uint64_t id) = 0;

    virtual void report_snapshot(uint64_t id, SnapshotStatus status) = 0;
};

class AsioTransport: public Transport
{

public:
    explicit AsioTransport(std::weak_ptr<RaftServer> raft, uint64_t id);

    ~AsioTransport();

    virtual void start(const std::string& host);

    virtual void add_peer(uint64_t id, const std::string& peer);

    virtual void remove_peer(uint64_t id);

    virtual void send(std::vector<proto::MessagePtr> msgs);

    virtual void stop();

private:
    std::weak_ptr<RaftServer> raft_;
    uint64_t id_;

    std::thread io_thread_;
    boost::asio::io_service io_service_;

    std::mutex mutex_;
    std::unordered_map<uint64_t, PeerPtr> peers_;

    std::shared_ptr<AsioServer> server_;
};

}
