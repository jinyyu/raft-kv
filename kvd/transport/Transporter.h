#pragma once
#include <memory>
#include <kvd/common/Status.h>
#include <kvd/raft/proto.h>
#include <kvd/raft/Node.h>

namespace kvd
{

class Transporter
{
public:
    virtual ~Transporter() = default;

    virtual void start(const std::string& host) = 0;

    virtual void stop() = 0;

    // sends out the given messages to the remote peers.
    // Each message has a To field, which is an id that maps
    // to an existing peer in the transport.
    // If the id cannot be found in the transport, the message
    // will be ignored.
    virtual void send(std::vector<proto::MessagePtr> msgs) = 0;

    virtual void add_peer(uint64_t id, const std::string& peer) = 0;
};

typedef std::shared_ptr<Transporter> TransporterPtr;

class RaftServer
{
public:
    virtual ~RaftServer() = default;

    virtual Status process(proto::MessagePtr msg) = 0;

    virtual bool is_id_removed(uint64_t id) = 0;

    virtual void report_unreachable(uint64_t id) = 0;

    virtual void report_snapshot(uint64_t id, SnapshotStatus status) = 0;
};

}
