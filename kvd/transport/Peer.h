#pragma once
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

}