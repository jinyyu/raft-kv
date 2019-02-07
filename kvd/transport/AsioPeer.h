#pragma once
#include <kvd/transport/Peer.h>
#include <boost/asio.hpp>

namespace kvd
{


class AsioPeer: public Peer
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


    boost::asio::io_service& io_service_;

};

}

