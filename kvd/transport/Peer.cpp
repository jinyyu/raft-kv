#include "kvd/transport/Peer.h"
#include "kvd/common/log.h"
namespace kvd
{

AsioPeer::AsioPeer(boost::asio::io_service& io_service, uint64_t peer, const std::string& peer_str)
    : io_service_(io_service)
{
    start();
}

AsioPeer::~AsioPeer()
{

}


void AsioPeer::start()
{

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

}
