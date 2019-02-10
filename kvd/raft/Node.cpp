#include <kvd/raft/Node.h>


namespace kvd
{


RawNode::RawNode(const Config& conf, const std::vector<PeerContext>& peers, boost::asio::io_service& io_service)
    : io_service_(io_service)
{

}

RawNode::~RawNode()
{

}

}