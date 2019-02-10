#include <kvd/raft/Node.h>
#include <kvd/common/log.h>

namespace kvd
{


RawNode::RawNode(const Config& conf, const std::vector<PeerContext>& peers, boost::asio::io_service& io_service)
    : io_service_(io_service)
{

}

RawNode::~RawNode()
{

}

void RawNode::tick()
{

}

Status RawNode::campaign()
{
    LOG_DEBUG("no impl yet");
    return Status::ok();
}

Status RawNode::propose(std::vector<uint8_t> data)
{
    LOG_DEBUG("no impl yet");
    return Status::ok();
}

Status RawNode::propose_conf_change(proto::ConfChange cs)
{
    LOG_DEBUG("no impl yet");
    return Status::ok();
}

Status RawNode::step(proto::Message msg)
{
    LOG_DEBUG("no impl yet");
    return Status::ok();
}

void RawNode::ready()
{

}

bool RawNode::has_ready()
{
    return true;
}

void RawNode::advance()
{

}

proto::ConfState RawNode::apply_conf_change(proto::ConfChange cs)
{
    return proto::ConfState();
}

void RawNode::transfer_leadership(uint64_t lead, ino64_t transferee)
{

}

Status RawNode::read_index(std::vector<uint8_t> rctx)
{
    LOG_DEBUG("no impl yet");
    return Status::ok();
}

RaftStatusPtr RawNode::raft_status()
{
    LOG_DEBUG("no impl yet");
    return nullptr;
}

void RawNode::report_unreachable(uint64_t id)
{

}

void RawNode::report_snapshot(uint64_t id, SnapshotStatus status)
{

}

void RawNode::stop()
{

}

}