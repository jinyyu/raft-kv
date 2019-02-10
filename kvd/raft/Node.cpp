#include <kvd/raft/Node.h>
#include <kvd/common/log.h>

namespace kvd
{


RawNode::RawNode(const Config& conf, const std::vector<PeerContext>& peers, boost::asio::io_service& io_service)
    : io_service_(io_service)
{
    raft_ = std::make_shared<Raft>(conf);

    uint64_t last_index = 0;
    Status status = conf.storage->last_index(last_index);
    if (!status.is_ok()) {
        LOG_FATAL("%s", status.to_string().c_str());
    }
    if (last_index == 0) {
        raft_->become_follower(1, last_index);

        std::vector<proto::EntryPtr> entries;

        for (size_t i = 0; i < peers.size(); ++i) {
            auto& peer = peers[i];
            proto::ConfChange cs = proto::ConfChange{
                .id = 0,
                .conf_change_type = proto::ConfChangeAddNode,
                .node_id = peer.id,
                .context = peer.context,
            };

            std::vector<uint8_t> data = cs.serialize();

            proto::EntryPtr entry(new proto::Entry());
            entry->type = proto::EntryConfChange;
            entry->term = 1;
            entry->index = i + 1;
            entry->data = peer.context;
            entries.push_back(entry);
        }

        raft_->raft_log()->append(entries);
        raft_->raft_log()->committed() = entries.size();

        for (auto& peer : peers) {
            this->add_node(peer.id);
        }
    }

    // Set the initial hard and soft states after performing all initialization.
    soft_state_ = raft_->soft_state();
    if (last_index == 0) {
        prev_hard_state_ = proto::HardState();
    } else {
        prev_hard_state_ = raft_->hard_state();
    }

}

RawNode::~RawNode()
{

}

void RawNode::add_node(uint64_t id)
{
    LOG_DEBUG("no impl yet");
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