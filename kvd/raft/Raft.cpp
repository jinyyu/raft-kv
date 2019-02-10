#include "kvd/raft/Raft.h"
#include "kvd/common/log.h"

namespace kvd
{

Raft::Raft(const Config& c)
{

}

Raft::~Raft()
{

}

void Raft::become_follower(uint64_t term, uint64_t lead)
{
    LOG_DEBUG("no impl yet");
}

void Raft::tick()
{
    if (tick_) {
        tick_();
    }
}

SoftStatePtr Raft::soft_state() const
{
    return std::make_shared<SoftState>(lead_, state_);
}

proto::HardState Raft::hard_state() const
{
    proto::HardState hs;
    hs.term = term_;
    hs.vote = vote_;
    hs.commit = raft_log_->committed();
    return hs;
}

}