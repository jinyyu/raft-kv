#include "kvd/raft/Raft.h"
#include "kvd/common/log.h"
#include <boost/algorithm/string.hpp>

namespace kvd
{

Raft::Raft(const Config& c)
    : id_(c.id),
      max_msg_size_(c.max_size_per_msg),
      max_uncommitted_size_(c.max_uncommitted_entries_size),
      max_inflight_(c.max_inflight_msgs),
      is_learner_(false),
      lead_(0),
      lead_transferee_(0),
      pending_conf_index_(0),
      uncommitted_size_(0),
      read_only_(new ReadOnly(c.read_only_option)),
      election_elapsed_(0),
      heartbeat_elapsed_(0),
      check_quorum_(c.check_quorum),
      pre_vote_(c.pre_vote),
      heartbeat_timeout_(c.heartbeat_tick),
      election_timeout_(c.election_tick),
      randomized_election_timeout_(0),
      disable_proposal_forwarding_(c.disable_proposal_forwarding)
{
    raft_log_ = std::make_shared<RaftLog>(c.storage, c.max_committed_size_per_ready);
    proto::HardState hs;
    proto::ConfState cs;
    Status status = c.storage->initial_state(hs, cs);
    if (!status.is_ok()) {
        LOG_FATAL("%s", status.to_string().c_str());
    }


    std::vector<uint64_t> peers = c.peers;
    std::vector<uint64_t> learners = c.learners;

    if (cs.nodes.size() > 0 || cs.learners.size() > 0) {
        if (peers.size() > 0 || learners.size() > 0) {
            // tests; the argument should be removed and these tests should be
            // updated to specify their nodes through a snapshot.
            LOG_FATAL("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
        }
        peers = cs.nodes;
        learners = cs.learners;
    }


    for (uint64_t peer :  peers) {
        ProgressPtr p(new Progress(max_inflight_));
        p->next = 1;
        prs_[peer] = p;
    }

    for (uint64_t learner :  learners) {
        auto it = prs_.find(learner);
        if (it != prs_.end()) {
            LOG_FATAL("node %lu is in both learner and peer list", learner);
        }

        ProgressPtr p(new Progress(max_inflight_));
        p->next = 1;
        p->is_learner = true;

        learner_prs_[learner] = p;

        if (id_ == learner) {
            is_learner_ = true;
        }
    }

    if (!hs.is_empty_state()) {
        load_state(hs);
    }


    if (c.applied > 0) {
        raft_log_->applied_to(c.applied);

    }
    become_follower(term_, 0);

    std::string node_str;
    {
        std::vector<std::string> nodes_strs;
        std::vector<uint64_t> node;
        this->nodes(node);
        for (uint64_t n : node) {
            nodes_strs.push_back(std::to_string(n));
        }
        node_str = boost::join(nodes_strs, ",");
    }

    LOG_INFO("raft %lu [peers: [%s], term: %lu, commit: %lu, applied: %lu, last_index: %lu, last_term: %lu]",
             id_,
             node_str.c_str(),
             term_,
             raft_log_->committed(),
             raft_log_->applied(),
             raft_log_->last_index(),
             raft_log_->last_term());
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

void Raft::load_state(const proto::HardState& state)
{
    if (state.commit < raft_log_->committed() || state.commit > raft_log_->last_index()) {
        LOG_FATAL("%lu state.commit %lu is out of range [%lu, %lu]",
                  id_,
                  state.commit,
                  raft_log_->committed(),
                  raft_log_->last_index());
    }
    raft_log_->committed() = state.commit;
    term_ = state.term;
    vote_ = state.vote;
}

void Raft::nodes(std::vector<uint64_t>& node) const
{
    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        node.push_back(it->first);
    }
}

ProgressPtr Raft::get_process(uint64_t id)
{
    auto it = prs_.find(id);
    if (it != prs_.end()) {
        return it->second;
    }
    else {
        return nullptr;
    }
}

}