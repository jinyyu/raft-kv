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

void Raft::become_candidate()
{
    LOG_WARN("no impl yet");
}

void Raft::become_pre_candidate()
{
    LOG_WARN("no impl yet");
}

void Raft::become_leader()
{
    LOG_WARN("no impl yet");
}

void Raft::campaign(const std::string& campaign_type)
{
    LOG_WARN("no impl yet");
}

void Raft::poll(uint64_t id, proto::MessageType type, bool v)
{
    LOG_WARN("no impl yet");
}

Status Raft::step(proto::MessagePtr msg)
{
    LOG_WARN("no impl yet");
    return Status::ok();
}

Status Raft::step_leader(proto::MessagePtr msg)
{
    LOG_WARN("no impl yet");
    return Status::ok();
}

Status Raft::step_candidate(proto::MessagePtr msg)
{
    LOG_WARN("no impl yet");
    return Status::ok();
}

void Raft::send(proto::MessagePtr msg)
{
    msg->from = id_;
    LOG_WARN("no impl yet");
}

void Raft::restore_node(std::vector<uint64_t> nodes, bool is_learner)
{
    LOG_WARN("no impl yet");
}

bool Raft::promotable() const
{
    auto it = prs_.find(id_);
    return it != prs_.end();
}

void Raft::add_node_or_learner(uint64_t id, bool is_learner)
{
    LOG_WARN("no impl yet");
}

void Raft::remove_node(uint64_t id)
{
    LOG_WARN("no impl yet");
}

Status Raft::step_follower(proto::MessagePtr msg)
{
    LOG_WARN("no impl yet");
    return Status::ok();
}

void Raft::handle_append_entries(proto::MessagePtr msg)
{
    LOG_WARN("no impl yet");
}

void Raft::handle_heartbeat(proto::MessagePtr msg)
{
    LOG_WARN("no impl yet");
}

bool Raft::restore(proto::SnapshotPtr snapshot)
{
    LOG_WARN("no impl yet");
    return true;
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
    std::sort(node.begin(), node.end());
}

void Raft::learner_nodes(std::vector<uint64_t>& learner) const
{
    for (auto it = learner_prs_.begin(); it != prs_.end(); ++it) {
        learner.push_back(it->first);
    }
    std::sort(learner.begin(), learner.end());
}

ProgressPtr Raft::get_progress(uint64_t id)
{
    auto it = prs_.find(id);
    if (it != prs_.end()) {
        return it->second;
    }
    else {
        return nullptr;
    }
}

void Raft::set_progress(uint64_t id, uint64_t match, uint64_t next, bool is_learner)
{
    LOG_WARN("no impl yet");
}

void Raft::del_progress(uint64_t id)
{
    prs_.erase(id);
    learner_prs_.erase(id);
}

void Raft::send_append(uint64_t to)
{
    maybe_send_append(to, true);
}

bool Raft::maybe_send_append(uint64_t to, bool send_if_empty)
{
    LOG_WARN("no impl yet");
    return true;
}

void Raft::send_heartbeat(uint64_t to, std::vector<uint8_t> ctx)
{
    LOG_WARN("no impl yet");
}

void Raft::for_each_progress(const std::function<void(uint64_t, ProgressPtr&)>& callback)
{
    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        callback(it->first, it->second);
    }

    for (auto it = learner_prs_.begin(); it != learner_prs_.end(); ++it) {
        callback(it->first, it->second);
    }
}

void Raft::bcast_append()
{
    for_each_progress([this](uint64_t id, ProgressPtr& progress) {
        if (id == id_) {
            return;
        }

        this->send_append(id);
    });
}

void Raft::bcast_heartbeat()
{
    LOG_WARN("no impl yet");
}

void Raft::bcast_heartbeat_with_ctx(std::vector<uint8_t> ctx)
{
    for_each_progress([this, ctx](uint64_t id, ProgressPtr& progress) {
        if (id == id_) {
            return;
        }

        this->send_heartbeat(id, std::move(ctx));
    });
}

bool Raft::maybe_commit()
{
    LOG_WARN("no impl yet");
    return true;
}

void Raft::reset(uint64_t term)
{
    LOG_WARN("no impl yet");
}

bool Raft::append_entry(std::vector<proto::EntryPtr> entries)
{
    LOG_WARN("no impl yet");
    return true;
}

void Raft::tick_election()
{
    LOG_WARN("no impl yet");
}

void Raft::tick_heartbeat()
{
    LOG_WARN("no impl yet");
}

bool Raft::past_election_timeout()
{
    return election_elapsed_ >= randomized_election_timeout_;
}

void Raft::reset_randomized_election_timeout()
{
    LOG_WARN("no impl yet");
}

bool Raft::check_quorum_active() const
{
    LOG_WARN("no impl yet");
    return true;
}

void Raft::send_timeout_now(uint64_t to)
{
    LOG_WARN("no impl yet");
}

void Raft::abort_leader_transfer()
{
    lead_transferee_ = 0;
}

bool Raft::increase_uncommitted_size(std::vector<proto::EntryPtr> entries)
{
    LOG_WARN("no impl yet");
    return true;
}

void Raft::reduce_uncommitted_size(const std::vector<proto::EntryPtr>& entries)
{
    LOG_WARN("no impl yet");
}

}