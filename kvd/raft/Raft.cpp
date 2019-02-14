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
      disable_proposal_forwarding_(c.disable_proposal_forwarding),
      random_device_(0, c.election_tick)
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

    if (!cs.nodes.empty() || !cs.learners.empty()) {
        if (!peers.empty() || !learners.empty()) {
            // tests; the argument should be removed and these tests should be
            // updated to specify their nodes through a snapshot.
            LOG_FATAL("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
        }
        peers = cs.nodes;
        learners = cs.learners;
    }


    for (uint64_t peer : peers) {
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
    step_ = [this](proto::MessagePtr msg) {
        this->step_follower(std::move(msg));
    };
    reset(term);

    tick_ = [this]() {
        this->tick_election();
    };
    lead_ = lead;
    state_ = RaftState::Follower;

    LOG_INFO("%lu became follower at term %lu", id_, term_);
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
    bool ret = it != prs_.end();

    if (ret) {
        LOG_INFO("promotable");
    }
    else {
        LOG_INFO("not promotable %d", id_);
    }
    return ret;
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
    LOG_INFO("step follower");
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
    else {
        //LOG_WARN("tick function is not set");
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
    if (!isLearner_ {
            delete(r.learnerPrs, id)
            r.prs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight)}
            return
        }

    if _, ok := r.prs[id]; ok {
        panic(fmt.Sprintf("%x unexpected changing from voter to learner for %x", r.id, id))
    }
    r.learnerPrs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight), IsLearner: true}
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
    if (term_ != term) {
        term_ = term;
        vote_ = 0;
    }
    lead_ = 0;

    election_elapsed_ = 0;
    heartbeat_elapsed_ = 0;
    reset_randomized_election_timeout();

    abort_leader_transfer();

    votes_.clear();
    for_each_progress([this](uint64_t id, ProgressPtr& progress) {
        bool is_learner = progress->is_learner;
        progress = std::make_shared<Progress>(max_inflight_);
        progress->next = raft_log_->last_index();
        progress->is_learner = is_learner;

        if (id == id_) {
            progress->match = raft_log_->last_index();
        }

    });

    pending_conf_index_ = 0;
    uncommitted_size_ = 0;
    read_only_->pending_read_index.clear();
    read_only_->read_index_queue.clear();
}

void Raft::add_node(uint64_t id)
{
    add_node_or_learner(id, false);
}

bool Raft::append_entry(std::vector<proto::EntryPtr> entries)
{
    LOG_WARN("no impl yet");
    return true;
}

void Raft::tick_election()
{
    election_elapsed_++;

    if (promotable() && past_election_timeout()) {
        election_elapsed_ = 0;
        proto::MessagePtr msg(new proto::Message());
        msg->from = id_;
        msg->type = proto::MsgHup;
        step(std::move(msg));
    }
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
    randomized_election_timeout_ = election_timeout_ + random_device_.gen();
    assert(randomized_election_timeout_ <= 2 * election_timeout_);
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