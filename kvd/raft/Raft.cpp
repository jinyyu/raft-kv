#include <boost/algorithm/string.hpp>
#include <kvd/raft/Raft.h>
#include <kvd/common/log.h>
#include <kvd/common/Slice.h>
#include <kvd/raft/util.h>

namespace kvd
{


static const std::string kCampaignPreElection = "CampaignPreElection";
static const std::string kCampaignElection = "CampaignElection";
static const std::string kCampaignTransfer = "CampaignTransfer";

static uint32_t num_of_pending_conf(const std::vector<proto::EntryPtr>& entries)
{
    uint32_t n = 0;
    for (const proto::EntryPtr& entry: entries) {
        if (entry->type == proto::EntryConfChange) {
            n++;
        }
    }
    return n;
}

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
    step_ = std::bind(&Raft::step_follower, this, std::placeholders::_1);
    reset(term);

    tick_ = std::bind(&Raft::tick_election, this);

    lead_ = lead;
    state_ = RaftState::Follower;

    LOG_INFO("%lu became follower at term %lu", id_, term_);
}

void Raft::become_candidate()
{
    LOG_WARN(
        "----------------------------------------------------------------------------------------------------------------no impl yet");
}

void Raft::become_pre_candidate()
{
    if (state_ == RaftState::Leader) {
        LOG_FATAL("invalid transition [leader -> pre-candidate]");
    }
    // Becoming a pre-candidate changes our step functions and state,
    // but doesn't change anything else. In particular it does not increase
    // r.Term or change r.Vote.
    step_ = std::bind(&Raft::step_candidate, this, std::placeholders::_1);
    votes_.clear();
    tick_ = std::bind(&Raft::tick_election, this);
    lead_ = 0;
    state_ = RaftState::PreCandidate;
    LOG_INFO("%lu became pre-candidate at term %lu", id_, term_);
}

void Raft::become_leader()
{
    LOG_WARN("no impl yet");
}

void Raft::campaign(const std::string& campaign_type)
{
    uint64_t term = 0;
    proto::MessageType vote_msg = 0;
    if (campaign_type == kCampaignPreElection) {
        become_pre_candidate();
        vote_msg = proto::MsgPreVote;
        // PreVote RPCs are sent for the next term before we've incremented r.Term.
        term = term_ + 1;
    }
    else {
        become_candidate();
        vote_msg = proto::MsgVote;
        term = term_;
    }

    if (quorum() == poll(id_, vote_msg, true)) {
        // We won the election after voting for ourselves (which must mean that
        // this is a single-node cluster). Advance to the next state.
        if (campaign_type == kCampaignPreElection) {
            campaign(kCampaignElection);
        }
        else {
            become_leader();
        }
        return;
    }

    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        if (it->first == id_) {
            continue;
        }

        LOG_INFO("%lu [log_term: %lu, index: %lu] sent %s request to %lu at term %lu",
                 id_,
                 raft_log_->last_term(),
                 raft_log_->last_index(),
                 proto::msg_type_to_string(vote_msg),
                 it->first,
                 term_);

        std::vector<uint8_t> ctx;
        if (campaign_type == kCampaignTransfer) {
            ctx = std::vector<uint8_t>(kCampaignTransfer.begin(), kCampaignTransfer.end());
        }
        proto::MessagePtr msg(new proto::Message());
        msg->term = term;
        msg->to = it->first;
        msg->type = vote_msg;
        msg->index = raft_log_->last_index();
        msg->term = raft_log_->last_term();
        msg->context = std::move(ctx);

        send(std::move(msg));
    }
}

uint32_t Raft::poll(uint64_t id, proto::MessageType type, bool v)
{
    uint32_t granted = 0;
    if (v) {
        LOG_INFO("%lu received %s from %lu at term %lu", id_, proto::msg_type_to_string(type), id, term_);
    }
    else {
        LOG_INFO("%lu received %s rejection from %lu at term %lu", id_, proto::msg_type_to_string(type), id, term_);
    }

    auto it = votes_.find(id);
    if (it == votes_.end()) {
        votes_[id] = v;
    }

    for (it = votes_.begin(); it != votes_.end(); ++it) {
        if (it->second) {
            granted++;
        }
    }
    return granted;
}

Status Raft::step(proto::MessagePtr msg)
{
    if (msg->term == 0) {
        LOG_INFO("local msg");
    }
    else if (msg->term > term_) {
        if (msg->type == proto::MsgVote || msg->type == proto::MsgPreVote) {
            bool force = (Slice((const char*) msg->context.data(), msg->context.size()) == Slice(kCampaignTransfer));
            bool in_lease = (check_quorum_ && lead_ != 0 && election_elapsed_ < election_timeout_);
            if (!force && in_lease) {
                // If a server receives a RequestVote request within the minimum election timeout
                // of hearing from a current leader, it does not update its term or grant its vote
                LOG_INFO(
                    "%lu [log_term: %lu, index: %lu, vote: %lu] ignored %s from %lu [log_term: %lu, index: %lu] at term %lu: lease is not expired (remaining ticks: %d)",
                    id_,
                    raft_log_->last_term(),
                    raft_log_->last_index(),
                    vote_,
                    proto::msg_type_to_string(msg->type),
                    msg->from,
                    msg->log_term,
                    msg->index,
                    term_,
                    election_timeout_ - election_elapsed_);
                return Status::ok();
            }
        }

        if (msg->type == proto::MsgPreVote) {
            // Never change our term in response to a PreVote
        }
        else if (msg->type == proto::MsgPreVoteResp && !msg->reject) {
            // We send pre-vote requests with a term in our future. If the
            // pre-vote is granted, we will increment our term when we get a
            // quorum. If it is not, the term comes from the node that
            // rejected our vote so we should become a follower at the new
            // term.
        }
        else {
            LOG_INFO("%lu [term: %lu] received a %s message with higher term from %lu [term: %lu]",
                     id_, term_,
                     proto::msg_type_to_string(msg->type),
                     msg->from,
                     msg->term);

            if (msg->type == proto::MsgApp || msg->type == proto::MsgHeartbeat || msg->type == proto::MsgSnap) {
                become_follower(msg->term, msg->from);
            }
            else {
                become_follower(msg->term, 0);
            }
        }
    }
    else if (msg->term < term_) {
        if ((check_quorum_ || pre_vote_) && (msg->type == proto::MsgHeartbeat || msg->type == proto::MsgApp)) {
            // We have received messages from a leader at a lower term. It is possible
            // that these messages were simply delayed in the network, but this could
            // also mean that this node has advanced its term number during a network
            // partition, and it is now unable to either win an election or to rejoin
            // the majority on the old term. If checkQuorum is false, this will be
            // handled by incrementing term numbers in response to MsgVote with a
            // higher term, but if checkQuorum is true we may not advance the term on
            // MsgVote and must generate other messages to advance the term. The net
            // result of these two features is to minimize the disruption caused by
            // nodes that have been removed from the cluster's configuration: a
            // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
            // but it will not receive MsgApp or MsgHeartbeat, so it will not create
            // disruptive term increases, by notifying leader of this node's activeness.
            // The above comments also true for Pre-Vote
            //
            // When follower gets isolated, it soon starts an election ending
            // up with a higher term than leader, although it won't receive enough
            // votes to win the election. When it regains connectivity, this response
            // with "pb.MsgAppResp" of higher term would force leader to step down.
            // However, this disruption is inevitable to free this stuck node with
            // fresh election. This can be prevented with Pre-Vote phase.
            proto::MessagePtr m(new proto::Message());
            m->to = msg->from;
            m->type = proto::MsgAppResp;
            send(std::move(m));
        }
        else if (msg->type == proto::MsgPreVote) {
            // Before Pre-Vote enable, there may have candidate with higher term,
            // but less log. After update to Pre-Vote, the cluster may deadlock if
            // we drop messages with a lower term.
            LOG_INFO(
                "%lu [log_term: %lu, index: %lu, vote: %lu] rejected %s from %lu [log_term: %lu, index: %lu] at term %lu",
                id_,
                raft_log_->last_term(),
                raft_log_->last_index(),
                vote_,
                proto::msg_type_to_string(msg->type),
                msg->from,
                msg->log_term,
                msg->index,
                term_);
            proto::MessagePtr m(new proto::Message());
            m->to = msg->from;
            m->type = proto::MsgPreVoteResp;
            m->reject = true;
            m->term = term_;
            send(std::move(m));
        }
        else {
            // ignore other cases
            LOG_INFO("%lu [term: %lu] ignored a %s message with lower term from %lu [term: %lu]",
                     id_, term_, proto::msg_type_to_string(msg->type), msg->from, msg->term);
        }
        return Status::ok();
    }

    switch (msg->type) {
    case proto::MsgHup: {
        if (state_ != RaftState::Leader) {
            std::vector<proto::EntryPtr> entries;
            Status status =
                raft_log_->slice(raft_log_->applied() + 1, raft_log_->committed() + 1, RaftLog::unlimited(), entries);
            if (!status.is_ok()) {
                LOG_FATAL("unexpected error getting unapplied entries (%s)", status.to_string().c_str());
            }

            uint32_t pending = num_of_pending_conf(entries);
            if (pending > 0 && raft_log_->committed() > raft_log_->applied()) {
                LOG_WARN(
                    "%lu cannot campaign at term %lu since there are still %u pending configuration changes to apply",
                    id_,
                    term_,
                    pending);
                return Status::ok();
            }
            LOG_INFO("%lu is starting a new election at term %lu", id_, term_);
            if (pre_vote_) {
                campaign(kCampaignPreElection);
            }
            else {
                campaign(kCampaignElection);
            }
        }
        else {
            LOG_DEBUG("%lu ignoring MsgHup because already leader", id_);
        }
        break;
    }
    case proto::MsgVote:
    case proto::MsgPreVote: {
        if (is_learner_) {
            // TODO: learner may need to vote, in case of node down when confchange.
            LOG_INFO(
                "%lu [log_term: %lu, index: %lu, vote: %lu] ignored %s from %lu [log_term: %lu, index: %lu] at term %lu: learner can not vote",
                id_,
                raft_log_->last_term(),
                raft_log_->last_index(),
                vote_,
                proto::msg_type_to_string(msg->type),
                msg->from,
                msg->log_term,
                msg->index,
                msg->term);
            return Status::ok();
        }
        // We can vote if this is a repeat of a vote we've already cast...
        bool can_vote = vote_ == msg->from ||
            // ...we haven't voted and we don't think there's a leader yet in this term...
            (vote_ == 0 && lead_ == 0) ||
            // ...or this is a PreVote for a future term...
            (msg->type == proto::MsgPreVote && msg->term > term_);
        // ...and we believe the candidate is up to date.
        if (can_vote && this->raft_log_->is_up_to_date(msg->index, msg->term)) {
            LOG_INFO(
                "%lu [log_term: %lu, index: %lu, vote: %lu] cast %s for %lu [log_term: %lu, index: %lu] at term %lu",
                id_,
                raft_log_->last_term(),
                raft_log_->last_index(),
                vote_,
                proto::msg_type_to_string(msg->type),
                msg->from,
                msg->log_term,
                msg->index,
                msg->term);
            // When responding to Msg{Pre,}Vote messages we include the term
            // from the message, not the local term. To see why consider the
            // case where a single node was previously partitioned away and
            // it's local term is now of date. If we include the local term
            // (recall that for pre-votes we don't update the local term), the
            // (pre-)campaigning node on the other end will proceed to ignore
            // the message (it ignores all out of date messages).
            // The term in the original message and current local term are the
            // same in the case of regular votes, but different for pre-votes.

            proto::MessagePtr m(new proto::Message());
            m->to = msg->from;
            m->term = msg->term;
            m->type = vote_resp_msg_type(msg->type);
            send(std::move(m));

            if (msg->type == proto::MsgVote) {
                // Only record real votes.
                election_elapsed_ = 0;
                vote_ = msg->from;
            }
        }
        else {
            LOG_INFO(
                "%lu [log_term: %lu, index: %lu, vote: %lu] rejected %s from %lu [log_term: %lu, index: %lu] at term %lu",
                id_,
                raft_log_->last_term(),
                raft_log_->last_index(),
                vote_,
                proto::msg_type_to_string(msg->type),
                msg->from,
                msg->log_term,
                msg->index,
                msg->term);

            proto::MessagePtr m(new proto::Message());
            m->to = msg->from;
            m->term = term_;
            m->type = vote_resp_msg_type(msg->type);
            m->reject = true;
            send(std::move(m));
        }

        break;
    }
    default: {
        return step_(msg);
    }
    }

    return Status::ok();
}

Status Raft::step_leader(proto::MessagePtr msg)
{
    LOG_WARN("no impl yet");
    return Status::ok();
}

Status Raft::step_candidate(proto::MessagePtr msg)
{
    // Only handle vote responses corresponding to our candidacy (while in
    // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
    // our pre-candidate state).
    switch (msg->type) {
    case proto::MsgProp:LOG_INFO("%lu no leader at term %lu; dropping proposal", id_, term_);
        return Status::invalid_argument("raft proposal dropped");
    case proto::MsgApp:become_follower(msg->term, msg->from); // always m.Term == r.Term
        handle_append_entries(std::move(msg));
        break;
    case proto::MsgHeartbeat:become_follower(msg->term, msg->from);  // always m.Term == r.Term
        handle_heartbeat(std::move(msg));
        break;
    case proto::MsgSnap:become_follower(msg->term, msg->from); // always m.Term == r.Term
        handle_snapshot(std::move(msg));
        break;
    case proto::MsgPreVoteResp:
    case proto::MsgVoteResp: {
        uint64_t gr = poll(msg->from, msg->type, !msg->reject);
        LOG_INFO("%lu [quorum:%u] has received %lu %s votes and %lu vote rejections",
                 id_,
                 quorum(),
                 gr,
                 proto::msg_type_to_string(msg->type),
                 votes_.size() - gr);
        if (gr == quorum()) {
            if (state_ == RaftState::PreCandidate) {
                campaign(kCampaignElection);
            }
            else {
                assert(state_ == RaftState::Candidate);
                become_leader();
                bcast_append();
            }
        }
        else if (gr == votes_.size() - gr) {
            // pb.MsgPreVoteResp contains future term of pre-candidate
            // m.Term > r.Term; reuse r.Term
            become_follower(term_, 0);
        }
        break;
    }
    case proto::MsgTimeoutNow: LOG_DEBUG("%lu [term %lu state %d] ignored MsgTimeoutNow from %lu",
                                         id_,
                                         term_,
                                         state_,
                                         msg->from);
    }
    return Status::ok();
}

void Raft::send(proto::MessagePtr msg)
{
    msg->from = id_;
    if (msg->type == proto::MsgVote || msg->type == proto::MsgVoteResp || msg->type == proto::MsgPreVote
        || msg->type == proto::MsgPreVoteResp) {
        if (msg->term == 0) {
            // All {pre-,}campaign messages need to have the term set when
            // sending.
            // - MsgVote: m.Term is the term the node is campaigning for,
            //   non-zero as we increment the term when campaigning.
            // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
            //   granted, non-zero for the same reason MsgVote is
            // - MsgPreVote: m.Term is the term the node will campaign,
            //   non-zero as we use m.Term to indicate the next term we'll be
            //   campaigning for
            // - MsgPreVoteResp: m.Term is the term received in the original
            //   MsgPreVote if the pre-vote was granted, non-zero for the
            //   same reasons MsgPreVote is
            LOG_FATAL("term should be set when sending %d", msg->type);
        }
    }
    else {
        if (msg->term != 0) {
            LOG_FATAL("term should not be set when sending %d (was %lu)", msg->type, msg->term);
        }
        // do not attach term to MsgProp, MsgReadIndex
        // proposals are a way to forward to the leader and
        // should be treated as local message.
        // MsgReadIndex is also forwarded to leader.
        if (msg->type != proto::MsgProp && msg->type != proto::MsgReadIndex) {
            msg->term = term_;
        }
    }
    msgs_.push_back(std::move(msg));
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
    ProgressPtr pr = get_progress(id);
    if (pr == nullptr) {
        set_progress(id, 0, raft_log_->last_index() + 1, is_learner);
    }
    else {

        if (is_learner && !pr->is_learner) {
            // can only change Learner to Voter
            LOG_INFO("%lu ignored addLearner: do not support changing %lu from raft peer to learner.", id_, id);
            return;
        }

        if (is_learner == pr->is_learner) {
            // Ignore any redundant addNode calls (which can happen because the
            // initial bootstrapping entries are applied twice).
            return;
        }

        // change Learner to Voter, use origin Learner progress
        learner_prs_.erase(id);
        pr->is_learner = false;
        prs_[id] = pr;
    }

    if (id_ == id) {
        is_learner_ = is_learner;
    }

    // When a node is first added, we should mark it as recently active.
    // Otherwise, CheckQuorum may cause us to step down if it is invoked
    // before the added node has a chance to communicate with us.
    get_progress(id)->recent_active = true;
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

void Raft::handle_snapshot(proto::MessagePtr msg)
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

    it = learner_prs_.find(id);
    if (it != learner_prs_.end()) {
        return it->second;
    }
    return nullptr;
}

void Raft::set_progress(uint64_t id, uint64_t match, uint64_t next, bool is_learner)
{
    if (!is_learner) {
        learner_prs_.erase(id);
        ProgressPtr progress(new Progress(max_inflight_));
        progress->next = next;
        progress->match = match;
        prs_[id] = progress;
        return;
    }

    auto it = prs_.find(id);
    if (it != prs_.end()) {
        LOG_FATAL("%lu unexpected changing from voter to learner for %lu", id_, id);
    }

    ProgressPtr progress(new Progress(max_inflight_));
    progress->next = next;
    progress->match = match;
    progress->is_learner = true;

    learner_prs_[id] = progress;
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
    if (uncommitted_size_ == 0) {
        // Fast-path for followers, who do not track or enforce the limit.
        return;
    }

    uint32_t size = 0;

    for (const proto::EntryPtr& e: entries) {
        size += e->payload_size();
    }
    if (size > uncommitted_size_) {
        // uncommittedSize may underestimate the size of the uncommitted Raft
        // log tail but will never overestimate it. Saturate at 0 instead of
        // allowing overflow.
        uncommitted_size_ = 0;
    }
    else {
        uncommitted_size_ -= size;
    }
}

}