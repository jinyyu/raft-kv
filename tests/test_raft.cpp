#include <gtest/gtest.h>
#include <kvd/raft/Raft.h>
#include <kvd/common/log.h>
#include "network.hpp"


using namespace kvd;


static RaftPtr newTestRaft(uint64_t id,
                           std::vector<uint64_t> peers,
                           uint64_t election,
                           uint64_t heartbeat,
                           StoragePtr storage)
{
    Config c = newTestConfig(id, peers, election, heartbeat, storage);
    c.max_inflight_msgs = 256;
    Status status = c.validate();
    assert(status.is_ok());
    return std::make_shared<Raft>(c);
}

TEST(raft, ProgressLeader)
{
    auto r = newTestRaft(1, {1, 2}, 5, 1, std::make_shared<MemoryStorage>());
    r->become_candidate();
    r->become_leader();
    r->get_progress(2)->become_replicate();

    proto::MessagePtr propMsg(new proto::Message());
    propMsg->from = 1;
    propMsg->to = 1;
    propMsg->type = proto::MsgProp;
    proto::Entry e;
    e.data = std::vector<uint8_t>{'f', 'o', 'o'};
    propMsg->entries.push_back(e);


    // Send proposals to r1. The first 5 entries should be appended to the log.
    for (uint32_t i = 0; i < 5; i++) {
        LOG_INFO("ProgressLeader %u", i);
        auto pr = r->get_progress(r->id());
        ASSERT_TRUE(pr->state == ProgressStateReplicate);
        ASSERT_TRUE(pr->match = i + 1);
        ASSERT_TRUE(pr->next == pr->match + 1);
        Status status = r->step(propMsg);
        if (!status.is_ok()) {
            LOG_ERROR("proposal resulted in error: %s", status.to_string().c_str());
        }
        ASSERT_TRUE(status.is_ok());
    }
}

TEST(raft, ProgressResumeByHeartbeatResp)
{
    // ensures raft.heartbeat reset progress.paused by heartbeat response.
    auto r = newTestRaft(1, {1, 2}, 5, 1, std::make_shared<MemoryStorage>());
    r->become_candidate();
    r->become_leader();

    r->get_progress(2)->paused = true;

    proto::MessagePtr msg(new proto::Message());
    msg->from = 1;
    msg->to = 1;
    msg->type = proto::MsgBeat;
    Status status = r->step(msg);
    ASSERT_TRUE(status.is_ok());

    ASSERT_TRUE(r->get_progress(2)->paused);

    r->get_progress(2)->become_replicate();

    proto::MessagePtr m2(new proto::Message());
    m2->from = 2;
    m2->to = 1;
    m2->type = proto::MsgHeartbeatResp;
    status = r->step(m2);
    ASSERT_FALSE(r->get_progress(2)->paused);
}

TEST(raft, ProgressPaused)
{
    auto r = newTestRaft(1, {1, 2}, 5, 1, std::make_shared<MemoryStorage>());
    r->become_candidate();
    r->become_leader();

    proto::MessagePtr msg(new proto::Message());
    msg->from = 1;
    msg->to = 1;
    msg->type = proto::MsgProp;
    proto::Entry e;
    e.data = std::vector<uint8_t>{'f', 'o', 'o'};
    msg->entries.push_back(e);
    r->step(msg);
    r->step(msg);
    r->step(msg);

    auto msgs = r->msgs();
    ASSERT_TRUE(r->msgs().size() == 1);
}

TEST(raft, ProgressFlowControl)
{
    auto c = newTestConfig(1, {1, 2}, 5, 1, std::make_shared<MemoryStorage>());
    c.max_inflight_msgs = 3;
    c.max_size_per_msg = 2048;
    RaftPtr r(new Raft(c));
    r->become_candidate();
    r->become_leader();

    // Throw away all the messages relating to the initial election.
    r->msgs().clear();

    // While node 2 is in probe state, propose a bunch of entries.
    r->get_progress(2)->become_probe();


    for (size_t i = 0; i < 10; i++) {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 1;
        msg->to = 1;
        msg->type = proto::MsgProp;
        proto::Entry e;
        e.data.resize(1000, 'a');
        msg->entries.push_back(e);
        r->step(msg);
    }
    auto ms = r->msgs();
    r->msgs().clear();

    // First append has two entries: the empty entry to confirm the
    // election, and the first proposal (only one proposal gets sent
    // because we're in probe state).
    ASSERT_TRUE(ms.size() == 1);
    ASSERT_TRUE(ms[0]->type == proto::MsgApp);

    ASSERT_TRUE(ms[0]->entries.size() == 2);
    ASSERT_TRUE(ms[0]->entries[0].data.empty());
    ASSERT_TRUE(ms[0]->entries[1].data.size() == 1000);

    // When this append is acked, we change to replicate state and can
    // send multiple messages at once.
    {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 2;
        msg->to = 1;
        msg->type = proto::MsgAppResp;
        msg->index = ms[0]->entries[1].index;
        r->step(msg);
    }
    ms = r->msgs();
    r->msgs().clear();
    ASSERT_TRUE(ms.size() == 3);

    for (size_t i = 0; i < ms.size(); ++i) {
        ASSERT_TRUE(ms[i]->type == proto::MsgApp);
        ASSERT_TRUE(ms[i]->entries.size() == 2);
    }

    // Ack all three of those messages together and get the last two
    // messages (containing three entries).
    {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 2;
        msg->to = 1;
        msg->type = proto::MsgAppResp;
        msg->index = ms[2]->entries[1].index;
        r->step(msg);
    }

    ms = r->msgs();
    r->msgs().clear();

    ASSERT_TRUE(ms.size() == 2);
    for (size_t i = 0; i < ms.size(); ++i) {
        ASSERT_TRUE(ms[i]->type == proto::MsgApp);
    }

    ASSERT_TRUE(ms[0]->entries.size() == 2);
    ASSERT_TRUE(ms[1]->entries.size() == 1);
}

TEST(raft, UncommittedEntryLimit)
{
    // Use a relatively large number of entries here to prevent regression of a
    // bug which computed the size before it was fixed. This test would fail
    // with the bug, either because we'd get dropped proposals earlier than we
    // expect them, or because the final tally ends up nonzero. (At the time of
    // writing, the former).
    uint64_t maxEntries = 1024;
    proto::Entry testEntry;
    testEntry.data.resize(8, 'a');
    uint64_t maxEntrySize = maxEntries * testEntry.payload_size();

    auto cfg = newTestConfig(1, {1, 2, 3}, 5, 1, std::make_shared<MemoryStorage>());
    cfg.max_uncommitted_entries_size = maxEntrySize;
    cfg.max_inflight_msgs = 2 * 1024; // avoid interference
    RaftPtr r(new Raft(cfg));
    r->become_candidate();
    r->become_leader();

    ASSERT_TRUE(r->uncommitted_size() == 0);

    // Set the two followers to the replicate state. Commit to tail of log.
    uint64_t numFollowers = 2;
    r->get_progress(2)->become_replicate();
    r->get_progress(3)->become_replicate();
    r->uncommitted_size() = 0;

    proto::MessagePtr propMsg(new proto::Message());
    propMsg->from = 1;
    propMsg->to = 1;
    propMsg->type = proto::MsgProp;
    propMsg->entries.push_back(testEntry);

    // Send proposals to r1. The first 5 entries should be appended to the log.
    std::vector<proto::EntryPtr> propEnts;

    for (uint64_t i = 0; i < maxEntries; i++) {
        Status status = r->step_leader(propMsg);
        ASSERT_TRUE(status.is_ok());
        propEnts.push_back(std::make_shared<proto::Entry>(testEntry));
    }

    // Send one more proposal to r1. It should be rejected.
    Status status = r->step(propMsg);
    ASSERT_FALSE(status.is_ok());
    fprintf(stderr, "status :%s\n", status.to_string().c_str());

    auto ms = r->msgs();
    r->msgs().clear();
    // Read messages and reduce the uncommitted size as if we had committed
    // these entries.

    ASSERT_TRUE(ms.size() == maxEntries * numFollowers);

    r->reduce_uncommitted_size(propEnts);
    ASSERT_TRUE(r->uncommitted_size() == 0);

    // Send a single large proposal to r1. Should be accepted even though it
    // pushes us above the limit because we were beneath it before the proposal.

    propEnts.resize(2 * maxEntries);

    for (size_t i = 0; i < propEnts.size(); ++i) {
        propEnts[i] = std::make_shared<proto::Entry>(testEntry);
    }

    proto::MessagePtr propMsgLarge(new proto::Message());
    propMsgLarge->from = 1;
    propMsgLarge->to = 1;
    propMsgLarge->type = proto::MsgProp;
    for (size_t i = 0; i < propEnts.size(); ++i) {
        propMsgLarge->entries.push_back(*propEnts[i]);
    }

    status = r->step(propMsgLarge);
    ASSERT_TRUE(status.is_ok());


    // Send one more proposal to r1. It should be rejected, again.
    status = r->step(propMsg);
    ASSERT_FALSE(status.is_ok());
    fprintf(stderr, "status :%s\n", status.to_string().c_str());


    // Read messages and reduce the uncommitted size as if we had committed
    // these entries.
    ms = r->msgs();
    r->msgs().clear();
    ASSERT_TRUE(ms.size() == numFollowers * 1);
    r->reduce_uncommitted_size(propEnts);
    ASSERT_TRUE(r->uncommitted_size() == 0);
}

void testLeaderElection(bool preVote)
{
    ConfigFunc cfg = [](Config&) {};
    RaftState candState = RaftState::Candidate;
    uint64_t candTerm = 1;
    if (preVote) {
        cfg = preVoteConfig;
        // In pre-vote mode, an election that fails to complete
        // leaves the node in pre-candidate state without advancing
        // the term.
        candState = RaftState::PreCandidate;
        candTerm = 0;
    }

    struct Test
    {
        NetworkPtr network;
        RaftState state;
        uint64_t expTerm;
    };

    std::vector<Test> tests;

    {
        std::vector<RaftPtr> peers{nullptr, nullptr, nullptr};
        Test t{.network = std::make_shared<Network>(cfg, peers), .state = RaftState::Leader, .expTerm = 1};
        tests.push_back(t);
    }


    for (size_t i = 0; i < tests.size(); ++i) {
        Test& test = tests[i];
        std::vector<proto::MessagePtr> msgs;
        proto::MessagePtr m(new proto::Message());
        m->from = 1;
        m->to = 1;
        m->type = proto::MsgHup;
        msgs.push_back(m);
        test.network->send(msgs);

        auto sm = test.network->peers[1];
        ASSERT_TRUE(sm->state_ == test.state);
        ASSERT_TRUE(sm->term_ == test.expTerm);
    }
}

TEST(raft, LeaderElection)
{
    testLeaderElection(false);
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
