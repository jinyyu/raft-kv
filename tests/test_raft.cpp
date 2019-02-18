#include <gtest/gtest.h>
#include <kvd/raft/Raft.h>
#include <kvd/common/log.h>

using namespace kvd;

static Config newTestConfig(uint64_t id,
                            std::vector<uint64_t> peers,
                            uint32_t election,
                            uint32_t heartbeat,
                            StoragePtr storage)
{
    Config c;
    c.id = id;
    c.peers = peers;
    c.election_tick = election;
    c.heartbeat_tick = heartbeat;
    c.storage = storage;
    return c;
}

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

    {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 1;
        msg->to = 1;
        msg->type = proto::MsgProp;
        proto::Entry e;
        e.data = std::vector<uint8_t>{'f', 'o', 'o'};
        msg->entries.push_back(e);
        r->step(msg);
    }
    {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 1;
        msg->to = 1;
        msg->type = proto::MsgProp;
        proto::Entry e;
        e.data = std::vector<uint8_t>{'f', 'o', 'o'};
        msg->entries.push_back(e);
        r->step(msg);
    }
    {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 1;
        msg->to = 1;
        msg->type = proto::MsgProp;
        proto::Entry e;
        e.data = std::vector<uint8_t>{'f', 'o', 'o'};
        msg->entries.push_back(e);
        r->step(msg);
    }

    auto msgs = r->msgs();
    fprintf(stderr, "===================%lu\n", msgs.size());
    ASSERT_TRUE(r->msgs().size() == 1);
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
