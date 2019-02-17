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
        if (i > 1) {
            return;
        }
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

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
