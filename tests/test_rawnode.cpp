#include <gtest/gtest.h>
#include <kvd/raft/Node.h>
#include <kvd/raft/util.h>
#include "network.hpp"

using namespace kvd;

TEST(node, RawNodeStep)
{
    boost::asio::io_service service;
    for (uint8_t i = 0; i < proto::MsgTypeSize; ++i) {
        MemoryStoragePtr s(new MemoryStorage());
        auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);

        std::vector<PeerContext> nodes{PeerContext{.id = 1}};
        RawNode node(c, nodes, service);
        proto::MessagePtr msg(new proto::Message());
        msg->type = i;
        Status status = node.step(msg);

        // LocalMsg should be ignored.
        if (is_local_msg(i)) {
            ASSERT_TRUE(!status.is_ok());
        }
    }
}

// RawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
TEST(node, RawNodeProposeAndConfChange)
{
    boost::asio::io_service service;
    MemoryStoragePtr s(new MemoryStorage());
    auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);
    std::vector<PeerContext> peer{PeerContext{.id = 1}};
    RawNode rawNode(c, peer, service);
    auto rd = rawNode.ready();
    s->append(rd->entries);
    rawNode.advance(rd);

    auto d = rawNode.ready();
    ASSERT_TRUE(!d->must_sync);
    ASSERT_TRUE(d->hard_state.is_empty_state());
    ASSERT_TRUE(d->entries.empty());

    rawNode.campaign();
    bool proposed = false;

    uint64_t lastIndex = 0;
    std::vector<uint8_t> ccdata;
    while (true) {
        rd = rawNode.ready();
        s->append(rd->entries);
        // Once we are the leader, propose a command and a ConfChange.
        if (!proposed && rd->soft_state->lead == rawNode.raft_->id_) {
            rawNode.propose(str_to_vector("somedata"));

            proto::ConfChange cc;
            cc.conf_change_type = proto::ConfChangeAddNode;
            cc.node_id = 1;
            ccdata = cc.serialize();

            rawNode.propose_conf_change(cc);

            proposed = true;
        }
        rawNode.advance(rd);

        // Exit when we have four entries: one ConfChange, one no-op for the election,
        // our proposed command and proposed ConfChange.
        Status status = s->last_index(lastIndex);
        ASSERT_TRUE(status.is_ok());
        if (lastIndex >= 4) {
            break;
        }
    }

    std::vector<proto::EntryPtr> entries;
    Status status = s->entries(lastIndex - 1, lastIndex + 1, RaftLog::unlimited(), entries);
    ASSERT_TRUE(status.is_ok());
    ASSERT_TRUE(entries.size() == 2);

    ASSERT_TRUE(entries[0]->data == str_to_vector("somedata"));
    ASSERT_TRUE(entries[1]->type == proto::EntryConfChange);
    ASSERT_TRUE(entries[1]->data == ccdata);
}

int main(int argc, char* argv[])
{
    //testing::GTEST_FLAG(filter) = "raft.OldMessages";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
