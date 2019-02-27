#include <gtest/gtest.h>
#include <kvd/raft/Node.h>
#include <kvd/raft/util.h>
#include "network.hpp"

using namespace kvd;

static bool read_state_cmp(const std::vector<ReadState>& l, const std::vector<ReadState>& r)
{
    if (l.size() != r.size()) {
        return false;
    }

    for (size_t i = 0; i < l.size(); ++i) {
        if (l[i].index != r[i].index) {
            return false;
        }

        if (l[i].request_ctx != r[i].request_ctx) {
            return false;
        }
    }
    return true;
}

TEST(node, RawNodeStep)
{
    for (uint8_t i = 0; i < proto::MsgTypeSize; ++i) {
        MemoryStoragePtr s(new MemoryStorage());
        auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);

        std::vector<PeerContext> nodes{PeerContext{.id = 1}};
        RawNode node(c, nodes);
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
    MemoryStoragePtr s(new MemoryStorage());
    auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);
    std::vector<PeerContext> peer{PeerContext{.id = 1}};
    RawNode rawNode(c, peer);
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

TEST(node, RawNodeProposeAddDuplicateNode)
{
    MemoryStoragePtr s(new MemoryStorage());
    auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);
    std::vector<PeerContext> peer{PeerContext{.id = 1}};
    RawNode rawNode(c, peer);

    auto rd = rawNode.ready();
    s->append(rd->entries);
    rawNode.advance(rd);

    rawNode.campaign();
    while (true) {
        rd = rawNode.ready();
        s->append(rd->entries);

        if (rd->soft_state->lead == rawNode.raft_->id_) {
            rawNode.advance(rd);
            break;
        }
        rawNode.advance(rd);
    }

    auto proposeConfChangeAndApply = [&rawNode, s](const proto::ConfChange& cc) {
        rawNode.propose_conf_change(cc);
        auto rd = rawNode.ready();
        s->append(rd->entries);

        for (auto entry : rd->committed_entries) {
            if (entry->type == proto::EntryConfChange) {
                proto::ConfChange cc;
                proto::ConfChange::from_data(entry->data, cc);
                rawNode.apply_conf_change(cc);
            }
        }
        rawNode.advance(rd);
    };

    proto::ConfChange cc1;
    cc1.conf_change_type = proto::ConfChangeAddNode;
    cc1.node_id = 1;
    auto ccdata1 = cc1.serialize();

    proposeConfChangeAndApply(cc1);

    // try to add the same node again
    proposeConfChangeAndApply(cc1);

    // the new node join should be ok
    proto::ConfChange cc2;
    cc2.conf_change_type = proto::ConfChangeAddNode;
    cc2.node_id = 2;
    auto ccdata2 = cc2.serialize();

    proposeConfChangeAndApply(cc2);

    uint64_t lastIndex;
    Status status = s->last_index(lastIndex);
    ASSERT_TRUE(status.is_ok());

    // the last three entries should be: ConfChange cc1, cc1, cc2

    std::vector<proto::EntryPtr> entries;
    status = s->entries(lastIndex - 2, lastIndex + 1, RaftLog::unlimited(), entries);
    ASSERT_TRUE(status.is_ok());

    ASSERT_TRUE(entries.size() == 3);

    ASSERT_TRUE(entries[0]->data == ccdata1);

    ASSERT_TRUE(entries[2]->data == ccdata2);
}

TEST(node, RawNodeReadIndex)
{
    std::vector<proto::MessagePtr> msgs;
    auto appendStep = [&msgs](proto::MessagePtr msg) {
        msgs.push_back(msg);
        return Status::ok();
    };

    std::vector<ReadState> wrs;
    wrs.push_back(ReadState{.index = 1, .request_ctx = str_to_vector("somedata")});
    MemoryStoragePtr s(new MemoryStorage());

    auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);

    std::vector<PeerContext> peer{PeerContext{.id = 1}};
    RawNode rawNode(c, peer);

    rawNode.raft_->read_states_ = wrs;
    // ensure the ReadStates can be read out

    ASSERT_TRUE(rawNode.has_ready());
    auto rd = rawNode.ready();

    ASSERT_TRUE(read_state_cmp(rd->read_states, wrs));

    s->append(rd->entries);
    rawNode.advance(rd);
    // ensure raft.readStates is reset after advance
    ASSERT_TRUE(rawNode.raft_->read_states_.empty());

    auto wrequestCtx = str_to_vector("somedata2");
    rawNode.campaign();
    while (true) {
        rd = rawNode.ready();
        s->append(rd->entries);
        if (rd->soft_state->lead == rawNode.raft_->id_) {
            rawNode.advance(rd);

            // Once we are the leader, issue a ReadIndex request
            rawNode.raft_->step_ = appendStep;
            rawNode.read_index(wrequestCtx);
            break;
        }
        rawNode.advance(rd);
    }
    // ensure that MsgReadIndex message is sent to the underlying raft
    ASSERT_TRUE(msgs.size() == 1);
    ASSERT_TRUE(msgs[0]->type == proto::MsgReadIndex);
    ASSERT_TRUE(msgs[0]->entries[0].data == wrequestCtx);
}

int main(int argc, char* argv[])
{
    //testing::GTEST_FLAG(filter) = "raft.OldMessages";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
