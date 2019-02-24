#include <gtest/gtest.h>
#include <kvd/raft/Raft.h>
#include <kvd/common/log.h>
#include <kvd/raft/util.h>
#include "network.hpp"

using namespace kvd;

TEST(raft, SingleNodeCandidate)
{
    std::vector<RaftPtr> peers{nullptr};
    Network tt(peers);
    {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 1;
        msg->to = 1;
        msg->type = proto::MsgHup;
        tt.send(msg);
    }
    ASSERT_TRUE(tt.peers[1]->state_ == RaftState::Leader);
}

TEST(raft, SingleNodePreCandidate)
{
    std::vector<RaftPtr> peers{nullptr};
    Network tt(preVoteConfig, peers);
    {
        proto::MessagePtr msg(new proto::Message());
        msg->from = 1;
        msg->to = 1;
        msg->type = proto::MsgHup;
        tt.send(msg);
    }
    ASSERT_TRUE(tt.peers[1]->state_ == RaftState::Leader);
}

int main(int argc, char* argv[])
{
    //testing::GTEST_FLAG(filter) = "raft.DuelingCandidates";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
