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
            LOG_WARN("------------------------------------%s", proto::msg_type_to_string(i));
            ASSERT_TRUE(status.is_ok());
        }
    }
}


int main(int argc, char* argv[])
{
    //testing::GTEST_FLAG(filter) = "raft.OldMessages";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
