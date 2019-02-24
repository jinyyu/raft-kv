#include <gtest/gtest.h>
#include <kvd/raft/Raft.h>
#include <kvd/common/log.h>
#include <kvd/raft/util.h>
#include "network.hpp"

using namespace kvd;


int main(int argc, char* argv[])
{
    //testing::GTEST_FLAG(filter) = "raft.DuelingCandidates";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
