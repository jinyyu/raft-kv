#include <gtest/gtest.h>
#include <kvd/raft/Raft.h>

using namespace kvd;

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
