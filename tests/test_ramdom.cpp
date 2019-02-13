#include <kvd/common/RandomDevice.h>
#include <gtest/gtest.h>


using namespace kvd;


TEST(random, random)
{

    for (uint32_t i = 0; i < 1024; ++i) {
        RandomDevice device(0, i);
        for (uint32_t j = 0; j < 4096; ++j) {

            uint32_t v = device.gen();
            ASSERT_TRUE(v >= 0);
            ASSERT_TRUE(v < 1024);

        }
    }

}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}