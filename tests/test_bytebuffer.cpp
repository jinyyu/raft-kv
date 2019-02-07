#include <gtest/gtest.h>
#include <kvd/common/ByteBuffer.h>


using namespace kvd;

TEST(test_buffer, test_buffer)
{
    ByteBuffer buff;

    buff.put((const uint8_t *) "abc", 3);
    ASSERT_TRUE(buff.remain());
    ASSERT_TRUE(buff.remaining() == 3);

    buff.skip_bytes(1);
    char buffer[4096] = {0};
    memcpy(buffer, buff.reader(), buff.remaining());
    ASSERT_TRUE(buff.remaining() == 2);
    ASSERT_TRUE(buffer == std::string("bc"));

    buff.skip_bytes(2);
    ASSERT_TRUE(buff.remaining() == 0);

    fprintf(stderr, "%d\n", buff.capacity());
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
