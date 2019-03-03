#include <gtest/gtest.h>
#include <kvd/common/ByteBuffer.h>


using namespace kvd;

TEST(test_buffer, test_buffer)
{
    ByteBuffer buff;

    buff.put((const uint8_t*) "abc", 3);
    ASSERT_TRUE(buff.readable());
    ASSERT_TRUE(buff.readable_bytes() == 3);

    buff.read_bytes(1);
    char buffer[4096] = {0};
    memcpy(buffer, buff.reader(), buff.readable_bytes());
    ASSERT_TRUE(buff.readable_bytes() == 2);
    ASSERT_TRUE(buffer == std::string("bc"));
    ASSERT_TRUE(buff.slice().to_string() == "bc");

    buff.read_bytes(2);
    ASSERT_TRUE(buff.readable_bytes() == 0);

    ASSERT_TRUE(buff.slice().to_string() == "");
    fprintf(stderr, "%d\n", buff.capacity());

    buff.put((const uint8_t*) "123456", 6);
    ASSERT_TRUE(buff.slice().to_string() == "123456");
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

