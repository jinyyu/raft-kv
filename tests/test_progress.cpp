#include <gtest/gtest.h>
#include <kvd/raft/Progress.h>

using namespace kvd;


static bool cmp_InFlights(const InFlights& l, const InFlights& r)
{
    return l.start == r.start && l.count == r.count && l.size == r.size && l.buffer == r.buffer;
}

TEST(progress, add)
{

    InFlights in(10);
    in.buffer.resize(10, 0);

    for (uint32_t i = 0; i < 5; i++) {
        in.add(i);
    }

    InFlights wantIn(10);
    wantIn.start = 0;
    wantIn.count = 5;
    wantIn.buffer = std::vector<uint64_t>{0, 1, 2, 3, 4, 0, 0, 0, 0, 0};

    ASSERT_TRUE(cmp_InFlights(wantIn, in));


    InFlights wantIn2(10);
    wantIn.start = 0;
    wantIn.count = 10;
    wantIn.buffer = std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ASSERT_FALSE(cmp_InFlights(wantIn2, in));

    // rotating case
    InFlights in2(10);
    in2.start = 5;
    in2.size = 10;
    in2.buffer.resize(10, 0);

    for (uint32_t i = 0; i < 5; i++) {
        in2.add(i);
    }

    InFlights wantIn21(10);
    wantIn.start = 5;
    wantIn.count = 5;
    wantIn.buffer = std::vector<uint64_t>{0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
    ASSERT_FALSE(cmp_InFlights(wantIn2, in2));

    for (uint32_t i = 0; i < 5; i++) {
        in2.add(i);
    }

    InFlights wantIn22(10);
    wantIn.start = 10;
    wantIn.count = 10;
    wantIn.buffer = std::vector<uint64_t>{5, 6, 7, 8, 9, 0, 1, 2, 3, 4};
    ASSERT_FALSE(cmp_InFlights(wantIn2, in2));

    ASSERT_FALSE(cmp_InFlights(wantIn22, in2));
}

TEST(progress, freeto)
{
    InFlights in(10);

    for (uint32_t i = 0; i < 10; i++) {
        in.add(i);
    }
    in.free_to(4);

    InFlights wantIn(10);
    wantIn.start = 5;
    wantIn.count = 5;
    wantIn.buffer = std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    ASSERT_TRUE(cmp_InFlights(wantIn, in));

    in.free_to(8);

    InFlights wantIn2(10);
    wantIn2.start = 9;
    wantIn2.count = 1;
    wantIn2.buffer = std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ASSERT_TRUE(cmp_InFlights(wantIn2, in));

    // rotating case
    for (uint32_t i = 10; i < 15; i++) {
        in.add(i);
    }

    in.free_to(12);

    InFlights wantIn3(10);
    wantIn3.start = 3;
    wantIn3.count = 2;
    wantIn3.size = 10;
    wantIn3.buffer = std::vector<uint64_t>{10, 11, 12, 13, 14, 5, 6, 7, 8, 9};
    ASSERT_TRUE(cmp_InFlights(wantIn3, in));

    in.free_to(14);

    InFlights wantIn4(10);
    wantIn4.start = 0;
    wantIn4.count = 0;
    wantIn4.size = 10;
    wantIn4.buffer = std::vector<uint64_t>{10, 11, 12, 13, 14, 5, 6, 7, 8, 9};
    ASSERT_TRUE(cmp_InFlights(wantIn4, in));
}


TEST(progress, FreeFirstOne)
{
    InFlights in(10);
    for (uint32_t i = 0; i < 10; i++) {
        in.add(i);
    }
    in.free_first_one();

    InFlights wantIn(10);
    wantIn.start = 1;
    wantIn.count = 9;
    wantIn.buffer = std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ASSERT_TRUE(cmp_InFlights(wantIn, in));
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
