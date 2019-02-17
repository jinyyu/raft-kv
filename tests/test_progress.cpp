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

TEST(progress, BecomeProbe)
{
    struct Test
    {
        ProgressPtr pr;
        uint64_t wnext;
    };
    std::vector<Test> tests;
    {
        ProgressPtr pr(new Progress(256));
        pr->state = ProgressStateReplicate;
        pr->match = 1;
        pr->next = 5;
        tests.push_back(Test{.pr = pr, .wnext = 2});
    }

    {
        ProgressPtr pr(new Progress(256));
        pr->state = ProgressStateSnapshot;
        pr->match = 1;
        pr->next = 5;
        pr->pending_snapshot = 10;
        tests.push_back(Test{.pr = pr, .wnext = 11});
    }

    {
        ProgressPtr pr(new Progress(256));
        pr->state = ProgressStateSnapshot;
        pr->match = 1;
        pr->next = 5;
        pr->pending_snapshot = 0;
        tests.push_back(Test{.pr = pr, .wnext = 2});
    }

    for (Test& test : tests) {
        test.pr->become_probe();
        ASSERT_TRUE(test.pr->match == 1);
        ASSERT_TRUE(test.pr->state == ProgressStateProbe);
        ASSERT_TRUE(test.pr->next == test.wnext);
    }
}

TEST(progress, BecomeReplicate)
{
    ProgressPtr pr(new Progress(256));
    pr->match = 1;
    pr->next = 5;
    pr->become_replicate();
    ASSERT_TRUE(pr->next = pr->match + 1);
    ASSERT_TRUE(pr->state = ProgressStateReplicate);
}

TEST(progress, BecomeSnapshot)
{
    ProgressPtr pr(new Progress(256));
    pr->match = 1;
    pr->next = 5;
    pr->become_snapshot(10);
    ASSERT_TRUE(pr->match = 1);
    ASSERT_TRUE(pr->state = ProgressStateSnapshot);
    ASSERT_TRUE(pr->pending_snapshot = 10);
}

TEST(progress, Update)
{
    uint64_t prevM = 3;
    uint64_t prevN = 5;

    struct Test
    {
        uint64_t update;
        uint64_t wm;
        uint64_t wn;
        bool wok;
    };
    std::vector<Test> tests;
    tests.push_back(Test{.update = prevM - 1, .wm = prevM, .wn = prevN, .wok = false});
    tests.push_back(Test{.update = prevM, .wm = prevM, .wn = prevN, .wok = false});
    tests.push_back(Test{.update = prevM + 1, .wm = prevM + 1, .wn = prevN, .wok = true});
    tests.push_back(Test{.update = prevM + 2, .wm = prevM + 2, .wn = prevN + 1, .wok = true});

    for (Test& test: tests) {
        ProgressPtr pr(new Progress(256));
        pr->match = prevM;
        pr->next = prevN;

        bool ok = pr->maybe_update(test.update);
        ASSERT_TRUE(ok == test.wok);
        ASSERT_TRUE(pr->match = test.wm);
        ASSERT_TRUE(pr->next = test.wn);
    }
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
