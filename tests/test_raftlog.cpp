#include <gtest/gtest.h>
#include <kvd/raft/RaftLog.h>
#include <kvd/common/log.h>

using namespace kvd;


static proto::EntryPtr newEntry(uint64_t index, uint64_t term)
{
    proto::EntryPtr e(new proto::Entry());
    e->index = index;
    e->term = term;
    return e;
}

bool entry_cmp(const std::vector<proto::EntryPtr>& left, const std::vector<proto::EntryPtr>& right)
{
    if (left.size() != right.size()) {
        return false;
    }

    for (size_t i = 0; i < left.size(); ++i) {
        if (left[i]->index != right[i]->index) {
            return false;
        }

        if (left[i]->term != right[i]->term) {
            return false;
        }
    }
    return true;
}

TEST(raftlog, term)
{
    uint64_t offset = 100;
    uint64_t num = 100;
    MemoryStoragePtr storage(new MemoryStorage());
    RaftLog log(storage, std::numeric_limits<uint64_t>::max());
    return;

    for (uint64_t i = 1; i < num; i++) {
        std::vector<proto::EntryPtr> entries;
        entries.push_back(newEntry(offset + i, i));
        log.append(std::move(entries));
    }



    struct Test
    {
        uint64_t Index;
        uint64_t w;
    };

    Test tests[] = {
        {offset - 1, 0},
 //       {offset, 0},
  //      {offset + num/2, num / 2},
 //       {offset + num - 1, num - 1},
 //       {offset + num, 0},
    };

    for (size_t i = 0; i < sizeof(tests); ++i) {
        LOG_INFO("testing term %lu", i);
        uint64_t term;
        log.term(tests[i].Index, term);

        ASSERT_TRUE(term == tests[i].w);
    }
}


TEST(raftlog, append)
{
    std::vector<proto::EntryPtr> previousEnts;
    previousEnts.push_back(newEntry(1, 1));
    previousEnts.push_back(newEntry(2, 2));

    struct Test
    {
        std::vector<proto::EntryPtr> ents;
        uint64_t windex;
        std::vector<proto::EntryPtr> wents;
        uint64_t wunstable;
    };

    std::vector<Test> tests;
    {

        std::vector<proto::EntryPtr> ents;

        std::vector<proto::EntryPtr> wents;
        wents.push_back(newEntry(1, 1));
        wents.push_back(newEntry(2, 2));

        tests.push_back(Test{.ents= ents, .windex = 2, .wents = wents, .wunstable = 3});
    }

    {
        std::vector<proto::EntryPtr> ents;
        ents.push_back(newEntry(3, 2));

        std::vector<proto::EntryPtr> wents;
        wents.push_back(newEntry(1, 1));
        wents.push_back(newEntry(2, 2));
        wents.push_back(newEntry(3, 2));

        tests.push_back(Test{.ents= ents, .windex = 3, .wents = wents, .wunstable = 3});
    }


    for (size_t i = 0; i < tests.size(); ++i) {
        MemoryStoragePtr storage(new MemoryStorage());
        storage->append(previousEnts);

        RaftLog l(storage, std::numeric_limits<uint64_t>::max());
        LOG_INFO("testing append %lu", i);

        auto& t = tests[i];

        uint64_t index = l.append(t.ents);
        ASSERT_TRUE(index == t.windex);

        std::vector<proto::EntryPtr> ents;
        Status status = l.entries(1, std::numeric_limits<uint64_t>::max(), ents);
        ASSERT_TRUE(status.is_ok());

        ASSERT_TRUE(entry_cmp(ents, t.wents));

        ASSERT_TRUE(l.unstable()->offset() == t.wunstable);

    }
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
