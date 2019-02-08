#include <gtest/gtest.h>
#include <kvd/raft/Storage.h>
using namespace kvd;


proto::EntryPtr newMemoryStorage(uint64_t term, uint64_t index)
{
    proto::EntryPtr ptr(new proto::Entry());
    ptr->term = term;
    ptr->index = index;
    return ptr;
}

TEST(storage, first_index)
{
    MemoryStorage m;
    m.ref_entries().clear();
    m.ref_entries().push_back(newMemoryStorage(3, 3));
    m.ref_entries().push_back(newMemoryStorage(4, 4));
    m.ref_entries().push_back(newMemoryStorage(5, 5));

    uint64_t first = 0;
    Status status = m.first_index(first);
    ASSERT_TRUE(first == 4);
    ASSERT_TRUE(status.is_ok());

    status = m.compact(4);
    ASSERT_TRUE(status.is_ok());
    m.first_index(first);
    ASSERT_TRUE(first == 5);

    status = m.compact(5);
    fprintf(stderr, "%s", status.to_string().c_str());
    ASSERT_TRUE(m.ref_entries().size() == 1);
    m.first_index(first);
    ASSERT_TRUE(first == 6);
}

TEST(storage, last_index)
{
    MemoryStorage m;
    m.ref_entries().clear();
    m.ref_entries().push_back(newMemoryStorage(3, 3));
    m.ref_entries().push_back(newMemoryStorage(4, 4));
    m.ref_entries().push_back(newMemoryStorage(5, 5));

    uint64_t last;
    m.last_index(last);
    ASSERT_TRUE(last == 5);

    std::vector<proto::EntryPtr> entries;
    entries.push_back(newMemoryStorage(5, 6));
    m.append(std::move(entries));
    last = 0;
    m.last_index(last);
    ASSERT_TRUE(last == 6);
}

TEST(storage, compact)
{

    {
        uint64_t i = 2;
        Status status = Status::invalid_argument("requested index is unavailable due to compaction");
        uint64_t windex = 3;
        uint64_t wterm = 3;
        uint64_t wlen = 3;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(m.ref_entries()[0]->index == windex);
        ASSERT_TRUE(m.ref_entries()[0]->term == wterm);
        ASSERT_TRUE(m.ref_entries().size() == wlen);
    }

    {
        uint64_t i = 3;
        Status status = Status::invalid_argument("requested index is unavailable due to compaction");
        uint64_t windex = 3;
        uint64_t wterm = 3;
        uint64_t wlen = 3;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(m.ref_entries()[0]->index == windex);
        ASSERT_TRUE(m.ref_entries()[0]->term == wterm);
        ASSERT_TRUE(m.ref_entries().size() == wlen);
    }


    {
        uint64_t i = 4;
        Status status = Status::ok();
        uint64_t windex = 4;
        uint64_t wterm = 4;
        uint64_t wlen = 2;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(m.ref_entries()[0]->index == windex);
        ASSERT_TRUE(m.ref_entries()[0]->term == wterm);
        ASSERT_TRUE(m.ref_entries().size() == wlen);
    }


    {
        uint64_t i = 5;
        Status status = Status::ok();
        uint64_t windex = 5;
        uint64_t wterm = 5;
        uint64_t wlen = 1;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(m.ref_entries()[0]->index == windex);
        ASSERT_TRUE(m.ref_entries()[0]->term == wterm);
        ASSERT_TRUE(m.ref_entries().size() == wlen);
    }

}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
