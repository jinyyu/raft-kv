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

TEST(storage, term)
{
    {
        uint64_t i = 2;
        Status status = Status::invalid_argument("requested index is unavailable due to compaction");
        uint64_t wterm = 0;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        uint64_t term = 0;
        Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }

    {
        uint64_t i = 3;
        Status status = Status::ok();
        uint64_t wterm = 3;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        uint64_t term;
        Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }

    {
        uint64_t i = 4;
        Status status;
        uint64_t wterm = 4;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        uint64_t term;
        Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }


    {
        uint64_t i = 5;
        Status status;
        uint64_t wterm = 5;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        uint64_t term;
        Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }


    {
        uint64_t i = 6;
        Status status = Status::invalid_argument("requested entry at index is unavailable");
        uint64_t wterm = 0;

        MemoryStorage m;
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        uint64_t term = 0;
        Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }
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

TEST(storage, append)
{
    MemoryStorage m;

    {
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(1, 1));
        add_entries.push_back(newMemoryStorage(2, 2));

        std::vector<proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.ref_entries(), out_entries));
    }

    {
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(3, 3));
        add_entries.push_back(newMemoryStorage(4, 4));
        add_entries.push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.ref_entries(), out_entries));
    }

    {
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(3, 3));
        add_entries.push_back(newMemoryStorage(6, 4));
        add_entries.push_back(newMemoryStorage(6, 5));

        std::vector<proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(6, 4));
        out_entries.push_back(newMemoryStorage(6, 5));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.ref_entries(), out_entries));
    }
    {
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(3, 3));
        add_entries.push_back(newMemoryStorage(4, 4));
        add_entries.push_back(newMemoryStorage(5, 5));
        add_entries.push_back(newMemoryStorage(5, 6));

        std::vector<proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));
        out_entries.push_back(newMemoryStorage(5, 6));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.ref_entries(), out_entries));
    }

    // truncate incoming entries, truncate the existing entries and append
    {
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(3, 2));
        add_entries.push_back(newMemoryStorage(3, 3));
        add_entries.push_back(newMemoryStorage(5, 4));

        std::vector<proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(5, 4));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.ref_entries(), out_entries));
    }

    // truncate the existing entries and append
    {
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(5, 4));

        std::vector<proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(5, 4));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.ref_entries(), out_entries));
    }

    // direct append
    {
        m.ref_entries().clear();
        m.ref_entries().push_back(newMemoryStorage(3, 3));
        m.ref_entries().push_back(newMemoryStorage(4, 4));
        m.ref_entries().push_back(newMemoryStorage(5, 5));

        std::vector<proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(5, 6));

        std::vector<proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));
        out_entries.push_back(newMemoryStorage(5, 6));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.ref_entries(), out_entries));
    }
}

TEST(storage, apply)
{
    proto::ConfState cs;
    cs.nodes.push_back(1);
    cs.nodes.push_back(2);
    cs.nodes.push_back(3);

    std::vector<uint8_t> data;
    data.push_back('d');
    data.push_back('a');
    data.push_back('t');
    data.push_back('a');

    MemoryStorage m;

    proto::SnapshotPtr snapshot(new proto::Snapshot());
    snapshot->metadata.index = 4;
    snapshot->metadata.term = 4;
    snapshot->metadata.conf_state = cs;
    auto status = m.apply_snapshot(std::move(snapshot));
    ASSERT_TRUE(status.is_ok());

    snapshot = std::make_shared<proto::Snapshot>();
    snapshot->metadata.index = 3;
    snapshot->metadata.term = 3;
    snapshot->metadata.conf_state = cs;
    status = m.apply_snapshot(std::move(snapshot));
    ASSERT_FALSE(status.is_ok());
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
