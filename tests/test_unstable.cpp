#include <gtest/gtest.h>
#include <kvd/raft/Unstable.h>

using namespace kvd;


static proto::SnapshotPtr newSnapshot(uint64_t index, uint64_t term)
{
    proto::SnapshotPtr s(new proto::Snapshot());
    s->metadata.index = index;
    s->metadata.term = term;
    return s;
}


TEST(unstable, first_index)
{
    {
        uint64_t windex = 0;

        proto::EntryPtr entry(new proto::Entry());
        entry->term = 1;
        entry->index = 5;


        Unstable unstable(5);
        unstable.ref_snapshot() = nullptr;
        unstable.ref_entries().push_back(entry);

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_FALSE(ok);
        ASSERT_TRUE(index == windex);
    }

    {
        uint64_t windex = 0;

        Unstable unstable(5);
        unstable.ref_snapshot() = nullptr;
        unstable.ref_entries().clear();

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_FALSE(ok);
        ASSERT_TRUE(index == windex);
    }

    {
        uint64_t windex = 5;

        Unstable unstable(5);
        unstable.ref_snapshot() = newSnapshot(4, 1);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(index == windex);
    }

    {
        uint64_t windex = 5;


        Unstable unstable(5);
        unstable.ref_snapshot() =  newSnapshot(4, 1);;

        unstable.ref_entries().clear();

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(index == windex);
    }
}

TEST(unstable, last_index)
{
    {
        uint64_t windex = 5;

        Unstable unstable(5);
        unstable.ref_snapshot() =  newSnapshot(4, 1);;

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);

        uint64_t index;
        bool ok;
        unstable.maybe_last_index(index, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(index == windex);
    }

    {
        uint64_t windex = 5;

        Unstable unstable(5);
        unstable.ref_snapshot() = nullptr;

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);

        uint64_t index;
        bool ok;
        unstable.maybe_last_index(index, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(index == windex);
    }

    {
        uint64_t windex = 4;

        Unstable unstable(5);
        unstable.ref_snapshot() =  newSnapshot(4, 1);

        unstable.ref_entries().clear();

        uint64_t index;
        bool ok;
        unstable.maybe_last_index(index, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(index == windex);
    }

    {
        uint64_t windex = 0;

        Unstable unstable(5);

        uint64_t index;
        bool ok;
        unstable.maybe_last_index(index, ok);
        ASSERT_TRUE(!ok);
        ASSERT_TRUE(index == windex);
    }
}



TEST(unstalbe, term)
{
    {
        uint64_t wterm = 1;

        Unstable unstable(5);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);


        uint64_t term;
        bool ok;
        unstable.maybe_term(5, term, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(term == wterm);
    }

    {
        uint64_t wterm = 0;

        Unstable unstable(5);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);


        uint64_t term;
        bool ok;
        unstable.maybe_term(6, term, ok);
        ASSERT_TRUE(!ok);
        ASSERT_TRUE(term == wterm);
    }

    {
        uint64_t wterm = 0;

        Unstable unstable(5);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);


        uint64_t term;
        bool ok;
        unstable.maybe_term(4, term, ok);
        ASSERT_TRUE(!ok);
        ASSERT_TRUE(term == wterm);
    }

    {
        uint64_t wterm = 1;

        Unstable unstable(5);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);

        unstable.ref_snapshot() = newSnapshot(4, 1);

        uint64_t term;
        bool ok;
        unstable.maybe_term(5, term, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(term == wterm);
    }

    {
        Unstable unstable(5);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);

        unstable.ref_snapshot() = newSnapshot(4, 1);

        uint64_t term;
        bool ok;
        unstable.maybe_term(6, term, ok);
        ASSERT_TRUE(!ok);
        ASSERT_TRUE(term == 0);
    }

    {
        Unstable unstable(5);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);

        unstable.ref_snapshot() = newSnapshot(4, 1);

        uint64_t term;
        bool ok;
        unstable.maybe_term(4, term, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(term == 1);
    }

    {
        Unstable unstable(5);

        proto::EntryPtr entry(new proto::Entry());
        entry->index = 5;
        entry->term = 1;
        unstable.ref_entries().push_back(entry);

        unstable.ref_snapshot() = newSnapshot(4, 1);

        uint64_t term;
        bool ok;
        unstable.maybe_term(3, term, ok);
        ASSERT_TRUE(!ok);
        ASSERT_TRUE(term == 0);
    }

    {
        Unstable unstable(5);

        unstable.ref_entries().clear();

        unstable.ref_snapshot() = newSnapshot(4, 1);

        uint64_t term;
        bool ok;
        unstable.maybe_term(5, term, ok);
        ASSERT_TRUE(!ok);
        ASSERT_TRUE(term == 0);
    }

    {
        Unstable unstable(5);

        unstable.ref_entries().clear();

        unstable.ref_snapshot() = newSnapshot(4, 1);

        uint64_t term;
        bool ok;
        unstable.maybe_term(4, term, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(term == 1);
    }

    {
        Unstable unstable(5);

        unstable.ref_entries().clear();

        uint64_t term;
        bool ok;
        unstable.maybe_term(5, term, ok);
        ASSERT_TRUE(!ok);
        ASSERT_TRUE(term == 0);
    }
}


int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

