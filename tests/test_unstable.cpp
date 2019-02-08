#include <gtest/gtest.h>
#include <kvd/raft/Unstable.h>

using namespace kvd;

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

        proto::SnapshotPtr snapshot(new proto::Snapshot());
        snapshot->metadata.index = 4;
        snapshot->metadata.term = 1;


        Unstable unstable(5);
        unstable.ref_snapshot() = snapshot;

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

        proto::SnapshotPtr snapshot(new proto::Snapshot());
        snapshot->metadata.index = 4;
        snapshot->metadata.term = 1;


        Unstable unstable(5);
        unstable.ref_snapshot() = snapshot;

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

        proto::SnapshotPtr snapshot(new proto::Snapshot());
        snapshot->metadata.index = 4;
        snapshot->metadata.term = 1;


        Unstable unstable(5);
        unstable.ref_snapshot() = snapshot;

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

        proto::SnapshotPtr snapshot(new proto::Snapshot());
        snapshot->metadata.index = 4;
        snapshot->metadata.term = 1;


        Unstable unstable(5);
        unstable.ref_snapshot() = snapshot;

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

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

