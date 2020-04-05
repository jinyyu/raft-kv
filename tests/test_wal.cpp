#include <gtest/gtest.h>
#include <raft-kv/wal/wal.h>

using namespace kv;

TEST(wal, wal_len) {
  WAL_Record record;

  for (int i = 0; i < MAX_WAL_RECORD_LEN; ++i) {
    set_WAL_Record_len(record, i);
    ASSERT_EQ(WAL_Record_len(record), i);
  }

  ASSERT_EQ(MAX_WAL_RECORD_LEN, 0xff << 16 | 0xff << 8 | 0xff);
  fprintf(stderr, "max wal record len %d\n", MAX_WAL_RECORD_LEN);

  uint32_t len = MAX_WAL_RECORD_LEN + 1;
  set_WAL_Record_len(record, len);
  ASSERT_EQ(WAL_Record_len(record), MAX_WAL_RECORD_LEN);
}

TEST(wal, ScanWalName) {
  struct Test {
    std::string str;
    uint64_t wseq;
    uint64_t windex;
    bool wok;
  };

  std::vector<Test> test;

  test.push_back(Test{.str = "0000000000000000-0000000000000000.wal", .wseq = 0, .windex = 0, .wok = true});
  test.push_back(Test{.str = "0000000000000100-0000000000000101.wal", .wseq = 0x100, .windex = 0x101, .wok = true});
  test.push_back(Test{.str = "0000000000000000.wa", .wseq = 0, .windex = 0, .wok = false});
  test.push_back(Test{.str = "0000000000000000-0000000000000000.snap", .wseq = 0, .windex = 0, .wok = false});
  for (auto& test : test) {
    uint64_t seq;
    uint64_t index;
    bool ok = WAL::parse_wal_name(test.str, &seq, &index);

    ASSERT_EQ(ok, test.wok);
    ASSERT_EQ(seq, test.wseq);
    ASSERT_EQ(index, test.windex);
  }
}

TEST(wal, SearchIndex) {
  struct Test {
    std::vector<std::string> names;
    uint64_t index;
    int widx;
    bool wok;
  };

  std::vector<Test> test;
  {
    Test t;
    t.names = {"0000000000000000-0000000000000000.wal", "0000000000000001-0000000000001000.wal", "0000000000000002-0000000000002000.wal"};
    t.index = 0x1000;
    t.widx = 1;
    t.wok = true;
    //test.push_back(t);
  }
  {
    Test t;
    t.names = {"0000000000000001-0000000000004000.wal", "0000000000000002-0000000000003000.wal", "0000000000000003-0000000000005000.wal"};
    t.index = 0x4000;
    t.widx = 1;
    t.wok = true;
    //test.push_back(t);
  }
  {
    Test t;
    t.names = {"0000000000000001-0000000000002000.wal", "0000000000000002-0000000000003000.wal", "0000000000000003-0000000000005000.wal"};
    t.index = 0x1000;
    t.widx = -1;
    t.wok = false;
    test.push_back(t);
  }

  for (auto& test : test) {
    uint64_t i;
    bool ok = WAL::search_index(test.names, test.index, &i);
    ASSERT_EQ(ok, test.wok);
    ASSERT_EQ(i, test.widx);


  }
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
