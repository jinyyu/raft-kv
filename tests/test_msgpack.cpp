#include <msgpack.hpp>
#include <gtest/gtest.h>
#include <raft-kv/raft/proto.h>

class MyClass {
 public:
  std::string str;
  std::vector<int> vec;
 public:
  MSGPACK_DEFINE (str, vec);
};

TEST(test_msgpack, test_msgpack) {
  std::vector<MyClass> vec;

  MyClass my;
  my.str = "abc";
  my.vec.push_back(1);
  my.vec.push_back(3);

  vec.push_back(std::move(my));

  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, vec);

  msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());

  msgpack::object obj = oh.get();
  std::vector<MyClass> rvec;
  obj.convert(rvec);

  ASSERT_TRUE(rvec.size() == 1);
  MyClass& out = rvec[0];
  ASSERT_TRUE(out.str == "abc");
  ASSERT_TRUE(out.vec.size() == 2);
  ASSERT_TRUE(out.vec[0] == 1);
  ASSERT_TRUE(out.vec[1] == 3);
}

TEST(test_msgpack, test_error) {
  std::vector<MyClass> vec;

  MyClass my;
  my.str = "abc";
  my.vec.push_back(1);
  my.vec.push_back(3);

  vec.push_back(std::move(my));

  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, vec);

  msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());

  msgpack::object obj = oh.get();
  std::string out;
  ASSERT_ANY_THROW(obj.convert(out));

}

class B {
 public:
  int a;
 public:
  MSGPACK_DEFINE (a);
};

TEST(msgpack, entry_size) {
  using namespace kv::proto;

  Entry entry;
  entry.type = 10;
  entry.term = 10;
  entry.index = 10;
  {
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.type = 255;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.term = std::numeric_limits<uint8_t>::max() - 10;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.term = std::numeric_limits<uint8_t>::max() + 10;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.term = std::numeric_limits<uint16_t>::max() + 10;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.term = std::numeric_limits<uint32_t>::max() + 10;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.data.resize(std::numeric_limits<uint8_t>::max() - 1);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.data.resize(std::numeric_limits<uint16_t>::max() - 1);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_TRUE(entry.serialize_size() == sbuf.size());
  }

  {
    entry.data.resize(std::numeric_limits<uint16_t>::max() + 1);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    ASSERT_EQ(entry.serialize_size(), sbuf.size());
  }
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}