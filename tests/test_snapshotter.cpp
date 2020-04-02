#include <gtest/gtest.h>
#include <time.h>
#include <unistd.h>
#include <raft-kv/raft/proto.h>
#include <boost/filesystem.hpp>
#include <raft-kv/snap/snapshotter.h>

using namespace kv;

static std::string get_tmp_snapshot_dir() {
  char buffer[128];
  snprintf(buffer, sizeof(buffer), "_test_snapshot/%d_%d", (int) time(NULL), getpid());
  return buffer;
}

proto::Snapshot& get_test_snap() {
  static proto::SnapshotPtr snap;
  if (!snap) {
    snap = std::make_shared<proto::Snapshot>();
    snap->metadata.index = 1;
    snap->metadata.term = 1;
    snap->data.push_back('t');
    snap->data.push_back('e');
    snap->data.push_back('s');
    snap->data.push_back('t');

    snap->metadata.conf_state.nodes.push_back(1);
    snap->metadata.conf_state.nodes.push_back(2);
    snap->metadata.conf_state.nodes.push_back(3);
  }
  return *snap;
}

TEST(snap, SaveAndLoad) {
  std::string dir = get_tmp_snapshot_dir();
  boost::filesystem::create_directories(dir);
  Snapshotter snap(dir);
  proto::Snapshot& s = get_test_snap();
  Status status = snap.save_snap(s);
  ASSERT_TRUE(status.is_ok());

  proto::Snapshot snapshot;
  status = snap.load(snapshot);

  ASSERT_TRUE(status.is_ok());
  ASSERT_TRUE(s.equal(snapshot));
}

TEST(snap, Failback) {
  std::string dir = get_tmp_snapshot_dir();
  boost::filesystem::create_directories(dir);

  char tmp[256];
  snprintf(tmp, sizeof(tmp), "%s/0x%016d-%016d.snap", dir.c_str(), 0xFFFF, 0xFFFF);
  FILE* fp = fopen(tmp, "w");
  fwrite("somedata", 1, 5, fp);
  fclose(fp);

  Snapshotter snap(dir);
  proto::Snapshot& s = get_test_snap();
  Status status = snap.save_snap(s);
  ASSERT_TRUE(status.is_ok());

  proto::Snapshot snapshot;
  status = snap.load(snapshot);

  ASSERT_TRUE(status.is_ok());
  ASSERT_TRUE(s.equal(snapshot));
  std::string broken = std::string(tmp) + ".broken";
  ASSERT_TRUE(boost::filesystem::exists(broken));
}

int main(int argc, char* argv[]) {
  boost::system::error_code code;
  boost::filesystem::create_directories("_test_snapshot");

  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();

  boost::filesystem::remove_all("_test_snapshot", code);
  return ret;
}

