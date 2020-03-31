#include <gtest/gtest.h>
#include <raft-kv/raft/raft.h>
#include <raft-kv/common/log.h>
#include <raft-kv/raft/util.h>
#include "network.hpp"

using namespace kv;

static proto::Snapshot& testingSnap() {
  static proto::SnapshotPtr snapshot;
  if (!snapshot) {
    snapshot = std::make_shared<proto::Snapshot>();
    snapshot->metadata.index = 11;
    snapshot->metadata.term = 11;
    snapshot->metadata.conf_state.nodes.push_back(1);
    snapshot->metadata.conf_state.nodes.push_back(2);

  }
  return *snapshot;
}

TEST(snap, SendingSnapshotSetPendingSnapshot) {
  auto storage = std::make_shared<MemoryStorage>();
  auto sm = newTestRaft(1, {1}, 10, 1, storage);
  sm->restore(testingSnap());

  sm->become_candidate();
  sm->become_leader();

  // force set the next of node 1, so that
  // node 1 needs a snapshot
  sm->prs_[2]->next = sm->raft_log_->first_index();

  proto::MessagePtr msg(new proto::Message());
  msg->from = 2;
  msg->to = 1;
  msg->type = proto::MsgAppResp;
  msg->index = sm->prs_[2]->next -1;
  msg->reject = true;

  sm->step(msg);

  ASSERT_EQ(sm->prs_[2]->pending_snapshot, 11);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
