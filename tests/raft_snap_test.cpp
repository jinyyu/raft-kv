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
  msg->index = sm->prs_[2]->next - 1;
  msg->reject = true;

  sm->step(msg);

  ASSERT_EQ(sm->prs_[2]->pending_snapshot, 11);
}

TEST(snap, PendingSnapshotPauseReplication) {
  auto storage = std::make_shared<MemoryStorage>();
  auto sm = newTestRaft(1, {1, 2}, 10, 1, storage);
  sm->restore(testingSnap());

  sm->become_candidate();
  sm->become_leader();

  sm->prs_[2]->become_snapshot(11);

  proto::MessagePtr msg(new proto::Message());
  msg->from = 1;
  msg->to = 1;
  msg->type = proto::MsgProp;
  proto::Entry entry;
  entry.data = str_to_vector("somedata");
  msg->entries.push_back(entry);

  sm->step(msg);

  auto msgs = sm->read_messages();
  ASSERT_TRUE(msgs.empty());
}

TEST(snap, SnapshotFailure) {
  auto storage = std::make_shared<MemoryStorage>();
  auto sm = newTestRaft(1, {1, 2}, 10, 1, storage);
  sm->restore(testingSnap());

  sm->become_candidate();
  sm->become_leader();

  sm->prs_[2]->next = 1;
  sm->prs_[2]->become_snapshot(11);

  proto::MessagePtr msg(new proto::Message());
  msg->from = 2;
  msg->to = 1;
  msg->type = proto::MsgSnapStatus;
  msg->reject = true;

  sm->step(msg);

  ASSERT_EQ(sm->prs_[2]->pending_snapshot, 0);
  ASSERT_EQ(sm->prs_[2]->next, 1);
  ASSERT_TRUE(sm->prs_[2]->paused);
}

TEST(snap, SnapshotSucceed) {
  auto storage = std::make_shared<MemoryStorage>();
  auto sm = newTestRaft(1, {1, 2}, 10, 1, storage);
  sm->restore(testingSnap());

  sm->become_candidate();
  sm->become_leader();

  sm->prs_[2]->next = 1;
  sm->prs_[2]->become_snapshot(11);

  proto::MessagePtr msg(new proto::Message());
  msg->from = 2;
  msg->to = 1;
  msg->type = proto::MsgSnapStatus;
  msg->reject = false;

  sm->step(msg);

  ASSERT_EQ(sm->prs_[2]->pending_snapshot, 0);
  ASSERT_EQ(sm->prs_[2]->next, 12);
  ASSERT_TRUE(sm->prs_[2]->paused);
}

TEST(snap, SnapshotAbort) {
  auto storage = std::make_shared<MemoryStorage>();
  auto sm = newTestRaft(1, {1, 2}, 10, 1, storage);
  sm->restore(testingSnap());

  sm->become_candidate();
  sm->become_leader();

  sm->prs_[2]->next = 1;
  sm->prs_[2]->become_snapshot(11);

  // A successful msgAppResp that has a higher/equal index than the
  // pending snapshot should abort the pending snapshot.
  proto::MessagePtr msg(new proto::Message());
  msg->from = 2;
  msg->to = 1;
  msg->type = proto::MsgAppResp;
  msg->index = 11;

  sm->step(msg);

  ASSERT_EQ(sm->prs_[2]->pending_snapshot, 0);
  ASSERT_EQ(sm->prs_[2]->next, 12);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
