#include <gtest/gtest.h>
#include <raft-kv/raft/raft.h>
#include <raft-kv/common/log.h>
#include <raft-kv/raft/util.h>
#include "network.hpp"

using namespace kv;

TEST(raft, SingleNodeCandidate) {
  std::vector<RaftPtr> peers{nullptr};
  Network tt(peers);
  {
    proto::MessagePtr msg(new proto::Message());
    msg->from = 1;
    msg->to = 1;
    msg->type = proto::MsgHup;
    tt.send(msg);
  }
  ASSERT_TRUE(tt.peers[1]->state_ == RaftState::Leader);
}

TEST(raft, SingleNodePreCandidate) {
  std::vector<RaftPtr> peers{nullptr};
  Network tt(preVoteConfig, peers);
  {
    proto::MessagePtr msg(new proto::Message());
    msg->from = 1;
    msg->to = 1;
    msg->type = proto::MsgHup;
    tt.send(msg);
  }
  ASSERT_TRUE(tt.peers[1]->state_ == RaftState::Leader);
}

TEST(raft, OldMessages) {
  std::vector<RaftPtr> peers{nullptr, nullptr, nullptr};
  Network tt(peers);

  // make 0 leader @ term 3
  {
    proto::MessagePtr msg(new proto::Message());
    msg->from = 1;
    msg->to = 1;
    msg->type = proto::MsgHup;
    tt.send(msg);
  }

  {
    proto::MessagePtr msg(new proto::Message());
    msg->from = 2;
    msg->to = 2;
    msg->type = proto::MsgHup;
    tt.send(msg);
  }
  {
    proto::MessagePtr msg(new proto::Message());
    msg->from = 1;
    msg->to = 1;
    msg->type = proto::MsgHup;
    tt.send(msg);
  }

  // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
  {
    proto::MessagePtr msg(new proto::Message());
    msg->from = 2;
    msg->to = 1;
    msg->type = proto::MsgApp;
    msg->term = 2;
    proto::Entry e;
    e.index = 3;
    e.term = 2;
    msg->entries.push_back(e);
    tt.send(msg);
  }

  // commit a new entry
  {
    proto::MessagePtr msg(new proto::Message());
    msg->from = 1;
    msg->to = 1;
    msg->type = proto::MsgProp;
    proto::Entry e;
    e.data = str_to_vector("somedata");
    msg->entries.push_back(e);
    tt.send(msg);
  }

  MemoryStoragePtr ms(new MemoryStorage());
  {
    ms->entries_.clear();
    proto::EntryPtr e1(new proto::Entry());
    ms->entries_.push_back(e1);

    proto::EntryPtr e2(new proto::Entry());
    e2->index = 1;
    e2->term = 1;
    ms->entries_.push_back(e2);

    proto::EntryPtr e3(new proto::Entry());
    e3->index = 2;
    e3->term = 2;
    ms->entries_.push_back(e3);

    proto::EntryPtr e4(new proto::Entry());
    e4->index = 3;
    e4->term = 3;
    ms->entries_.push_back(e4);

    proto::EntryPtr e5(new proto::Entry());
    e5->index = 4;
    e5->term = 3;
    e5->data = str_to_vector("somedata");;
    ms->entries_.push_back(e5);
  }

  RaftLogPtr wlog(new RaftLog(ms, RaftLog::unlimited()));
  wlog->committed_ = 4;
  wlog->unstable_->offset_ = 5;

  for (auto it = tt.peers.begin(); it != tt.peers.end(); ++it) {
    auto sm = it->second;

    ASSERT_TRUE(sm->raft_log_->committed_ == wlog->committed_);
    ASSERT_TRUE(sm->raft_log_->applied_ == wlog->applied_);

    std::vector<proto::EntryPtr> tt_entries;
    wlog->all_entries(tt_entries);

    std::vector<proto::EntryPtr> sm_entries;
    sm->raft_log_->all_entries(sm_entries);

    ASSERT_TRUE(entry_cmp(tt_entries, sm_entries));
  }
}

int main(int argc, char* argv[]) {
  //testing::GTEST_FLAG(filter) = "raft.OldMessages";
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
