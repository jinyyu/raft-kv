#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <future>
#include <raft-kv/server/raft_node.h>
#include <raft-kv/common/log.h>

namespace kv {

static uint64_t defaultSnapCount = 10;
static uint64_t snapshotCatchUpEntriesN = 20;

RaftNode::RaftNode(uint64_t id, const std::string& cluster, uint16_t port)
    : port_(port),
      pthread_id_(0),
      timer_(io_service_),
      id_(id),
      last_index_(0),
      conf_state_(new proto::ConfState()),
      snapshot_index_(0),
      applied_index_(0),
      storage_(new MemoryStorage()),
      snap_count_(defaultSnapCount) {
  boost::split(peers_, cluster, boost::is_any_of(","));
  if (peers_.empty()) {
    LOG_FATAL("invalid args %s", cluster.c_str());
  }

  std::string work_dir = "node_" + std::to_string(id);
  snap_dir_ = work_dir + "/snap";
  wal_dir_ = work_dir + "/wal";

  if (!boost::filesystem::exists(snap_dir_)) {
    boost::filesystem::create_directories(snap_dir_);
  }

  snapshotter_.reset(new Snapshotter(snap_dir_));

  bool wal_exists = boost::filesystem::exists(wal_dir_);

  replay_WAL();

  Config c;
  c.id = id;
  c.election_tick = 10;
  c.heartbeat_tick = 1;
  c.storage = storage_;
  c.applied = 0;
  c.max_size_per_msg = 1024 * 1024;
  c.max_committed_size_per_ready = 0;
  c.max_uncommitted_entries_size = 1 << 30;
  c.max_inflight_msgs = 256;
  c.check_quorum = true;
  c.pre_vote = true;
  c.read_only_option = ReadOnlySafe;
  c.disable_proposal_forwarding = false;

  Status status = c.validate();

  if (!status.is_ok()) {
    LOG_FATAL("invalid configure %s", status.to_string().c_str());
  }

  if (wal_exists) {
    node_.reset(Node::restart_node(c));
  } else {
    std::vector<PeerContext> peers;
    for (size_t i = 0; i < peers_.size(); ++i) {
      peers.push_back(PeerContext{.id = i + 1});
    }
    node_.reset(Node::start_node(c, peers));
  }
}

RaftNode::~RaftNode() {
  LOG_DEBUG("stopped");
  if (transport_) {
    transport_->stop();
    transport_ = nullptr;
  }
}

void RaftNode::start_timer() {
  timer_.expires_from_now(boost::posix_time::millisec(100));
  timer_.async_wait([this](const boost::system::error_code& err) {
    if (err) {
      LOG_ERROR("timer waiter error %s", err.message().c_str());
      return;
    }

    this->start_timer();
    this->node_->tick();
    this->pull_ready_events();
  });
}

void RaftNode::pull_ready_events() {
  assert(pthread_id_ == pthread_self());
  while (node_->has_ready()) {
    auto rd = node_->ready();
    if (!rd->contains_updates()) {
      LOG_WARN("ready not contains updates");
      return;
    }

    wal_->save(rd->hard_state, rd->entries);

    if (!rd->snapshot.is_empty()) {
      Status status = save_snap(rd->snapshot);
      if (!status.is_ok()) {
        LOG_FATAL("save snapshot error %s", status.to_string().c_str());
      }
      storage_->apply_snapshot(rd->snapshot);
      publish_snapshot(rd->snapshot);
    }

    if (!rd->entries.empty()) {
      storage_->append(rd->entries);
    }
    if (!rd->messages.empty()) {
      transport_->send(rd->messages);
    }

    if (!rd->committed_entries.empty()) {
      std::vector<proto::EntryPtr> ents;
      entries_to_apply(rd->committed_entries, ents);
      if (!ents.empty()) {
        publish_entries(ents);
      }
    }
    maybe_trigger_snapshot();
    node_->advance(rd);
  }
}

Status RaftNode::save_snap(const proto::Snapshot& snap) {
  // must save the snapshot index to the WAL before saving the
  // snapshot to maintain the invariant that we only Open the
  // wal at previously-saved snapshot indexes.
  Status status;

  WAL_Snapshot wal_snapshot;
  wal_snapshot.index = snap.metadata.index;
  wal_snapshot.term = snap.metadata.term;

  status = wal_->save_snapshot(wal_snapshot);
  if (!status.is_ok()) {
    return status;
  }

  status = snapshotter_->save_snap(snap);
  if (!status.is_ok()) {
    LOG_FATAL("save snapshot error %s", status.to_string().c_str());
  }
  return status;

  /*/
  return rc.wal.ReleaseLockTo(snap.Metadata.Index)
   */
}

void RaftNode::publish_snapshot(const proto::Snapshot& snap) {
  if (snap.is_empty()) {
    return;
  }

  LOG_DEBUG("publishing snapshot at index %lu", snapshot_index_);

  if (snap.metadata.index <= applied_index_) {
    LOG_FATAL("snapshot index [%lu] should > progress.appliedIndex [%lu] + 1", snap.metadata.index, applied_index_);
  }

  //trigger to load snapshot
  proto::SnapshotPtr snapshot(new proto::Snapshot());
  snapshot->metadata = snap.metadata;
  SnapshotDataPtr data(new std::vector<uint8_t>(snap.data));

  redis_server_->recover_from_snapshot(data, [snapshot, this](const Status& status) {
    //由redis线程回调
    if (!status.is_ok()) {
      LOG_FATAL("recover from snapshot error %s", status.to_string().c_str());
    }

    io_service_.post([this, snapshot] {
      // 转到io线程执行

      *(this->conf_state_) = snapshot->metadata.conf_state;
      snapshot_index_ = snapshot->metadata.index;
      applied_index_ = snapshot->metadata.index;
      LOG_DEBUG("finished publishing snapshot at index %lu", snapshot_index_);

    });

  });

}

void RaftNode::open_WAL(const proto::Snapshot& snap) {
  if (!boost::filesystem::exists(wal_dir_)) {
    boost::filesystem::create_directories(wal_dir_);
    wal_ = WAL::create(wal_dir_);
    wal_ = nullptr;
  }

  WAL_Snapshot walsnap;
  walsnap.index = snap.metadata.index;
  walsnap.term = snap.metadata.term;
  LOG_INFO("loading WAL at term %lu and index %lu", walsnap.term, walsnap.index);

  wal_ = WAL::open(wal_dir_, walsnap);
}

void RaftNode::replay_WAL() {
  LOG_DEBUG("replaying WAL of member %lu", id_);

  proto::Snapshot snapshot;
  Status status = snapshotter_->load(snapshot);
  if (!status.is_ok()) {
    if (status.is_not_found()) {
      LOG_INFO("snapshot not found for node %lu", id_);
    } else {
      LOG_FATAL("error loading snapshot %s", status.to_string().c_str());
    }
  } else {
    storage_->apply_snapshot(snapshot);
  }

  open_WAL(snapshot);
  assert(wal_ != nullptr);

  proto::HardState hs;
  std::vector<proto::EntryPtr> ents;
  status = wal_->read_all(hs, ents);
  if (!status.is_ok()) {
    LOG_FATAL("failed to read WAL %s", status.to_string().c_str());
  }

  storage_->set_hard_state(hs);

  // append to storage so raft starts at the right place in log
  storage_->append(ents);

  // send nil once lastIndex is published so client knows commit channel is current
  if (!ents.empty()) {
    last_index_ = ents.back()->index;
  } else {
    snap_data_ = std::move(snapshot.data);
  }
}

bool RaftNode::publish_entries(const std::vector<proto::EntryPtr>& entries) {
  for (const proto::EntryPtr& entry : entries) {
    switch (entry->type) {
      case proto::EntryNormal: {
        if (entry->data.empty()) {
          // ignore empty messages
          break;
        }
        redis_server_->read_commit(entry);
        break;
      }

      case proto::EntryConfChange: {
        proto::ConfChange cc;
        try {
          msgpack::object_handle oh = msgpack::unpack((const char*) entry->data.data(), entry->data.size());
          oh.get().convert(cc);
        }
        catch (std::exception& e) {
          LOG_ERROR("invalid EntryConfChange msg %s", e.what());
          continue;
        }
        conf_state_ = node_->apply_conf_change(cc);

        switch (cc.conf_change_type) {
          case proto::ConfChangeAddNode:
            if (!cc.context.empty()) {
              std::string str((const char*) cc.context.data(), cc.context.size());
              transport_->add_peer(cc.node_id, str);
            }
            break;
          case proto::ConfChangeRemoveNode:
            if (cc.node_id == id_) {
              LOG_INFO("I've been removed from the cluster! Shutting down.");
              return false;
            }
            transport_->remove_peer(cc.node_id);
          default: {
            LOG_INFO("configure change %d", cc.conf_change_type);
          }
        }
        break;
      }
      default: {
        LOG_FATAL("unknown type %d", entry->type);
        return false;
      }
    }

    // after commit, update appliedIndex
    applied_index_ = entry->index;

    // replay has finished
    if (entry->index == this->last_index_) {
      LOG_DEBUG("replay has finished");
    }
  }
  return true;
}

void RaftNode::entries_to_apply(const std::vector<proto::EntryPtr>& entries, std::vector<proto::EntryPtr>& ents) {
  if (entries.empty()) {
    return;
  }

  uint64_t first = entries[0]->index;
  if (first > applied_index_ + 1) {
    LOG_FATAL("first index of committed entry[%lu] should <= progress.appliedIndex[%lu]+1", first, applied_index_);
  }
  if (applied_index_ - first + 1 < entries.size()) {
    ents.insert(ents.end(), entries.begin() + applied_index_ - first + 1, entries.end());
  }
}

void RaftNode::maybe_trigger_snapshot() {
  if (applied_index_ - snapshot_index_ <= snap_count_) {
    return;
  }

  LOG_DEBUG("start snapshot [applied index: %lu | last snapshot index: %lu]", applied_index_, snapshot_index_);

  auto get_cb = [this](const SnapshotDataPtr& data) {
    //由redis线程回调

    io_service_.post([this, data] {
      // 转到io线程执行

      proto::SnapshotPtr snap;
      Status status = storage_->create_snapshot(applied_index_, conf_state_, *data, snap);
      if (!status.is_ok()) {
        LOG_FATAL("create snapshot error %s", status.to_string().c_str());
      }

      status = save_snap(*snap);
      if (!status.is_ok()) {
        LOG_FATAL("save snapshot error %s", status.to_string().c_str());
      }

      uint64_t compactIndex = 1;
      if (applied_index_ > snapshotCatchUpEntriesN) {
        compactIndex = applied_index_ - snapshotCatchUpEntriesN;
      }
      status = storage_->compact(compactIndex);
      if (!status.is_ok()) {
        LOG_FATAL("compact error %s", status.to_string().c_str());
      }
      LOG_INFO("compacted log at index %lu", compactIndex);
      snapshot_index_ = applied_index_;
    });

  };
  redis_server_->get_snapshot(std::move(get_cb));
}

void RaftNode::schedule() {
  pthread_id_ = pthread_self();

  proto::SnapshotPtr snap;
  Status status = storage_->snapshot(snap);
  if (!status.is_ok()) {
    LOG_FATAL("get snapshot failed %s", status.to_string().c_str());
  }

  *conf_state_ = snap->metadata.conf_state;
  snapshot_index_ = snap->metadata.index;
  applied_index_ = snap->metadata.index;

  redis_server_ = std::make_shared<RedisStore>(this, std::move(snap_data_), port_);
  std::promise<pthread_t> promise;
  std::future<pthread_t> future = promise.get_future();
  redis_server_->start(promise);
  future.wait();
  pthread_t id = future.get();
  LOG_DEBUG("server start [%lu]", id);

  start_timer();
  io_service_.run();
}

void RaftNode::propose(std::shared_ptr<std::vector<uint8_t>> data, const StatusCallback& callback) {
  if (pthread_id_ != pthread_self()) {
    io_service_.post([this, data, callback]() {
      Status status = node_->propose(std::move(*data));
      callback(status);
      pull_ready_events();
    });
  } else {
    Status status = node_->propose(std::move(*data));
    callback(status);
    pull_ready_events();
  }
}

void RaftNode::process(proto::MessagePtr msg, const StatusCallback& callback) {
  if (pthread_id_ != pthread_self()) {
    io_service_.post([this, msg, callback]() {
      Status status = this->node_->step(msg);
      callback(status);
      pull_ready_events();
    });
  } else {
    Status status = this->node_->step(msg);
    callback(status);
    pull_ready_events();
  }
}

void RaftNode::is_id_removed(uint64_t id, const std::function<void(bool)>& callback) {
  LOG_DEBUG("no impl yet");
  callback(false);
}

void RaftNode::report_unreachable(uint64_t id) {
  LOG_DEBUG("no impl yet");
}

void RaftNode::report_snapshot(uint64_t id, SnapshotStatus status) {
  LOG_DEBUG("no impl yet");
}

static RaftNodePtr g_node = nullptr;

void on_signal(int) {
  LOG_INFO("catch signal");
  if (g_node) {
    g_node->stop();
  }
}

void RaftNode::main(uint64_t id, const std::string& cluster, uint16_t port) {
  ::signal(SIGINT, on_signal);
  ::signal(SIGHUP, on_signal);
  g_node = std::make_shared<RaftNode>(id, cluster, port);

  g_node->transport_ = Transport::create(g_node.get(), g_node->id_);
  std::string& host = g_node->peers_[id - 1];
  g_node->transport_->start(host);

  for (uint64_t i = 0; i < g_node->peers_.size(); ++i) {
    uint64_t peer = i + 1;
    if (peer == g_node->id_) {
      continue;
    }
    g_node->transport_->add_peer(peer, g_node->peers_[i]);
  }

  g_node->schedule();
}

void RaftNode::stop() {
  LOG_DEBUG("stopping");
  redis_server_->stop();

  if (transport_) {
    transport_->stop();
    transport_ = nullptr;
  }
  io_service_.stop();
}

}
