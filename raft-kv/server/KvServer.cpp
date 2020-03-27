#include <boost/algorithm/string.hpp>
#include <future>
#include <raft-kv/server/KvServer.h>
#include <raft-kv/common/log.h>

namespace kv {

KvServer::KvServer(uint64_t id, const std::string& cluster, uint16_t port)
    : port_(port),
      pthread_id_(0),
      timer_(io_servie_),
      id_(id),
      wal_dir_("raft-kv-" + std::to_string(id)),
      last_index_(0),
      conf_state_(new proto::ConfState()),
      snapshot_index_(0),
      applied_index_(0) {
  boost::split(peers_, cluster, boost::is_any_of(","));
  if (peers_.empty()) {
    LOG_FATAL("invalid args %s", cluster.c_str());
  }

  storage_ = std::make_shared<MemoryStorage>();

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

  std::vector<PeerContext> peers;
  for (size_t i = 0; i < peers_.size(); ++i) {
    peers.push_back(PeerContext{.id = i + 1});
  }
  node_ = std::make_shared<RawNode>(c, std::move(peers));

  bool old_wal = wal::exists(wal_dir_);
}

KvServer::~KvServer() {
  LOG_DEBUG("stopped");
  if (transport_) {
    transport_->stop();
    transport_ = nullptr;
  }
}

void KvServer::start_timer() {
  auto self = shared_from_this();
  timer_.expires_from_now(boost::posix_time::millisec(100));
  timer_.async_wait([self](const boost::system::error_code& err) {
    if (err) {
      LOG_ERROR("timer waiter error %s", err.message().c_str());
      return;
    }

    self->start_timer();
    self->node_->tick();
    self->check_raft_ready();
  });
}

void KvServer::check_raft_ready() {
  assert(pthread_id_ == pthread_self());
  while (node_->has_ready()) {
    auto rd = node_->ready();
    if (!rd->contains_updates()) {
      LOG_WARN("ready not contains updates");
      return;
    }

    if (!rd->snapshot.is_empty()) {
      //LOG_WARN("no impl yet");
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

bool KvServer::publish_entries(const std::vector<proto::EntryPtr>& entries) {
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
          LOG_ERROR("confert error %s", e.what());
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

void KvServer::entries_to_apply(const std::vector<proto::EntryPtr>& entries, std::vector<proto::EntryPtr>& ents) {
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

void KvServer::maybe_trigger_snapshot() {
  //LOG_WARN("not impl yet");
}

void KvServer::schedule() {
  redis_server_ = std::make_shared<RedisServer>(shared_from_this(), port_);
  std::promise<pthread_t> promise;
  std::future<pthread_t> future = promise.get_future();
  redis_server_->start(promise);
  future.wait();
  pthread_t id = future.get();
  LOG_DEBUG("http server start [%lu]", id);

  pthread_id_ = pthread_self();
  LOG_DEBUG("kdv server start [%lu]", pthread_id_);

  start_timer();
  pthread_id_ = pthread_self();
  io_servie_.run();
}

void KvServer::propose(std::shared_ptr<std::vector<uint8_t>> data, const StatusCallback& callback) {
  io_servie_.post([this, data, callback]() {
    Status status = node_->propose(std::move(*data));
    callback(status);
    check_raft_ready();
  });
}

void KvServer::process(proto::MessagePtr msg, const StatusCallback& callback) {
  io_servie_.post([this, msg, callback]() {
    Status status = this->node_->step(msg);
    callback(status);
    check_raft_ready();
  });
}

void KvServer::is_id_removed(uint64_t id, const std::function<void(bool)>& callback) {
  LOG_DEBUG("no impl yet");
  callback(false);
}

void KvServer::report_unreachable(uint64_t id) {
  LOG_DEBUG("no impl yet");
}

void KvServer::report_snapshot(uint64_t id, SnapshotStatus status) {
  LOG_DEBUG("no impl yet");
}

static KvdServerPtr g_node = nullptr;

void on_signal(int) {
  LOG_INFO("catch signal");
  if (g_node) {
    g_node->stop();
  }
}

void KvServer::main(uint64_t id, const std::string& cluster, uint16_t port) {
  ::signal(SIGINT, on_signal);
  ::signal(SIGHUP, on_signal);
  g_node = std::make_shared<KvServer>(id, cluster, port);

  std::shared_ptr<AsioTransport> transport(new AsioTransport(g_node, g_node->id_));
  std::string& host = g_node->peers_[id - 1];
  transport->start(host);
  g_node->transport_ = transport;

  for (uint64_t i = 0; i < g_node->peers_.size(); ++i) {
    uint64_t peer = i + 1;
    if (peer == g_node->id_) {
      continue;
    }
    transport->add_peer(peer, g_node->peers_[i]);
  }
  g_node->schedule();
}

void KvServer::stop() {
  LOG_DEBUG("stopping");
  redis_server_->stop();

  if (transport_) {
    transport_->stop();
    transport_ = nullptr;
  }
  io_servie_.stop();
}

}
