#include "kvd/KvdServer.h"
#include "kvd/transport/AsioTransport.h"
#include "kvd/common/log.h"
#include <boost/algorithm/string.hpp>
#include <future>

namespace kvd
{

KvdServer::KvdServer(uint64_t id, const std::string& cluster, uint16_t port)
    : port_(port),
      timer_(io_service_),
      id_(id)
{
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
    node_ = std::make_shared<RawNode>(c, std::move(peers), io_service_);
}

KvdServer::~KvdServer()
{
    LOG_DEBUG("stopped");
    if (transport_) {
        transport_->stop();
        transport_ = nullptr;
    }
}

void KvdServer::start_timer()
{
    auto self = shared_from_this();
    timer_.expires_from_now(boost::posix_time::microsec(100));
    timer_.async_wait([self](const boost::system::error_code& err) {
        if (err) {
            LOG_ERROR("timer waiter error %s", err.message().c_str());
            return;
        }
        //self->node_->tick();
        self->start_timer();
    });
}

void KvdServer::schedule()
{
    start_timer();
    http_server_ = std::make_shared<HTTPServer>(shared_from_this(), io_service_, port_);
    http_server_->start();
    io_service_.run();
}

Status KvdServer::process(proto::MessagePtr msg)
{
    std::shared_ptr<std::promise<Status>> promise(new std::promise<Status>());
    std::future<Status> future = promise->get_future();
    io_service_.post([this, promise, msg]() {
        Status status = this->node_->step(msg);
        promise->set_value(status);
    });
    future.wait();
    return future.get();
}

bool KvdServer::is_id_removed(uint64_t id)
{
    LOG_DEBUG("no impl yet");
    return false;
}

void KvdServer::report_unreachable(uint64_t id)
{
    LOG_DEBUG("no impl yet");
}

void KvdServer::report_snapshot(uint64_t id, SnapshotStatus status)
{
    LOG_DEBUG("no impl yet");
}

static KvdServerPtr g_node = nullptr;

void on_signal(int)
{
    LOG_INFO("catch signal");
    if (g_node) {
        g_node->stop();
    }
}

void KvdServer::main(uint64_t id, const std::string& cluster, uint16_t port)
{
    ::signal(SIGINT, on_signal);
    ::signal(SIGHUP, on_signal);
    g_node = std::make_shared<KvdServer>(id, cluster, port);

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

void KvdServer::stop()
{
    LOG_DEBUG("stopping");
    if (transport_) {
        transport_->stop();
        transport_ = nullptr;
    }

    io_service_.stop();
}

}
