#include "kvd/KvdServer.h"
#include "kvd/transport/AsioTransport.h"
#include "kvd/common/log.h"
#include <boost/algorithm/string.hpp>

namespace kvd
{

KvdServer::KvdServer(uint64_t id, const std::string& cluster, uint16_t port)
    : timer_(io_service_),
      id_(id)
{
    boost::split(peers_, cluster, boost::is_any_of(","));
    if (peers_.empty()) {
        LOG_FATAL("invalid args %s", cluster.c_str());
    }

    storage_ = std::make_shared<MemoryStorage>();

    Config c;
    std::vector<PeerContext> peers;
    node_ = std::make_shared<RawNode>(c, peers, io_service_);
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
    timer_.expires_from_now(boost::posix_time::seconds(1));
    timer_.async_wait([self](const boost::system::error_code& err) {
        if (err) {
            LOG_ERROR("timer waiter error %s", err.message().c_str());
            return;
        }
        self->start_timer();
    });
}

void KvdServer::schedule()
{
    start_timer();
    io_service_.run();
}

Status KvdServer::process(proto::MessagePtr msg)
{
    LOG_DEBUG("no impl yet");
    return Status::ok();
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

    AsioTransport* transport = new AsioTransport(g_node, g_node->id_);

    TransporterPtr ptr((Transporter*) transport);
    std::string& host = g_node->peers_[id - 1];
    ptr->start(host);
    g_node->transport_ = ptr;

    for (uint64_t i = 0; i < g_node->peers_.size(); ++i) {
        uint64_t peer = i + 1;
        if (peer == g_node->id_) {
            continue;
        }
        ptr->add_peer(peer, g_node->peers_[i]);
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
