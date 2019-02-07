#include "kvd/KvdServer.h"
#include "kvd/common/log.h"
#include <boost/algorithm/string.hpp>

namespace kvd
{


KvdServer::KvdServer(uint64_t id, const std::string &cluster, uint16_t port)
    : id_(id)
{
    boost::split(peers_, cluster, boost::is_any_of(","));
    if (peers_.empty()) {
        LOG_DEBUG("invalid args %s", cluster.c_str());
        exit(0);
    }
}

KvdServer::~KvdServer()
{
    LOG_DEBUG("stopped");
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
    if (g_node) {
        g_node->stop();
    }
}

void KvdServer::main(uint64_t id, const std::string &cluster, uint16_t port)
{
    g_node = std::make_shared<KvdServer>(id, cluster, port);
    g_node->schedule();
}

void KvdServer::stop()
{
    LOG_DEBUG("stopping");
    io_service_.stop();
}


}
