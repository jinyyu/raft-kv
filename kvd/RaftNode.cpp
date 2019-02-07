#include "kvd/RaftNode.h"
#include "kvd/log.h"
#include <signal.h>
#include <boost/algorithm/string.hpp>

namespace kvd
{


RaftNode::RaftNode(uint64_t id, const std::string& cluster, uint16_t port)
  :id_(id)
{
    boost::split(peers_, cluster,boost::is_any_of(","));
    if (peers_.empty()) {
        LOG_DEBUG("invalid args %s", cluster.c_str());
        exit(0);
    }
}

RaftNode::~RaftNode()
{
    LOG_DEBUG("stopped");
}

static RaftNodePtr g_node = nullptr;


void on_signal(int)
{
    if (g_node) {
        g_node->stop();
    }
}

void RaftNode::main(uint64_t id, const std::string& cluster, uint16_t port)
{
    g_node = std::make_shared<RaftNode>(id, cluster, port);
    g_node->schedule();
}

void RaftNode::stop()
{
    LOG_DEBUG("stopping");
    io_service_.stop();
}

}