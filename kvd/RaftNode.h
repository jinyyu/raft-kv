#pragma once
#include <string>
#include <stdint.h>
#include <memory>
#include <boost/asio.hpp>
#include <vector>

namespace kvd
{

class RaftNode
{
public:
    static void main(uint64_t id, const std::string &cluster, uint16_t port);

    explicit RaftNode(uint64_t id, const std::string &cluster, uint16_t port);

    ~RaftNode();

    void stop();
private:
    void schedule()
    {
        io_service_.run();
    }

    boost::asio::io_service io_service_;
    uint64_t id_;
    std::vector<std::string> peers_;
};

typedef std::shared_ptr<RaftNode> RaftNodePtr;

}