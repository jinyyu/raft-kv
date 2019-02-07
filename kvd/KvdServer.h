#pragma once
#include <string>
#include <stdint.h>
#include <memory>
#include <boost/asio.hpp>
#include <vector>
#include <kvd/transport/Transporter.h>

namespace kvd
{

class KvdServer: public RaftServer
{
public:
    static void main(uint64_t id, const std::string &cluster, uint16_t port);

    explicit KvdServer(uint64_t id, const std::string &cluster, uint16_t port);

    virtual ~KvdServer();

    void stop();

    virtual Status process(proto::MessagePtr msg);
    virtual bool is_id_removed(uint64_t id);
    virtual void report_unreachable(uint64_t id);
    virtual void report_snapshot(uint64_t id, SnapshotStatus status);
private:
    void schedule()
    {
        io_service_.run();
    }

    boost::asio::io_service io_service_;
    uint64_t id_;
    std::vector<std::string> peers_;
};

typedef std::shared_ptr<KvdServer> KvdServerPtr;

}