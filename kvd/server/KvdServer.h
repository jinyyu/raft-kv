#pragma once
#include <string>
#include <stdint.h>
#include <memory>
#include <vector>
#include <kvd/transport/Transport.h>
#include <kvd/raft/Node.h>
#include <kvd/server/HTTPServer.h>

namespace kvd
{

typedef std::function<void(const Status&)> StatusCallback;

class KvdServer: public RaftServer, public std::enable_shared_from_this<KvdServer>
{
public:
    static void main(uint64_t id, const std::string& cluster, uint16_t port);

    explicit KvdServer(uint64_t id, const std::string& cluster, uint16_t port);

    virtual ~KvdServer();

    void stop();

    void propose(std::shared_ptr<std::vector<uint8_t>> data, const StatusCallback& callback);

    virtual void process(proto::MessagePtr msg, const StatusCallback& callback);

    virtual void is_id_removed(uint64_t id, const std::function<void(bool)>& callback);

    virtual void report_unreachable(uint64_t id);

    virtual void report_snapshot(uint64_t id, SnapshotStatus status);

    bool publish_entries(const std::vector<proto::EntryPtr>& entries);
    void entries_to_apply(const std::vector<proto::EntryPtr>& entries, std::vector<proto::EntryPtr>& ents);
    void maybe_trigger_snapshot();

private:
    void start_timer();
    void check_raft_ready();
    void schedule();

    uint16_t port_;
    pthread_t raft_loop_id_;
    boost::asio::io_service raft_loop_;
    pthread_t server_loop_id_;
    boost::asio::io_service server_loop_;
    boost::asio::deadline_timer timer_;
    uint64_t id_;
    std::vector<std::string> peers_;
    uint64_t last_index_;
    proto::ConfStatePtr conf_state_;
    uint64_t snapshot_index_;
    uint64_t applied_index_;

    RawNodePtr node_;
    TransporterPtr transport_;
    MemoryStoragePtr storage_;
    std::shared_ptr<HTTPServer> http_server_;
};
typedef std::shared_ptr<KvdServer> KvdServerPtr;

}