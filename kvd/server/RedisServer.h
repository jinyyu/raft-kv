#pragma once
#include <boost/asio.hpp>
#include <unordered_map>
#include <thread>
#include <future>
#include <kvd/common/Status.h>
#include <kvd/raft/proto.h>
#include <msgpack.hpp>

namespace kvd
{


struct RaftCommit
{
    static const uint8_t kCommitSet = 0;
    static const uint8_t kCommitDel = 1;

    explicit RaftCommit(uint8_t type, std::string&& key, std::string&& value)
        : type(type),
          key(std::move(key)),
          value(std::move(value))
    {}
    RaftCommit()
    {}

    uint8_t type;
    std::string key;
    std::string value;
    MSGPACK_DEFINE (type, key, value);
};

class KvdServer;
class RedisServer: public std::enable_shared_from_this<RedisServer>
{
public:
    explicit RedisServer(std::weak_ptr<KvdServer> server, uint16_t port);

    ~RedisServer();

    void stop()
    {
        io_service_.stop();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void start(std::promise<pthread_t>& promise);

    bool get(const std::string& key, std::string& value)
    {
        auto it = key_values_.find(key);
        if (it != key_values_.end()) {
            value = it->second;
            return true;
        }
        else {
            return false;
        }
    }

    void set(std::string key, std::string value, const std::function<void(const Status&)>& callback);

    void read_commit(proto::EntryPtr entry);

private:
    void start_accept();

    std::weak_ptr<KvdServer> server_;
    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    std::thread worker_;
    std::unordered_map<std::string, std::string> key_values_;
};

}
