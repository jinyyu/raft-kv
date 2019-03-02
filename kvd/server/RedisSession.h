#pragma once
#include <memory>
#include <boost/asio.hpp>
#include <hiredis/hiredis.h>
#include <kvd/common/ByteBuffer.h>

namespace kvd
{

class RedisServer;
class RedisSession: public std::enable_shared_from_this<RedisSession>
{
public:
    explicit RedisSession(std::weak_ptr<RedisServer> server, boost::asio::io_service& io_service);

    ~RedisSession()
    {
        redisReaderFree(reader_);
    }

    void start();

    void handle_read(size_t bytes);

    void on_redis_reply(struct redisReply* reply);

    void send_reply(const char* data, uint32_t len);

    void start_send();

    static void ping_command(std::shared_ptr<RedisSession> self, struct redisReply* reply);


public:
    std::weak_ptr<RedisServer> server_;
    boost::asio::ip::tcp::socket socket_;
    std::vector<uint8_t> read_buffer_;
    redisReader* reader_;
    ByteBuffer send_buffer_;
};
typedef std::shared_ptr<RedisSession> RedisSessionPtr;


}