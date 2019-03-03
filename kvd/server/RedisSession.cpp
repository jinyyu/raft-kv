#include <kvd/server/RedisSession.h>
#include <kvd/common/log.h>
#include <unordered_map>
#include <kvd/server/RedisServer.h>
#include <glib.h>

namespace kvd
{

#define RECEIVE_BUFFER_SIZE (1024 * 512)

namespace shared
{

static const char* ok = "+OK\r\n";
static const char* err = "-ERR %s";
static const char* wrong_type = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
static const char* unknown_command = "-ERR unknown command `%s`\r\n";
static const char* wrong_number_arguments = "-ERR wrong number of arguments for '%s' command\r\n";
static const char* pong = "+PONG\r\n";
static const char* null = "$-1\r\n";

typedef std::function<void(RedisSessionPtr, struct redisReply* reply)> CommandCallback;

static std::unordered_map<std::string, CommandCallback> command_table = {
    {"ping", RedisSession::ping_command},
    {"PING", RedisSession::ping_command},
    {"get", RedisSession::get_command},
    {"GET", RedisSession::get_command},
    {"set", RedisSession::set_command},
    {"SET", RedisSession::set_command},
    {"del", RedisSession::del_command},
    {"DEL", RedisSession::del_command},
};

}

RedisSession::RedisSession(std::weak_ptr<RedisServer> server, boost::asio::io_service& io_service)
    : quit_(false),
      server_(std::move(server)),
      socket_(io_service),
      read_buffer_(RECEIVE_BUFFER_SIZE),
      reader_(redisReaderCreate())
{

}

void RedisSession::start()
{
    if (quit_) {
        return;
    }
    auto self = shared_from_this();
    auto buffer = boost::asio::buffer(read_buffer_.data(), read_buffer_.size());
    auto handler = [self](const boost::system::error_code& error, size_t bytes) {
        if (bytes == 0) {
            return;
        }
        if (error) {
            LOG_DEBUG("read error %s", error.message().c_str());
            return;
        }

        self->handle_read(bytes);

    };
    socket_.async_read_some(buffer, handler);
}

void RedisSession::handle_read(size_t bytes)
{
    uint8_t* start = read_buffer_.data();
    uint8_t* end = read_buffer_.data() + bytes;
    int err = REDIS_OK;
    std::vector<struct redisReply*> replays;

    while (!quit_ && start < end) {
        uint8_t* p = (uint8_t*) memchr(start, '\n', bytes);
        if (!p) {
            break;
        }

        size_t n = p + 1 - start;
        err = redisReaderFeed(reader_, (const char*) start, n);
        if (err != REDIS_OK) {
            LOG_DEBUG("redis protocol error %d, %s", err, reader_->errstr);
            quit_ = true;
            break;
        }

        struct redisReply* reply = NULL;
        err = redisReaderGetReply(reader_, (void**) &reply);
        if (err != REDIS_OK) {
            LOG_DEBUG("redis protocol error %d, %s", err, reader_->errstr);
            quit_ = true;
            break;
        }
        if (reply) {
            replays.push_back(reply);
        }

        start += n;
        bytes -= n;
    }
    if (err == REDIS_OK) {
        for (struct redisReply* reply : replays) {
            on_redis_reply(reply);
        }
        this->start();
    }

    for (struct redisReply* reply : replays) {
        freeReplyObject(reply);
    }
}

void RedisSession::on_redis_reply(struct redisReply* reply)
{
    char buffer[256];
    if (reply->type != REDIS_REPLY_ARRAY) {
        LOG_WARN("wrong type %d", reply->type);
        send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }

    if (reply->elements < 1) {
        LOG_WARN("wrong elements %lu", reply->elements);
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "");
        send_reply(buffer, n);
        return;
    }

    if (reply->element[0]->type != REDIS_REPLY_STRING) {
        LOG_WARN("wrong type %d", reply->element[0]->type);
        send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }

    std::string command(reply->element[0]->str, reply->element[0]->len);
    auto it = shared::command_table.find(command);
    if (it == shared::command_table.end()) {
        int n = snprintf(buffer, sizeof(buffer), shared::unknown_command, command.c_str());
        send_reply(buffer, n);
        return;
    }
    shared::CommandCallback& cb = it->second;
    cb(shared_from_this(), reply);
}

void RedisSession::send_reply(const char* data, uint32_t len)
{
    uint32_t bytes = send_buffer_.readable_bytes();
    send_buffer_.put((uint8_t*) data, len);
    if (bytes == 0) {
        start_send();
    }
}

void RedisSession::start_send()
{
    if (!send_buffer_.readable()) {
        return;
    }
    auto self = shared_from_this();
    uint32_t remaining = send_buffer_.readable_bytes();
    auto buffer = boost::asio::buffer(send_buffer_.reader(), remaining);
    auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
        if (error || bytes == 0) {
            LOG_DEBUG("send error %s", error.message().c_str());
            return;
        }
        std::string str((const char*) self->send_buffer_.reader(), bytes);
        self->send_buffer_.read_bytes(bytes);
        self->start_send();
    };
    boost::asio::async_write(socket_, buffer, handler);
}

void RedisSession::ping_command(std::shared_ptr<RedisSession> self, struct redisReply* reply)
{
    self->send_reply(shared::pong, strlen(shared::pong));
}

void RedisSession::get_command(std::shared_ptr<RedisSession> self, struct redisReply* reply)
{
    assert(reply->type = REDIS_REPLY_ARRAY);
    assert(reply->elements > 0);
    char buffer[256];

    if (reply->elements != 2) {
        LOG_WARN("wrong elements %lu", reply->elements);
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "get");
        self->send_reply(buffer, n);
        return;
    }

    if (reply->element[1]->type != REDIS_REPLY_STRING) {
        LOG_WARN("wrong type %d", reply->element[1]->type);
        self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }

    std::string value;
    std::string key(reply->element[1]->str, reply->element[1]->len);
    bool get = self->server_.lock()->get(key, value);
    if (!get) {
        self->send_reply(shared::null, strlen(shared::null));
    }
    else {
        char* str = g_strdup_printf("$%lu\r\n%s\r\n", value.size(), value.c_str());
        self->send_reply(str, strlen(str));
        g_free(str);
    }
}

void RedisSession::set_command(std::shared_ptr<RedisSession> self, struct redisReply* reply)
{
    assert(reply->type = REDIS_REPLY_ARRAY);
    assert(reply->elements > 0);
    char buffer[256];

    if (reply->elements != 3) {
        LOG_WARN("wrong elements %lu", reply->elements);
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "set");
        self->send_reply(buffer, n);
        return;
    }

    if (reply->element[1]->type != REDIS_REPLY_STRING || reply->element[2]->type != REDIS_REPLY_STRING) {
        LOG_WARN("wrong type %d", reply->element[1]->type);
        self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }
    std::string key(reply->element[1]->str, reply->element[1]->len);
    std::string value(reply->element[2]->str, reply->element[2]->len);
    self->server_.lock()->set(std::move(key), std::move(value), [self](const Status& status) {
        if (status.is_ok()) {
            self->send_reply(shared::ok, strlen(shared::ok));
        }
        else {
            char buff[256];
            int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
            self->send_reply(buff, n);
        }
    });
}

void RedisSession::del_command(std::shared_ptr<RedisSession> self, struct redisReply* reply)
{
    assert(reply->type = REDIS_REPLY_ARRAY);
    assert(reply->elements > 0);
    char buffer[256];

    if (reply->elements <= 1) { ;
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "del");
        self->send_reply(buffer, n);
        return;
    }

    std::vector<std::string> keys;
    for (size_t i = 1; i < reply->elements; ++i) {
        redisReply* element = reply->element[i];
        if (element->type != REDIS_REPLY_STRING) {
            self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
            return;
        }

        keys.emplace_back(element->str, element->len);
    }

    self->server_.lock()->del(std::move(keys), [self](const Status& status) {
        if (status.is_ok()) {
            self->send_reply(shared::ok, strlen(shared::ok));
        }
        else {
            char buff[256];
            int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
            self->send_reply(buff, n);
        }
    });
}

}
