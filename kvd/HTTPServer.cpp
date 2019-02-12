#include <kvd/HTTPServer.h>
#include <kvd/third_party/http_parser.h>
#include <kvd/KvdServer.h>
#include <kvd/common/log.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>

namespace kvd
{

static http_parser_settings g_http_parser_request_setting;

class HTTPSession: public std::enable_shared_from_this<HTTPSession>
{
public:
    explicit HTTPSession(std::weak_ptr<HTTPServer> server, boost::asio::io_service& io_service)
        : server(std::move(server)),
          socket(io_service),
          read_finished(false)
    {
        memset(&http_parser_, 0, sizeof(http_parser_));
        http_parser_init(&http_parser_, HTTP_REQUEST);
        http_parser_.data = this;
    }

    ~HTTPSession()
    {

    }

    void start()
    {
        auto self = shared_from_this();
        auto buffer = boost::asio::buffer(self->recv_buffer);
        auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
            if (error || bytes == 0) {
                LOG_DEBUG("read error %s", error.message().c_str());
                return;
            }

            size_t n_parsed = http_parser_execute(&self->http_parser_,
                                                  &g_http_parser_request_setting,
                                                  self->recv_buffer,
                                                  bytes);
            if (bytes != n_parsed) {
                LOG_ERROR("http parse error %s",
                          http_errno_description((enum http_errno) self->http_parser_.http_errno));
                return;
            }
            if (!self->read_finished) {
                self->start();
            }
            else {
                self->handle_request();
            }
        };
        socket.async_read_some(buffer, handler);
    }

    void handle_request()
    {
        if (method == "GET") {
            handle_get();
        }
    }

    void handle_get()
    {
        std::string value;
        bool success = server.lock()->get(url, value);
        rapidjson::Document document;
        document.SetObject();
        document.AddMember("ok", rapidjson::Value(success), document.GetAllocator());
        if (success) {
            document.AddMember("result",
                               rapidjson::Value(value.c_str(), document.GetAllocator()), document.GetAllocator());
        }
        else {
            document.AddMember("result", rapidjson::Value("key not found"), document.GetAllocator());
        }
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
        document.Accept(writer);

        data = std::string(sb.GetString(), sb.GetSize());
        LOG_DEBUG("%s------------------------------", data.c_str());
    }
    std::string url;
    std::string method;
    std::weak_ptr<HTTPServer> server;
    boost::asio::ip::tcp::socket socket;
    http_parser http_parser_;
    bool read_finished;
    std::string data;
    char recv_buffer[512];
};

static int on_request_complete(http_parser* parser)
{
    HTTPSession* session = (HTTPSession*) parser->data;
    session->read_finished = true;
    return 0;
}

static int on_url(http_parser* parser, const char* url, size_t length)
{
    HTTPSession* session = (HTTPSession*) parser->data;
    session->method = http_method_str((http_method) parser->method);
    if (length > 1 && *url == '/') {
        session->url = std::string(url + 1, length-1);
    }

    LOG_DEBUG("%s:%s", session->method.c_str(), session->url.c_str());
    return 0;
}

static int on_body(http_parser* parser, const char* data, size_t length)
{
    HTTPSession* session = (HTTPSession*) parser->data;
    session->data.insert(session->data.end(), data, data + length);
    return 0;
}

typedef std::shared_ptr<HTTPSession> HTTPSessionPtr;

HTTPServer::HTTPServer(std::weak_ptr<KvdServer> server, boost::asio::io_service& io_service, uint16_t port)
    : server_(std::move(server)),
      io_service_(io_service),
      acceptor_(io_service_)
{
    auto address = boost::asio::ip::address::from_string("0.0.0.0");
    auto endpoint = boost::asio::ip::tcp::endpoint(address, port);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(1));
    acceptor_.bind(endpoint);
    acceptor_.listen();
}

void HTTPServer::start()
{
    memset(&g_http_parser_request_setting, 0, sizeof(g_http_parser_request_setting));
    g_http_parser_request_setting.on_url = on_url;
    g_http_parser_request_setting.on_body = on_body;
    g_http_parser_request_setting.on_message_complete = on_request_complete;

    HTTPSessionPtr session(new HTTPSession(shared_from_this(), io_service_));
    acceptor_.async_accept(session->socket, [this, session](const boost::system::error_code& error) {
        if (error) {
            LOG_DEBUG("accept error %s", error.message().c_str());
            return;
        }
        this->start();
        session->start();
    });
    key_values_["kvd"] = "0.1";
}

}

