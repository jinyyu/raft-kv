#pragma once
#include <boost/asio.hpp>
#include <unordered_map>
#include <kvd/common/Status.h>

namespace kvd
{

class KvdServer;
class HTTPServer: public std::enable_shared_from_this<HTTPServer>
{
public:
    explicit HTTPServer(std::weak_ptr<KvdServer> server, boost::asio::io_service& io_service, uint16_t port);

    void start();

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

    void put(std::string key, std::string value, std::function<void(const Status&)> callback);

private:
    std::weak_ptr<KvdServer> server_;
    boost::asio::io_service& io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    std::unordered_map<std::string, std::string> key_values_;
};

}
