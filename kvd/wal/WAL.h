#pragma once
#include <kvd/wal/util.h>
#include <boost/asio.hpp>

namespace kvd
{

namespace wal
{

class WAL
{
public:
    explicit WAL(const std::string& dir, const std::vector<uint8_t>& metadata);

    ~WAL();

public:
    boost::asio::io_service io_service_;

};

}



}
