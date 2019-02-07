#pragma once
#include <vector>
#include <stdint.h>

namespace kvd
{


class ByteBuffer
{
    explicit ByteBuffer()
        : reader_(0),
          writer_(0)
    {

    }

    ~ByteBuffer()
    {

    }

    void put()


public:
    uint32_t reader_;
    uint32_t writer_;
    std::vector<uint8_t> buff_;
};

}
