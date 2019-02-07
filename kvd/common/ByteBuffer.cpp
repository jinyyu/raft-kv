#include "kvd/common/ByteBuffer.h"
#include <assert.h>
#include <string.h>

namespace kvd
{
static uint32_t MIN_BUFFERING = 4096;

ByteBuffer::ByteBuffer()
    : reader_(0),
      writer_(0),
      buff_(MIN_BUFFERING)
{

}

uint32_t ByteBuffer::remaining() const
{
    assert(writer_ >= reader_);
    return writer_ - reader_;
}

void ByteBuffer::skip_bytes(uint32_t bytes)
{
    assert(remaining() >= bytes);
    reader_ += bytes;
    may_shrink_to_fit();
}

void ByteBuffer::may_shrink_to_fit()
{
    uint32_t remaining = this->remaining();

    if (reader_ > remaining * 4) {
        memcpy(buff_.data(), buff_.data(), remaining);
        reader_ = 0;
        writer_ = remaining;
    }
}

void ByteBuffer::reset()
{
    reader_ = writer_ = 0;
    buff_.resize(MIN_BUFFERING);
    buff_.shrink_to_fit();
}

}

