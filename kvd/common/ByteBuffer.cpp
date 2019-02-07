#include "kvd/common/ByteBuffer.h"
#include <assert.h>
#include <string.h>
#include <kvd/common/log.h>

namespace kvd
{
static uint32_t MIN_BUFFERING = 4096;

ByteBuffer::ByteBuffer()
    : reader_(0),
      writer_(0),
      buff_(MIN_BUFFERING)
{

}

void ByteBuffer::put(const uint8_t *data, uint32_t len)
{
    uint32_t left = static_cast<uint32_t>(buff_.size()) - writer_;
    if (left < len) {
        buff_.resize(buff_.size() * 2 + len, 0);
    }
    memcpy(buff_.data() + writer_, data, len);
    writer_ += len;

    LOG_ERROR("put %d, reader = %d, writer = %d", len, reader_, writer_);
}


uint32_t ByteBuffer::remaining() const
{
    assert(writer_ >= reader_);
    return writer_ - reader_;
}

void ByteBuffer::skip_bytes(uint32_t bytes)
{
    LOG_ERROR("skip %d, reader = %d, writer = %d", bytes, reader_, writer_);
    assert(remaining() >= bytes);
    reader_ += bytes;
    may_shrink_to_fit();
}

void ByteBuffer::may_shrink_to_fit()
{
    /*
    if (!remain()) {
        reader_ = writer_ = 0;
    }
     */
}

void ByteBuffer::reset()
{
    reader_ = writer_ = 0;
    buff_.resize(MIN_BUFFERING);
    buff_.shrink_to_fit();
}

}

