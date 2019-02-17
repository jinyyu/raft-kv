#include <kvd/raft/ReadOnly.h>

namespace kvd
{


void ReadOnly::last_pending_request_ctx(std::vector<uint8_t>& ctx)
{
    if (read_index_queue.empty()) {
        return;
    }

    ctx.insert(ctx.end(), read_index_queue.back().begin(), read_index_queue.back().end());
}

}
