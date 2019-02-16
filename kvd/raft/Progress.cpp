#include <kvd/raft/Progress.h>
#include <kvd/common/log.h>

namespace kvd
{


void Progress::become_replicate()
{
    reset_state(ProgressStateReplicate);
    next = match + 1;
}

void Progress::reset_state(ProgressState st)
{
    paused = false;
    pending_snapshot = 0;
    this->state = st;
    this->inflights->reset();
}

std::string Progress::string() const
{
    char buffer[256];
    int n = snprintf(buffer,
                     sizeof(buffer),
                     "next = %lu, match = %lu, state = %d, waiting = %d, pendingSnapshot = %lu",
                     next,
                     match,
                     state,
                     is_paused(),
                     pending_snapshot);
    return std::string(buffer, n);
}

bool Progress::is_paused() const
{
    switch (state) {
    case ProgressStateProbe:return paused;
    case ProgressStateReplicate:return inflights->is_full();
    case ProgressStateSnapshot:return true;
    default:LOG_FATAL("unexpected state");
    }
}

}
