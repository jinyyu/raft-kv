#include <kvd/raft/Progress.h>

namespace kvd
{


void Progress::become_replicate()
{
    reset_state(ProgressStateReplicate);
    next = match +1;
}

void Progress::reset_state(ProgressState st)
{
    paused = false;
    pending_snapshot = 0;
    this->state = st;
    this->inflights->reset();
}

}
