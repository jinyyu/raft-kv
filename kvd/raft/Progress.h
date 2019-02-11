#pragma once
#include <memory>

namespace kvd
{

enum ProgressState
{
    ProgressStateProbe = 0,
    ProgressStateReplicat = 1,
    ProgressStateSnapshot = 2
};

struct InFlights
{
    explicit InFlights(uint64_t max_inflight_msgs)
        : start(0),
          count(0),
          buffer(max_inflight_msgs, 0)
    {

    }

    // the starting index in the buffer
    uint32_t start;
    // number of inflights in the buffer
    uint32_t count;

    // buffer contains the index of the last entry
    // inside one message.
    std::vector<uint64_t> buffer;
};

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
class Progress
{
public:
    explicit Progress(uint64_t max_inflight)
        : match(0),
          next(0),
          state(ProgressState::ProgressStateProbe),
          paused(false),
          pending_snapshot(0),
          recent_active(false),
          inflights(new InFlights(max_inflight)),
          is_learner(false)
    {

    }

    uint64_t match;
    uint64_t next;
    // state defines how the leader should interact with the follower.
    //
    // When in ProgressStateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in ProgressStateReplicate, leader optimistically increases next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in ProgressStateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    ProgressState state;

    // paused is used in ProgressStateProbe.
    // When Paused is true, raft should pause sending replication message to this peer.
    bool paused;
    // pending_snapshot is used in ProgressStateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. raft will not resend snapshot until the pending one
    // is reported to be failed.
    uint64_t pending_snapshot;


    // recent_active is true if the progress is recently active. Receiving any messages
    // from the corresponding follower indicates the progress is active.
    // recent_active can be reset to false after an election timeout.
    bool recent_active;


    // inflights is a sliding window for the inflight messages.
    // Each inflight message contains one or more log entries.
    // The max number of entries per message is defined in raft config as MaxSizePerMsg.
    // Thus inflight effectively limits both the number of inflight messages
    // and the bandwidth each Progress can use.
    // When inflights is full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.freeTo with the index of the last
    // received entry.

    std::shared_ptr<InFlights> inflights;

    // is_learner is true if this progress is tracked for a learner.
    bool is_learner;

};
typedef std::shared_ptr<Progress> ProgressPtr;

}