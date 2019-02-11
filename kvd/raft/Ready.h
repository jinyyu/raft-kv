#pragma once
#include <kvd/raft/proto.h>
#include <kvd/raft/ReadOnly.h>

namespace kvd
{

enum RaftState
{
    Follower = 0,
    Candidate = 1,
    Leader = 2,
    PreCandidate = 3,
};

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
struct SoftState
{
    explicit SoftState(uint64_t lead, RaftState state)
        : lead(lead), state(state)
    {

    }

    uint64_t lead;       // must use atomic operations to access; keep 64-bit aligned.
    RaftState state;
};
typedef std::shared_ptr<SoftState> SoftStatePtr;

struct Ready
{
    // The current volatile state of a Node.
    // SoftState will be nil if there is no update.
    // It is not required to consume or store SoftState.
    SoftStatePtr SoftState;

    // The current state of a Node to be saved to stable storage BEFORE
    // Messages are sent.
    // HardState will be equal to empty state if there is no update.
    proto::HardState HardState;


    // ReadStates can be used for node to serve linearizable read requests locally
    // when its applied index is greater than the index in ReadState.
    // Note that the readState will be returned when raft receives msgReadIndex.
    // The returned is only valid for the request that requested to read.
    std::vector<ReadState> ReadStates;

    // Entries specifies entries to be saved to stable storage BEFORE
    // Messages are sent
    std::vector<proto::EntryPtr> Entries;

    // Snapshot specifies the snapshot to be saved to stable storage.
    proto::SnapshotPtr snapshot;


    // CommittedEntries specifies entries to be committed to a
    // store/state-machine. These have previously been committed to stable
    // store.
    std::vector<proto::EntryPtr> CommittedEntries;

    // Messages specifies outbound messages to be sent AFTER Entries are
    // committed to stable storage.
    // If it contains a MsgSnap message, the application MUST report back to raft
    // when the snapshot has been received or has failed by calling ReportSnapshot.
    std::vector<proto::MessagePtr> Messages;

    // MustSync indicates whether the HardState and Entries must be synchronously
    // written to disk or if an asynchronous write is permissible.
    bool MustSync;
};
typedef std::shared_ptr<Ready> ReadyPtr;

}
