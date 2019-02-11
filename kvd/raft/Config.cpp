#include "kvd/raft/Config.h"


namespace kvd
{

Status Config::validate()
{
    if (this->id == 0) {
        return Status::invalid_argument("cannot use none as id");
    }

    if (this->heartbeat_tick <= 0) {
        return Status::invalid_argument("heartbeat tick must be greater than 0");
    }

    if (this->election_tick <= this->heartbeat_tick) {
        return Status::invalid_argument("election tick must be greater than heartbeat tick");
    }

    if (!this->storage) {
        return Status::invalid_argument("storage cannot be nil");
    }

    if (this->max_uncommitted_entries_size == 0) {
        this->max_uncommitted_entries_size = std::numeric_limits<uint64_t>::max();
    }

    // default max_committed_size_per_ready to max_size_per_msg because they were
    // previously the same parameter.
    if (this->max_committed_size_per_ready == 0) {
        max_committed_size_per_ready = this->max_size_per_msg;
    }

    if (this->max_inflight_msgs <= 0) {
        return Status::invalid_argument("max inflight messages must be greater than 0");
    }


    if (this->read_only_option == ReadOnlyLeaseBased && !this->check_quorum) {
        return Status::invalid_argument("check_quorum must be enabled when read_only_option is ReadOnlyLeaseBased");
    }


    return Status::ok();

}

}