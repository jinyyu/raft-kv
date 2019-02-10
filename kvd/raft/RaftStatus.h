#pragma once
#include <kvd/raft/proto.h>


namespace kvd
{


struct RaftStatus
{
    uint64_t id;

};
typedef std::shared_ptr<RaftStatus> RaftStatusPtr;

}
