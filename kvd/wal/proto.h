#pragma once
#include <stdint.h>
#include <vector>
#include <memory>

namespace kvd
{

namespace wal
{

struct Record
{
    int64_t type;
    uint32_t crc;
    std::vector<uint8_t> data;
};

typedef std::shared_ptr<Record> RecordPtr;

struct Snapshot
{
    uint64_t index;
    uint64_t term;
};
typedef std::shared_ptr<Snapshot> SnapshotPtr;

}

}