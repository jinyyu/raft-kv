#pragma once
#include <stdint.h>
#include <vector>
#include <msgpack.hpp>

namespace kvd
{

namespace proto
{

typedef uint8_t MessageType;

const MessageType MsgHup = 0;
const MessageType MsgBeat = 1;
const MessageType MsgProp = 2;
const MessageType MsgApp = 3;
const MessageType MsgAppResp = 4;
const MessageType MsgVote = 5;
const MessageType MsgVoteResp = 6;
const MessageType MsgSnap = 7;
const MessageType MsgHeartbeat = 8;
const MessageType MsgHeartbeatResp = 9;
const MessageType MsgUnreachable = 10;
const MessageType MsgSnapStatus = 11;
const MessageType MsgCheckQuorum = 12;
const MessageType MsgTransferLeader = 13;
const MessageType MsgTimeoutNow = 14;
const MessageType MsgReadIndex = 15;
const MessageType MsgReadIndexResp = 16;
const MessageType MsgPreVote = 17;
const MessageType MsgPreVoteResp = 18;

const char* msg_type_to_string(MessageType type);

typedef uint8_t EntryType;

const EntryType EntryNormal = 0;
const EntryType EntryConfChange = 1;

const char* entry_type_to_string(EntryType type);

struct Entry
{
    EntryType type;
    uint64_t term;
    uint64_t index;
    std::vector<uint8_t> data;
    MSGPACK_DEFINE (type, term, index, data);
};

struct ConfState
{
    std::vector<uint64_t> nodes;
    std::vector<uint64_t> learners;
    MSGPACK_DEFINE (nodes, learners);
};

struct SnapshotMetadata
{
    ConfState conf_state;
    uint64_t index;
    uint64_t term;
    MSGPACK_DEFINE (conf_state, index, term);
};

struct Snapshot
{
    std::vector<uint8_t> data;
    SnapshotMetadata metadata;
    MSGPACK_DEFINE (data, metadata);
};
typedef std::shared_ptr<Snapshot> SnapshotPtr;

struct Message
{
    MessageType type;
    uint64_t to;
    uint64_t from;
    uint64_t term;
    uint64_t log_term;
    uint64_t index;
    Entry entries;
    uint64_t commit;
    Snapshot snapshot;
    bool reject;
    uint64_t reject_hint;
    std::vector<uint8_t> context;
    MSGPACK_DEFINE (type, to, from, term, log_term, index, entries, commit, snapshot, reject, reject_hint, context);
};
typedef std::shared_ptr<Message> MessagePtr;

}
}