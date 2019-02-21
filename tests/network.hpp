#include <functional>
#include <kvd/common/log.h>
#include <kvd/raft/proto.h>
#include <boost/functional/hash.hpp>
#include <kvd/raft/Raft.h>
#include <kvd/common/RandomDevice.h>

using namespace kvd;

Config newTestConfig(uint64_t id,
                     std::vector<uint64_t> peers,
                     uint32_t election,
                     uint32_t heartbeat,
                     StoragePtr storage)
{
    Config c;
    c.id = id;
    c.peers = peers;
    c.election_tick = election;
    c.heartbeat_tick = heartbeat;
    c.storage = storage;
    c.max_uncommitted_entries_size = std::numeric_limits<uint32_t>::max();
    c.max_inflight_msgs = 256;
    return c;
}

struct connem
{
    uint64_t from;
    uint64_t to;
};

bool operator==(const connem& lhs, const connem& rhs)
{
    return lhs.from == rhs.from && lhs.to == rhs.to;
}

namespace std
{

template<>
struct hash<connem>
{
    std::size_t operator()(const connem& c) const
    {
        return boost::hash_value(tie(c.from, c.to));
    }
};

}

std::vector<uint64_t> idsBySize(size_t size)
{
    std::vector<uint64_t> ids(size);
    for (size_t i = 0; i < size; i++) {
        ids[i] = 1 + i;
    }
    return ids;
}

struct Network
{
    explicit Network(const std::vector<RaftPtr>& peers)
        : Network([](Config& c) {}, peers)
    {

    }

    explicit Network(const std::function<void(Config& c)> configFunc, const std::vector<RaftPtr>& peers)
        : dev(0, 100)
    {
        size_t size = peers.size();
        auto peerAddrs = idsBySize(size);

        for (size_t j = 0; j < peers.size(); ++j) {
            auto p = peers[j];
            uint64_t id = peerAddrs[j];

            if (p == nullptr) {
                auto mem = std::make_shared<MemoryStorage>();
                storage[id] = mem;
                Config cfg = newTestConfig(id, peerAddrs, 10, 1, mem);
                configFunc(cfg);
                RaftPtr sm(new Raft(cfg));
                this->peers[id] = sm;

            }
            else {
                std::unordered_map<uint64_t, bool> learners;
                for (size_t i = 0; i < p->learner_prs_.size(); ++i) {
                    learners[i] = true;
                }
                p->id_ = id;
                p->prs_.clear();
                p->learner_prs_.clear();

                for (size_t i = 0; i < size; i++) {
                    auto it = learners.find(peerAddrs[i]);
                    ProgressPtr pr(new Progress(0));

                    if (it != learners.end()) {
                        pr->is_learner = true;
                        p->learner_prs_[peerAddrs[i]] = pr;
                    }
                    else {
                        pr->is_learner = false;
                        p->prs_[peerAddrs[i]] = pr;
                    }
                }
                p->reset(p->term_);
            }
        }
    }

    void ignore(proto::MessageType t)
    {
        ignorem[t] = true;
    }

    void recover()
    {
        dropm.clear();
        ignorem.clear();
    }

    void send(std::vector<proto::MessagePtr> msgs)
    {
        if (msgs.size() > 0) {
            auto m = msgs[0];
            auto p = peers[m->to];
            p->step(m);
        }
    }

    std::vector<proto::MessagePtr> filter(const std::vector<proto::MessagePtr>& msgs)
    {
        std::vector<proto::MessagePtr> mm;

        for (proto::MessagePtr m :  msgs) {
            if (ignorem[m->type]) {
                continue;
            }

            switch (m->type) {
            case proto::MsgHup:
                // hups never go over the network, so don't drop them but panic
                LOG_FATAL("unexpected msgHup");
            default: {
                connem c;
                c.from = m->from;
                c.to = m->to;
                auto perc = dropm[c];

                auto n = (float) dev.gen();
                if (n < perc * 100) {
                    continue;
                }
            }
            }

            if (this->msgHook) {
                if (!this->msgHook(m)) {
                    continue;
                }
            }
            mm.push_back(m);

        }
        return mm;
    }

    RandomDevice dev;
    std::unordered_map<uint64_t, RaftPtr> peers;
    std::unordered_map<uint64_t, MemoryStoragePtr> storage;
    std::unordered_map<connem, float> dropm;
    std::unordered_map<proto::MessageType, bool> ignorem;

    // msgHook is called for each message sent. It may inspect the
    // message and return true to send it or false to drop it.

    std::function<bool(proto::MessagePtr)> msgHook;
};
