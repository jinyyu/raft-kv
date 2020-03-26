#include <cstdio>
#include "raft-kv/raft/proto.h"
using namespace kv;

int main(int argc, char* argv[])
{
    for (int i = 0; i <= proto::MsgPreVoteResp; ++i) {
        const char* str = proto::msg_type_to_string(i);
        fprintf(stderr, " %d, %s\n", i, str);
    }


    for (int i = 0; i < 2; ++i) {
        const char* str = proto::entry_type_to_string(i);
        fprintf(stderr, " %d, %s\n", i, str);
    }
}