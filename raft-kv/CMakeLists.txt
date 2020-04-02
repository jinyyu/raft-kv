
set(SRC
    common/log.h
    common/slice.h
    common/status.cpp
    common/bytebuffer.cpp
    common/random_device.cpp
    raft/proto.cpp
    raft/config.cpp
    raft/raft.cpp
    raft/storage.cpp
    raft/node.cpp
    raft/raft_log.cpp
    raft/unstable.cpp
    raft/progress.cpp
    raft/readonly.cpp
    raft/raft_status.cpp
    raft/ready.cpp
    raft/util.cpp
    transport/proto.h
    transport/transport.h
    transport/transport.cpp
    transport/peer.h
    transport/raft_server.cpp
    transport/peer.cpp
    wal/WAL.cpp
    wal/proto.cpp
    wal/util.cpp
    server/raft_node.cpp
    server/redis_session.cpp
    server/redis_store.cpp
    snap/snapshotter.cpp)
add_library(raft-kv++ ${SRC})
target_link_libraries(raft-kv++ ${LIBS})


add_executable(raft-kv raft-kv.cpp)
target_link_libraries(raft-kv ${LIBS} raft-kv++)