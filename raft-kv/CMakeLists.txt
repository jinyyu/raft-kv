
set(SRC
    common/log.h
    common/Slice.h
    common/Status.cpp
    common/ByteBuffer.cpp
    common/RandomDevice.cpp
    raft/proto.cpp
    raft/Config.cpp
    raft/Raft.cpp
    raft/Storage.cpp
    raft/Node.cpp
    raft/RaftLog.cpp
    raft/Unstable.cpp
    raft/Progress.cpp
    raft/ReadOnly.cpp
    raft/RaftStatus.cpp
    raft/Ready.cpp
    raft/util.cpp
    transport/proto.h
    transport/Transport.h
    transport/Transport.cpp
    transport/Peer.h
    transport/Server.cpp
    transport/Peer.cpp
    wal/WAL.cpp
    wal/proto.cpp
    wal/util.cpp
    server/KvServer.cpp
    server/RedisSession.cpp
    server/RedisServer.cpp)
add_library(raft-kv++ ${SRC})
target_link_libraries(raft-kv++ ${LIBS})


add_executable(raft-kv raft-kv.cpp)
target_link_libraries(raft-kv ${LIBS} raft-kv++)