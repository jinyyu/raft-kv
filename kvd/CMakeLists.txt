
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
    server/KvdServer.cpp
    server/RedisServer.cpp
    )
add_library(kvd++ ${SRC})
target_link_libraries(kvd++ ${LIBS})


add_executable(kvd kvd.cpp)
target_link_libraries(kvd ${LIBS} kvd++)