
set(SRC
    common/log.h
    common/Slice.h
    common/Status.cpp
    common/ByteBuffer.cpp
    raft/proto.cpp
    raft/Raft.cpp
    raft/Node.cpp
    transport/Transporter.h
    transport/AsioTransport.cpp
    KvdServer.cpp
    )
add_library(kvd++ ${SRC})
target_link_libraries(kvd++ ${LIBS})


add_executable(kvd kvd.cpp)
target_link_libraries(kvd ${LIBS} kvd++)