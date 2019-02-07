
set(SRC
    raft/raft.cpp
    raft/node.cpp)
add_library(kvd ${SRC})
target_link_libraries(kvd ${LIBS})


add_executable(kvd-node kvd.cpp)
target_link_libraries(kvd-node ${LIBS} kvd)