

set(LIBS
    ${LIBS}
    kvd++)

add_executable(msgpack test_msgpack.cpp)
target_link_libraries(msgpack ${LIBS})

add_executable(bytebuffer test_bytebuffer.cpp)
target_link_libraries(bytebuffer ${LIBS})

add_executable(proto test_proto.cpp)
target_link_libraries(proto ${LIBS})

add_executable(storage test_storage.cpp)
target_link_libraries(storage ${LIBS})

add_executable(unstable test_unstable.cpp)
target_link_libraries(unstable ${LIBS})