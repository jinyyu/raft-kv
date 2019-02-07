

set(LIBS
    ${LIBS}
    kvd)


add_executable(msgpack test_msgpack.cpp)
target_link_libraries(msgpack ${LIBS})


add_executable(bytebuffer test_bytebuffer.cpp)
target_link_libraries(bytebuffer ${LIBS})