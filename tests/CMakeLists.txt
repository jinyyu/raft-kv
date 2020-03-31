
set(LIBS
    ${LIBS}
    raft-kv++)

enable_testing()
find_package(GTest REQUIRED)

add_executable(test_msgpack test_msgpack.cpp)
target_link_libraries(test_msgpack ${LIBS})
gtest_add_tests(TARGET test_msgpack)

add_executable(test_bytebuffer test_bytebuffer.cpp)
target_link_libraries(test_bytebuffer ${LIBS})
gtest_add_tests(TARGET test_bytebuffer)

add_executable(test_proto test_proto.cpp)
target_link_libraries(test_proto ${LIBS})
gtest_add_tests(TARGET test_proto)

add_executable(test_storage test_storage.cpp)
target_link_libraries(test_storage ${LIBS})
gtest_add_tests(TARGET test_storage)

add_executable(test_unstable test_unstable.cpp)
target_link_libraries(test_unstable ${LIBS})
gtest_add_tests(TARGET test_unstable)

add_executable(test_raftlog test_raftlog.cpp)
target_link_libraries(test_raftlog ${LIBS})
gtest_add_tests(TARGET test_raftlog)

add_executable(test_progress test_progress.cpp)
target_link_libraries(test_progress ${LIBS})
gtest_add_tests(TARGET test_progress)

add_executable(test_raft test_raft.cpp network.hpp)
target_link_libraries(test_raft ${LIBS})
gtest_add_tests(TARGET test_raft)

add_executable(test_rawnode test_rawnode.cpp)
target_link_libraries(test_rawnode ${LIBS})
gtest_add_tests(TARGET test_rawnode)

add_executable(string_match string_match.cpp)
target_link_libraries(string_match ${LIBS})
gtest_add_tests(TARGET string_match)

add_executable(raft_snap_test raft_snap_test.cpp network.hpp)
target_link_libraries(raft_snap_test ${LIBS})
gtest_add_tests(TARGET raft_snap_test)