
set(LIBS
    ${LIBS}
    raft-kv++)

enable_testing()
find_package(GTest REQUIRED)

add_executable(msgpack test_msgpack.cpp)
target_link_libraries(msgpack ${LIBS})
gtest_add_tests(TARGET msgpack)

add_executable(bytebuffer test_bytebuffer.cpp)
target_link_libraries(bytebuffer ${LIBS})
gtest_add_tests(TARGET bytebuffer)

add_executable(proto test_proto.cpp)
target_link_libraries(proto ${LIBS})
gtest_add_tests(TARGET proto)

add_executable(storage test_storage.cpp)
target_link_libraries(storage ${LIBS})
gtest_add_tests(TARGET storage)

add_executable(unstable test_unstable.cpp)
target_link_libraries(unstable ${LIBS})
gtest_add_tests(TARGET unstable)

add_executable(raftlog test_raftlog.cpp)
target_link_libraries(raftlog ${LIBS})
gtest_add_tests(TARGET raftlog)

add_executable(ramdom test_ramdom.cpp)
target_link_libraries(ramdom ${LIBS})
gtest_add_tests(TARGET ramdom)

add_executable(lambda test_lambda.cpp)
target_link_libraries(lambda ${LIBS})
gtest_add_tests(TARGET lambda)

add_executable(progress test_progress.cpp)
target_link_libraries(progress ${LIBS})
gtest_add_tests(TARGET progress)

add_executable(raft test_raft.cpp network.hpp)
target_link_libraries(raft ${LIBS})
gtest_add_tests(TARGET raft)

add_executable(raft2 test_raft2.cpp network.hpp)
target_link_libraries(raft2 ${LIBS})
gtest_add_tests(TARGET raft2)

add_executable(rawnode test_rawnode.cpp)
target_link_libraries(rawnode ${LIBS})
gtest_add_tests(TARGET rawnode)

add_executable(string_match string_match.cpp)
target_link_libraries(string_match ${LIBS})
gtest_add_tests(TARGET string_match)