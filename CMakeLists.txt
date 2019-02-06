cmake_minimum_required(VERSION 3.5)
project(kvd)

set(CMAKE_CXX_STANDARD 11)

add_definitions(-Wall)
add_definitions(-DDEBUG)

add_subdirectory(kvd)


find_package(PkgConfig REQUIRED)
if (GoogleTest_FOUND)
    include_directories(${GoogleTest_INCLUDE_DIRS})
    set(LIBS
        ${LIBS} ${GoogleTest_LIBRARIES})
    add_subdirectory(tests)
endif (GoogleTest_FOUND)

configure_file(Procfile.in Procfile)
