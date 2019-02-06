cmake_minimum_required(VERSION 3.5)
project(kvd)

set(CMAKE_CXX_STANDARD 11)

add_definitions(-Wall)

add_subdirectory(kvd)

find_package(PkgConfig REQUIRED)

pkg_check_modules(GLIB2 glib-2.0>=2.10 REQUIRED)
include_directories(${GLIB2_INCLUDE_DIRS})
set(LIBS ${LIBS} ${GLIB2_LIBRARIES})


find_package(Boost REQUIRED COMPONENTS system filesystem )
include_directories(${Boost_INCLUDE_DIRS})
set(LIBS {LIBS} Boost::system Boost::filesystem)


if (GoogleTest_FOUND)
    include_directories(${GoogleTest_INCLUDE_DIRS})
    set(LIBS
        ${LIBS} ${GoogleTest_LIBRARIES})
    add_subdirectory(tests)
endif (GoogleTest_FOUND)

configure_file(Procfile.in Procfile)


