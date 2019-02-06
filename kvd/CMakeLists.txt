
find_package(PkgConfig REQUIRED)
pkg_check_modules(GLIB2 glib-2.0>=2.10 REQUIRED)
include_directories(${GLIB2_INCLUDE_DIRS})
set(LIBS ${LIBS} ${GLIB2_LIBRARIES})

find_package(Boost REQUIRED COMPONENTS system filesystem)
include_directories(${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} Boost::system Boost::filesystem)


set(SRC kvd.cpp)
add_executable(kvd ${SRC})
target_link_libraries(kvd ${LIBS})