cmake_minimum_required(VERSION 3.8)
SET(CMAKE_CXX_STANDARD 17)

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})


find_library(libpthread NAMES pthread)
if (NOT libpthread)
    MESSAGE(FATAL_ERROR "library pthread Not Found")
endif()

find_library(libdl NAMES dl)
if (NOT libdl)
    MESSAGE(FATAL_ERROR "library dl Not Found")
endif()

find_library(libuuid NAMES uuid)
if (NOT libuuid)
    MESSAGE(FATAL_ERROR "library uuid Not Found")
endif()


SET(LIBS ${libpthread} ${libdl} ${libuuid})

set(TEST_FILES)
file(GLOB TEST_FILES ./*.cc)

foreach(test_file_source ${TEST_FILES}) 
  get_filename_component(test_file_name ${test_file_source} NAME)
  if(${test_file_name} MATCHES "_test.cc$")
    string(REPLACE ".cc" "" test_exec_name ${test_file_name})
    add_executable(${test_exec_name} ${test_file_name})
    target_link_libraries(${test_exec_name} raft rcf ${LIBS} ${GTEST_BOTH_LIBRARIES})
  endif()
endforeach()

include_directories(../kv)

add_executable(bench_server bench_server.cc)
target_link_libraries(bench_server kv gflags)

add_executable(bench_client bench_client.cc)
target_link_libraries(bench_client kv gflags)

add_executable(ycsb_server ycsb_server.cc)
target_link_libraries(ycsb_server kv gflags)

add_executable(ycsb_client ycsb_client.cc)
target_link_libraries(ycsb_client kv gflags)

add_executable(readbench_client readbench_client.cc)
target_link_libraries(readbench_client kv gflags)

add_executable(leaderpre_bench leaderpre_bench.cc)
target_link_libraries(leaderpre_bench kv gflags)



