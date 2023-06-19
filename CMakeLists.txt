cmake_minimum_required(VERSION 3.8)
project(raft)

set(CMAKE_CXX_STANDARD 17)

if(${LOG} STREQUAL on)
  add_compile_definitions(ENABLE_LOG)
endif()

# enable recording more fine granularity performance records
if(${PERF} STREQUAL on)
  add_compile_definitions(ENABLE_PERF_RECORDING)
endif()

include_directories(${CMAKE_SOURCE_DIR}/RCF/include)
add_subdirectory(${CMAKE_SOURCE_DIR}/RCF)

include_directories(${CMAKE_SOURCE_DIR}/third-party/leveldb/include)

add_subdirectory(${CMAKE_SOURCE_DIR}/third-party)
add_subdirectory(${CMAKE_SOURCE_DIR}/raft)
add_subdirectory(${CMAKE_SOURCE_DIR}/kv)
add_subdirectory(${CMAKE_SOURCE_DIR}/bench)



