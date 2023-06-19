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

find_library(libisal NAMES isal)
if (NOT libisal)
    MESSAGE(FATAL_ERROR "libraryl isa Not Found")
endif()


SET(LIBS ${libpthread} ${libdl} ${libuuid} ${libisal})

set(
  RAFT_SRC 
  log_entry.cc 
  serializer.cc
  encoder.cc
  util.cc
  log_manager.cc
  storage.cc
  rcf_rpc.cc
  raft.cc
  raft_node.cc
)

add_library(raft ${RAFT_SRC})
target_include_directories(raft PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(raft rcf ${LIBS})
# target_compile_definitions(raft PRIVATE ENABLE_RAFT_LOG="${ENABLE_RAFT_LOG}")

add_subdirectory(test)

