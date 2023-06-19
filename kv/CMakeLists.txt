include_directories(${LEVELDB_DIR}/include)
include_directories(${ROCKSDB_DIR}/include)

find_library(librocksdb NAMES librocksdb.a rocksdb)
if (NOT librocksdb)
    message(FATAL_ERROR "librocksdb not found")
else()
  message("-- Found Library rocksdb: " ${librocksdb})
endif()

find_library(DL_LIBRARY NAMES libdl.so dl)
if (NOT DL_LIBRARY)
  message(FATAL_ERROR "LIBDL not found")
endif()

find_package("ZLIB" REQUIRED)
if(ZLIB_FOUND)
else()
    message(FATAL_ERROR "Zlib not found")
endif()

find_package(BZip2 REQUIRED)
if (NOT BZIP2_FOUND)
  message(FATAL_ERROR "BZip2 not found")
endif()

find_library(LZ4_LIBRARIES NAMES liblz4.a lz4)
if (NOT LZ4_LIBRARIES)
  message(FATAL_ERROR "LZ4 not found")
else()
  message("-- Found Library LZ4 " ${LZ4_LIBRARIES})
endif()

find_library(SNAPPY_LIBRARIES NAMES libsnappy.a snappy)
if (NOT SNAPPY_LIBRARIES)
  message(FATAL_ERROR "Snappy not found")
else()
  message("-- Found Library Snappy " ${SNAPPY_LIBRARIES})
endif()

find_library(ZSTD_LIBRARIES NAMES libzstd.a zstd)
if (NOT ZSTD_LIBRARIES)
  message(FATAL_ERROR "ZSTD not found")
else()
  message("-- Found Library ZSTD " ${ZSTD_LIBRARIES})
endif()

set(
  KV_SRC 
  type.cc
  config.cc
  kv_format.cc
  storage_engine.cc
  kv_server.cc
  rpc.cc
  kv_node.cc
  client.cc
)

set(
  ROCKSDB_LIB
  ${librocksdb}
  ${DL_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${BZIP2_LIBRARIES}
  ${SNAPPY_LIBRARIES}
  ${LZ4_LIBRARIES}
  ${ZSTD_LIBRARIES}
)


add_library(kv ${KV_SRC})
target_link_libraries(kv raft rcf ${ROCKSDB_LIB})
target_include_directories(kv PUBLIC ${CMAKE_CURRENT_LIST_DIR})

add_subdirectory(test)

