add_crate(chunk_engine)

target_add_lib(storage cache-metrics core-app core-service memory-common storage-fbs meta-client mgmtd-client storage-client kv
               analytics aio chunk_engine profiler)
target_include_directories(storage PUBLIC ${CMAKE_SOURCE_DIR}/third_party/leveldb)
target_add_bin(storage_main "storage.cpp" storage jemalloc)
