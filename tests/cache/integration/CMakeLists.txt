target_add_test(test_cache_minio client-cache cache-manager cache-origin meta-fbs common ${AWSSDK_LINK_LIBRARIES})
target_include_directories(test_cache_minio PRIVATE ${AWSSDK_INCLUDE_DIR})
