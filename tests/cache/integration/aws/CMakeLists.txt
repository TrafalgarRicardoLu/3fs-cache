target_add_test(test_cache_aws_s3 cache-origin common ${AWSSDK_LINK_LIBRARIES})
target_include_directories(test_cache_aws_s3 PRIVATE ${AWSSDK_INCLUDE_DIR})
