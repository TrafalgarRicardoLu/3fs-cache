add_executable(test_cache_minio TestMinIO.cc)
target_link_libraries(test_cache_minio gmock test_main client-cache cache-manager cache-origin meta-fbs common
                      ${AWSSDK_LINK_LIBRARIES})
target_include_directories(test_cache_minio PRIVATE ${AWSSDK_INCLUDE_DIR})
target_include_directories(test_cache_minio PUBLIC ${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}
                                                   ${PROJECT_BINARY_DIR}/src ${PROJECT_BINARY_DIR})
add_test(NAME test_cache_minio COMMAND test_cache_minio)
set_target_properties(test_cache_minio PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests")
