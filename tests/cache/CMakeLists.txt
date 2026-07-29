file(GLOB_RECURSE CACHE_UNIT_TEST_FILES CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")
list(FILTER CACHE_UNIT_TEST_FILES EXCLUDE REGEX "^integration/")
add_executable(test_cache ${CACHE_UNIT_TEST_FILES})
target_link_libraries(test_cache gmock test_main cache-origin cache-metrics cache-fbs common)
target_include_directories(test_cache PUBLIC ${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}
                                            ${PROJECT_BINARY_DIR}/src ${PROJECT_BINARY_DIR})
add_test(NAME test_cache COMMAND test_cache)
set_target_properties(test_cache PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests")

if(HF3FS_ENABLE_CACHE_INTEGRATION_TESTS)
    add_subdirectory(integration)
endif()

if(HF3FS_ENABLE_CACHE)
    add_subdirectory(integration/aws)
else()
    add_test(NAME test_cache_aws_s3
             COMMAND ${CMAKE_COMMAND} -E echo "SKIPPED: build has HF3FS_ENABLE_CACHE=OFF")
    set_tests_properties(test_cache_aws_s3 PROPERTIES SKIP_REGULAR_EXPRESSION "SKIPPED:")
endif()
