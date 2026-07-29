target_add_test(test_cache cache-origin cache-metrics cache-fbs common)

if(HF3FS_ENABLE_CACHE_INTEGRATION_TESTS)
    add_subdirectory(integration)
endif()
