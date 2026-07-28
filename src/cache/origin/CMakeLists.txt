target_add_lib(cache-origin common cache-fbs)

if(HF3FS_ENABLE_CACHE)
    target_include_directories(cache-origin PRIVATE ${AWSSDK_INCLUDE_DIR})
    target_link_libraries(cache-origin ${AWSSDK_LINK_LIBRARIES})
endif()
