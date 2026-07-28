find_program(CLANG_FORMAT NAMES clang-format-14 clang-format)
if(CLANG_FORMAT)
    execute_process(
        COMMAND ${CLANG_FORMAT} --version
        OUTPUT_VARIABLE CLANG_FORMAT_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT CLANG_FORMAT_VERSION MATCHES "clang-format version 14\\.")
        message(FATAL_ERROR "clang-format 14 required, found: ${CLANG_FORMAT_VERSION}")
    endif()
    message(STATUS "Found clang-format at ${CLANG_FORMAT}")

    set(SOURCE_DIRS
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/tests
        ${CMAKE_SOURCE_DIR}/benchmarks
    )

    # For now, it just hard codes the source files list to globs. That works
    # fine until we have another directory in `src/`. We should ideally gather
    # this from SOURCE_FILES list. But, should filter the thirs_party sources.
    # Taking a quick route for now. We should deal with it sometime down the line.
    add_custom_target(format
            COMMENT "Running clang-format"
            COMMAND find ${SOURCE_DIRS} -name '*.cc' -o -name '*.cpp' -o -name '*.h' | grep -v "_generated.h" | xargs ${CLANG_FORMAT} -i)

    add_custom_target(check-format
            COMMENT "Running clang-format"
            COMMAND find ${SOURCE_DIRS} -name '*.cc' -o -name '*.cpp' -o -name '*.h' | grep -v "_generated.h" | xargs ${CLANG_FORMAT} --Werror --dry-run)
else()
    message(FATAL_ERROR "clang-format-14 not found")
endif()
