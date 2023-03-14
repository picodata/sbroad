get_all_targets(SOURCE_TARGETS ${PROJECT_SOURCE_DIR}/src)

add_custom_target(test
    DEPENDS ${SOURCE_TARGETS}
    COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py)

add_custom_target(test-verbose
    DEPENDS ${SOURCE_TARGETS}
    COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py
        --verbose)
