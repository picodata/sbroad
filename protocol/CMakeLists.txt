cmake_minimum_required(VERSION 2.8)

project(protocol C)

include(cmake/utils.cmake)

if (NOT TARANTOOL_INCLUDE_DIRS)
    message(FATAL_ERROR
        "Specify the path to tarantool include dirs via TARANTOOL_INCLUDE_DIRS variable.")
endif()

include(cmake/FindMsgPuck.cmake)

if (NOT MSGPUCK_FOUND)
    message(FATAL_ERROR
        "MsgPuck headers were not found. Please install MsgPuck library.")
endif()

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${TARANTOOL_INCLUDE_DIRS})
include_directories(${MSGPUCK_INCLUDE_DIRS})

add_subdirectory(src)
add_subdirectory(test)
