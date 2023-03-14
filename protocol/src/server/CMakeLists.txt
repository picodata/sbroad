build_module(tcpserver server.c)
target_link_libraries(tcpserver PRIVATE
                      tarantool
                      ${MSGPUCK_LIBRARIES})
