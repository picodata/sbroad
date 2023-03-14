#pragma once

#include <stdint.h>

#include "module.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * Stored procedure that creates a server at the specified address and starts
 * accept loop.
 * Server accept loop runs in a separate fiber.
 * Every connected client runs in a separate fiber.
 *
 * It takes 2 arguments encoded in msgpuck format -
 * the first is a URI in ip:port or unix/:path format, represented as a string,
 * and the second is an timeout after which client will be disconnected,
 * represented as double.
 *
 * @return 0 - on success, -1 in case of error.
 */
int
server_start(box_function_ctx_t *ctx,
	    const char *args, const char *args_end);

/**
 * Stored procedure that stops server accept loop and releases server resources.
 *
 * It has no arguments.
 *
 * @return 0 - on success, -1 in case of error.
 */
int
server_stop(box_function_ctx_t *ctx,
	    const char *args, const char *args_end);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
