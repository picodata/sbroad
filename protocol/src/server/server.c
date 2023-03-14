
#include <stdarg.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include <module.h>
#include "msgpuck.h"
#include "tarantool/sio.h"
#include "tarantool/diag.h"
#include "tarantool/evio.h"
#include "tarantool/trivia/util.h"

enum {
	SERVER_TIMEOUT_INFINITY = 3600 * 24 * 365 * 10
};

/**
 * cord_on_yield is declared but not defined in the tarantool's core
 * so it must be defined by core users.
 */
void
cord_on_yield() {}

struct server {
	/** Server socket. */
	int socket;
	/** Fiber on which the accept loop runs. */
	struct fiber *fiber;
};

/** Server instance. */
static struct server server;

/** Create new server socket */
static int
server_socket_new(const char *host, const char *service)
{
	struct addrinfo hints;
	memset(&hints, 0, sizeof(struct addrinfo));
	/* Allow IPv4 or IPv6 */
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	/* Loop-back address if host is not specified */
	hints.ai_flags = AI_PASSIVE;
	struct addrinfo *ai;
	const double delay1s = 1.;
	int rc = coio_getaddrinfo(host, service, &hints, &ai, delay1s);
	if (rc != 0)
		return -1;

	for (; ai != NULL; ai = ai->ai_next) {
		struct sockaddr *addr = ai->ai_addr;
		const socklen_t addr_len = ai->ai_addrlen;

		int server_socket = sio_socket(addr->sa_family, SOCK_STREAM, 0);
		if (server_socket < 0)
			continue;

		if (evio_setsockopt_server(server_socket, addr->sa_family,
					   SOCK_STREAM) != 0)
			goto cleanup_and_try_again;

		if (sio_bind(server_socket, addr, addr_len) != 0)
			goto cleanup_and_try_again;

		if (sio_listen(server_socket) != 0)
			goto cleanup_and_try_again;

		return server_socket;

		cleanup_and_try_again:
			close(server_socket);
	}

	return diag_set(IllegalParams,
			"Can't create a server at the specified address: "
			"%s:%s", host, service);
}

static int
client_worker(va_list args)
{
	int client_socket = va_arg(args, int);
	double idle_timeout = va_arg(args, double);

	coio_wait(client_socket, COIO_READ, idle_timeout);
	coio_close(client_socket);
	say_info("client[%d]: disconnected", client_socket);

	return 0;
}

static int
server_start_client_worker(int client_socket, double idle_timeout)
{
	say_info("client[%d]: connected", client_socket);

	struct fiber *client_fiber = fiber_new("client", client_worker);
	if (client_fiber == NULL)
		return -1;
	fiber_set_joinable(client_fiber, false);
	fiber_start(client_fiber, client_socket, idle_timeout);

	return 0;
}

static int
server_wouldblock(int err)
{
	return sio_wouldblock(err);
}

/**
 * Yield control to other fibers until there are no pending clients
 * in the backlog.
 * Return 1 if there are pending connections and 0 if waiting was interrupted.
 */
static int
server_wait_for_connection()
{
	int events = coio_wait(server.socket, COIO_READ,
			       SERVER_TIMEOUT_INFINITY);
	return (events & COIO_READ) != 0;
}

static int
server_accept_and_setopt()
{
	struct sockaddr_storage client_addr;
	socklen_t addr_len = sizeof(struct sockaddr_storage);
	struct sockaddr *client_addr_ptr = (struct sockaddr *)&client_addr;
	int client_socket = sio_accept(server.socket, client_addr_ptr,
				       &addr_len);

	if (client_socket < 0)
		return -1;

	if (evio_setsockopt_client(client_socket, client_addr.ss_family,
				   SOCK_STREAM) != 0) {
		close(client_socket);
		return -1;
	}

	return client_socket;
}

/**
 * Accept a pending client and run its worker.
 * If serving completed successfully 0 is returned, otherwise -1.
 */
static int
server_serve_connection(double idle_timeout)
{
	int client_socket = server_accept_and_setopt();
	if (client_socket >= 0) {
		if (server_start_client_worker(client_socket,
					       idle_timeout) == 0) {
			return 0;
		} else {
			close(client_socket);
			return -1;
		}
	}
	return -1;
}

#ifdef __linux__
/**
 * Check if the error is one of the network errors.
 */
static int
server_network_error(int err)
{
	return err == ENETDOWN   || err == EPROTO || err == ENOPROTOOPT  ||
	       err == EHOSTDOWN  || err == ENONET || err == EHOSTUNREACH ||
	       err == EOPNOTSUPP || err == ENETUNREACH;
}
#endif /* __linux__ */

/** Check whether the error should be treated as EAGAIN. */
static int
server_should_try_again(int err)
{
#ifdef __linux__
	/* Take a look at the Error handling section
	   in the accept's manual page. */
	return server_wouldblock(err) || server_network_error(err);
#else
	return server_wouldblock(err);
#endif
}

/** Server accept loop. */
static int
server_worker(va_list args)
{
	double idle_timeout = va_arg(args, double);

	say_info("server has been started");
	while (! fiber_is_cancelled()) {
		if (server_wait_for_connection()) {
			if (server_serve_connection(idle_timeout) != 0 &&
			    ! server_should_try_again(errno)) {
			    	say_info("server was stopped due to error: %s",
					 box_error_message(box_error_last()));
				return -1;
			}
		}
	}
	say_info("server was stopped");
	return 0;
}

static int
server_init(const char *host, const char *service)
{
	server.socket = server_socket_new(host, service);
	if (server.socket < 0)
		return -1;
	server.fiber = fiber_new("server", server_worker);
	if (server.fiber == NULL) {
		close(server.socket);
		return -1;
	}
	fiber_set_joinable(server.fiber, true);
	return 0;
}

static void
server_start_accept_loop(double idle_timeout)
{
	fiber_start(server.fiber, idle_timeout);
}

int
server_start(box_function_ctx_t *ctx,
	    const char *args, const char *args_end)
{
	(void)ctx;
	(void)args_end;
	const char *usage = "server_start(host = <str>, service = <str>, "
					  "idle_timeout_in_seconds = <double>)";
	uint32_t args_count = mp_decode_array(&args);
	if (args_count != 3)
		goto illegal_params;

	if (mp_typeof(*args) != MP_STR)
		goto illegal_params;
	uint32_t host_len = 0;
	const char *host = mp_decode_str(&args, &host_len);

	if (mp_typeof(*args) != MP_STR)
		goto illegal_params;
	uint32_t service_len = 0;
	const char *service = mp_decode_str(&args, &service_len);

	if (mp_typeof(*args) != MP_DOUBLE)
		goto illegal_params;
	const double idle_timeout = mp_decode_double(&args);
	if (idle_timeout < 0)
		goto illegal_params;

	/* make host and service null-terminated */
	host = xstrndup(host, host_len);
	service = xstrndup(service, service_len);
	int rc = server_init(host, service);
	free((void *)host);
	free((void *)service);
	if (rc != 0)
		return -1;

	server_start_accept_loop(idle_timeout);
	return 0;

illegal_params:
	return diag_set(IllegalParams, "Usage: %s", usage);
}

static int
server_stop_accept_loop()
{
	fiber_cancel(server.fiber);
	fiber_wakeup(server.fiber);
	return fiber_join(server.fiber);
}

static int
server_free()
{
	int server_socket = server.socket;
	memset(&server, 0, sizeof(server));
	return coio_close(server_socket);
}

int
server_stop(box_function_ctx_t *ctx,
	    const char *args, const char *args_end)
{
	(void)ctx;
	(void)args_end;
	const char *usage = "server_stop()";
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 0)
		goto illegal_params;

	server_stop_accept_loop();
	server_free();
	return 0;

illegal_params:
	return diag_set(IllegalParams, "Usage: %s", usage);
}
