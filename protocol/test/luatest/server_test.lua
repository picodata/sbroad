local t = require('luatest')
local g = t.group()

local net_box = require('net.box')

-- specify where to find tcpserver lib
local cpath = require('cpath')
cpath.append('build/src/server')

local function create_server_funcitons()
    box.schema.func.create('tcpserver.server_start', {language = 'C'})
    box.schema.user.grant('guest', 'execute', 'function', 'tcpserver.server_start')

    box.schema.func.create('tcpserver.server_stop', {language = 'C'})
    box.schema.user.grant('guest', 'execute', 'function', 'tcpserver.server_stop')
end

local function drop_server_funtions()
    box.schema.func.drop('tcpserver.server_start')
    box.schema.func.drop('tcpserver.server_stop')
end

g.before_all(function()
    box.cfg{listen=3306}
    create_server_funcitons()
    box.schema.space.create('test')
    box.space.test:create_index('primary')
end)

g.after_all(function()
    drop_server_funtions()
    box.space.test:drop()
end)

local function would_block(e)
    local errno = require('errno')
    return e == errno.EWOULDBLOCK or e == errno.EAGAIN
end

local function test_connection(host, service)
    local caller = net_box:new(3306)
    local socket = require('socket')
    local idle_timeout = 0.05

    caller:call('tcpserver.server_start', {host, service, idle_timeout})

    local port = tonumber(service)
    local client1, e1 = socket.tcp_connect(host, port)
    local client2, e2 = socket.tcp_connect(host, port)

    t.assert_not_equals(client1, nil, e1)
    t.assert_not_equals(client2, nil, e2)

    client1:nonblock(true)
    client2:nonblock(true)

    client1:sysread(1)
    client2:sysread(1)
    t.assert(would_block(client1:errno()), 'connection must be still oppend')
    t.assert(would_block(client2:errno()), 'connection must be still oppend')

    require('fiber').sleep(idle_timeout * 2)

    client1:sysread(1)
    client2:sysread(1)
    t.assert_not(would_block(client1:errno()), 'connection must be already closed')
    t.assert_not(would_block(client2:errno()), 'connection must be already closed')

    client1:close()
    client2:close()

    caller:call('tcpserver.server_stop')
end

g.test_illigal_params = function()
    local caller = net_box:new(3306)
    t.assert_error_msg_contains("Usage: server_start",
        function()
            caller:call('tcpserver.server_start', {})
        end
    )
    t.assert_error_msg_contains("Usage: server_stop",
        function()
            caller:call('tcpserver.server_stop', {"extra"})
        end
    )
    t.assert_error_msg_contains("Usage: server_start",
        function()
            caller:call('tcpserver.server_start', {1, 1, 1})
        end
    )
    t.assert_error_msg_contains("Usage: server_start",
        function()
            caller:call('tcpserver.server_start', {'host', 'service', 'time'})
        end
    )
    t.assert_error_msg_contains("Usage: server_start",
        function()
            caller:call('tcpserver.server_start', {'host', 'service', -0.1})
        end
    )
    t.assert_error_msg_contains("Usage: server_start",
        function()
            caller:call('tcpserver.server_start', {'localhost', '11111', -0.1})
        end
    )
    t.assert_error_msg_contains("Usage: server_start",
        function()
            caller:call('tcpserver.server_start', {'localhost', '11111', 0.1, 'extra'})
        end
    )
    t.assert_error_msg_contains("getaddrinfo",
        function()
            caller:call('tcpserver.server_start', {'badhost', '11111', 0.1})
        end
    )
    t.assert_error_msg_contains("getaddrinfo",
        function()
            caller:call('tcpserver.server_start', {'localhost', 'badservice', 0.1})
        end
    )
    t.assert_error_msg_contains("Can't create a server at the specified address",
        function()
            caller:call('tcpserver.server_start', {'8.8.8.8', '11111', 0.1})
        end
    )
end

g.test_connection_IPv4 = function()
    test_connection('127.0.0.1', '43139')
end

g.test_connection_IPv6 = function()
    test_connection('::1', '43151')
end

g.test_connection_localhost = function()
    test_connection('localhost', '42124')
end
