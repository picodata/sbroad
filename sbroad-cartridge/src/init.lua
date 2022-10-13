require 'sbroad.core'
local cartridge = require('cartridge')

_G.get_jaeger_agent_host = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["jaeger_agent_host"] == nil then
        return "localhost"
    end

    return cfg["jaeger_agent_host"]
end

_G.get_jaeger_agent_port = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["jaeger_agent_port"] == nil then
        return 6831
    end

    return cfg["jaeger_agent_port"]
end
