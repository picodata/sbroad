local cartridge = require('cartridge')
local checks = require('checks')
local core = require('sbroad.core')

local function init (is_master)
    if is_master then
        core.init()
    end
end

local function calculate_bucket_id(values, space_name) -- luacheck: no unused args
    checks('string|table', '?string')

    local has_err, result = pcall(function ()
       if type(values) == 'table' and space_name == nil then
           return false, error("space name is required")
       end

       return true
    end)

    if has_err == false then
        return nil, result
    end

    has_err, result = pcall(
        function()
            return box.func["libsbroad.calculate_bucket_id"]:call({ values,  space_name })
        end
    )

    if has_err == false then
        return nil, result
    end

    return result
end

local function init_statistics ()
    box.func["libsbroad.init_statistics"]:call({})
end

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

return {
    init = init,
    init_statistics = init_statistics,
    calculate_bucket_id = calculate_bucket_id,
}
