require('sbroad.core')

local cartridge = require('cartridge')
local checks = require('checks')

local function init ()
    box.schema.func.create(
        'libsbroad.calculate_bucket_id',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create("BUCKET_ID", {
        language = "Lua",
        body = [[
            function(x)
                return box.func["libsbroad.calculate_bucket_id"]:call({ x })
            end
        ]],
        if_not_exists = true,
        param_list = {"string"},
        returns = "unsigned",
        aggregate = "none",
        exports = {"SQL"},
    })

    box.schema.func.create(
        'libsbroad.init_statistics',
        { if_not_exists = true, language = 'C' }
    )
end

local function calculate_bucket_id(values, space_name) -- luacheck: no unused args
    checks('string|table', '?string')

    local has_err, result = pcall(function ()
       if type(values) == 'table' and space_name == nil then
           return false, error("space_name is required")
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
