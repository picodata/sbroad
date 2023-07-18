require('strict').on()
require('sbroad')
require('sbroad.core-router')
local helper = require('sbroad.helper')

local cartridge = require('cartridge')

_G.get_schema = function()
    return cartridge.get_schema()
end

_G.get_waiting_timeout = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["executor_waiting_timeout"] == nil then
        return 0
    end

    return cfg["executor_waiting_timeout"]
end

_G.get_router_cache_capacity = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["router_cache_capacity"] == nil then
        return 50
    end

    return cfg["router_cache_capacity"]
end

_G.get_sharding_column = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["executor_sharding_column"] == nil then
        return "bucket_id"
    end

    return cfg["executor_sharding_column"]
end

local function init()
    box.schema.func.create(
        'libsbroad.invalidate_coordinator_cache',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create(
        'libsbroad.dispatch_query',
        { if_not_exists = true, language = 'C' }
    )
end

local function invalidate_cache ()
    box.func["libsbroad.invalidate_coordinator_cache"]:call({})
end

local function trace(query, params, context, id)
    local has_err, parser_res = pcall(
        function()
            return box.func["libsbroad.dispatch_query"]:call({ query, params, context, id, true })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return helper.format_result(parser_res[1])
end

local function execute(query, params)
    local has_err, parser_res = pcall(
        function()
            return box.func["libsbroad.dispatch_query"]:call({ query, params, box.NULL, box.NULL, false })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return helper.format_result(parser_res[1])
end

return {
    init=init,
    invalidate_cache = invalidate_cache,
    execute = execute,
    trace = trace,
}
