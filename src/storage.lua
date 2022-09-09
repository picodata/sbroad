require('strict').on()
local cartridge = require('cartridge')

_G.get_storage_cache_capacity = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["storage_cache_capacity"] == nil then
        return 200
    end

    return cfg["storage_cache_capacity"]
end


_G.get_storage_cache_size_bytes = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["storage_cache_size_bytes"] == nil then
        return 204800
    end

    return cfg["storage_cache_size_bytes"]
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

_G.prepare = function(pattern)
    local prep, err = box.prepare(pattern)
    if err ~= nil then
        error("Failed to prepare statement: %s. Error: %s", pattern, err)
    end
    return prep.stmt_id
end

_G.unprepare = function(stmt_id)
    box.unprepare(stmt_id)
end

_G.read = function(stmt_id, stmt, params)
    local res, err = box.execute(stmt_id, params)
    if err ~= nil then
    -- The statement can be evicted from the cache,
    -- while we were yielding in Lua. So we execute
    -- it without the cache.
        res, err = box.execute(stmt, params)
        if err ~= nil then
            error(err)
        end
    end

    local result = {}
    result.metadata = res.metadata
    result.rows = {}
    for _, row in ipairs(res.rows) do
        local tuple = {}
        for _, field in ipairs(row) do
            table.insert(tuple, field)
        end
        table.insert(result.rows, tuple)
    end

    return box.tuple.new{result}
end

_G.write = function(stmt_id, stmt, params)
    local res, err = box.execute(stmt_id, params)
    if err ~= nil then
        -- The statement can be evicted from the cache,
        -- while we were yielding in Lua. So we execute
        -- it without the cache.
        res, err = box.execute(stmt, params)
        if err ~= nil then
            error(err)
        end
    end

    return box.tuple.new{res}
end

local function init()
    box.schema.func.create(
        'libsbroad.execute_query',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create(
        'libsbroad.invalidate_segment_cache',
        { if_not_exists = true, language = 'C' }
    )
end

local function invalidate_cache()
    box.func["libsbroad.invalidate_segment_cache"]:call({})
end

return {
    init = init,
    invalidate_cache = invalidate_cache
}
