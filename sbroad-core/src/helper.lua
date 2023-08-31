-- Make table read-only
local function protect(tbl)
    return setmetatable({}, {
        __index = tbl,
        __newindex = function(_, key, value)
            error("attempting to change constant " ..
                   tostring(key) .. " to " .. tostring(value))
        end
    })
end

local constants = {
    -- Used for sending tracing to Jaeger agent.
    GLOBAL_TRACER = "global",
    -- Gathers stats about spans and saves them to temporary spaces
    -- on each node, but does it only for 1% of the queries.
    STAT_TRACER = "stat",
    -- Like STAT_TRACER but saves stats for each query.
    -- It is used only for tests.
    TEST_TRACER = "test_stat"
}
constants = protect(constants)

local function module_name()
    if package.loaded['pico'] ~= nil then
	return ''
    else
	return 'libsbroad'
    end
end

local function format_result(result)
    local formatted = {}
    if result['metadata'] ~= nil then
	formatted.metadata = setmetatable(result.metadata, { __serialize = nil })
    end
    if result['rows'] ~= nil then
	formatted.rows = setmetatable(result.rows, { __serialize = nil })
    end
    if result['metadata'] == nil and result['rows'] == nil then
	formatted = setmetatable(result, { __serialize = nil })
    end

    return formatted
end

local function vtable_limit_exceeded(limit, current_val)
    return string.format("Exceeded maximum number of rows (%d) in virtual table: %d", limit, current_val)
end

local function dql_error(err, rs_uuid)
    if type(err) ~= 'table' and type(err) ~= 'string' then
        io.stderr:write(string.format("expected string or table, got: %s", type(err)))
        error(err)
    end
    if type(err) == 'table' then
        local meta_t = getmetatable(err)
        meta_t.__tostring = function (self)
            return self.message
        end
        err.uuid = rs_uuid
        setmetatable(err, meta_t)
    end
    error(err)
end

return {
    module_name = module_name,
    vtable_limit_exceeded = vtable_limit_exceeded,
    dql_error = dql_error,
    format_result = format_result,
    constants = constants
}
