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

return {
    module_name = module_name,
    format_result = format_result,
    constants = constants
}
