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
}
