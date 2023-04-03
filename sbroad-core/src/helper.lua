local function module_name()
    if package.loaded['pico'] ~= nil then
	return ''
    else
	return 'libsbroad'
    end
end

return {
    module_name = module_name,
}
