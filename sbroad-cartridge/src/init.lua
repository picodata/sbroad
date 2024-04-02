local checks = require('checks')
local core = require('sbroad.core')
local rust = require("sbroad.rust")

local function init (is_master)
    if is_master then
        core.init()
    end
    rust.init()
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

    if space_name then
        return rust.calculate_bucket_id(values, space_name)
    end

    return rust.calculate_bucket_id(values)
end


return {
    init = init,
    calculate_bucket_id = calculate_bucket_id,
}
