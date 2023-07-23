local cartridge = require('cartridge')
local yaml = require("yaml")
local checks = require('checks')
local helper = require('sbroad.helper')

_G.set_schema = nil

local function set_schema(new_schema)
    checks('table|string')

    local schema_str = new_schema
    if type(new_schema) == 'table' then
        schema_str = yaml.encode(new_schema)
    end
    local _, err = cartridge.set_schema(schema_str)
    if err ~= nil then
        return err
    end

    return nil
end

local function const_trace(query, params, context, id)
    local has_err, parser_res = pcall(
            function()
                return box.func["libsbroad.dispatch_query"]:call({
                    query, params, context, id,
                    helper.constants.TEST_TRACER })
            end
    )

    if has_err == false then
        return nil, parser_res
    end

    return helper.format_result(parser_res[1])
end

local function init(opts) -- luacheck: no unused args
    _G.set_schema = set_schema
    _G.sbroad.const_trace = const_trace

    return true
end

return {
    role_name = 'app.roles.api',
    init = init,
    dependencies = {
        'cartridge.roles.sbroad-router',
        'cartridge.roles.crud-router',
    },
}
