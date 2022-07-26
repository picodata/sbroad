local vshard = require('vshard')
local cartridge = require('cartridge')
local yaml = require("yaml")

_G.query = nil
_G.explain = nil
_G.calculate_bucket_id = nil
_G.calculate_bucket_id_by_dict = nil
_G.sql_execute = nil
_G.set_schema = nil

local function explain(query)
    local has_err, res = pcall(
        function()
            return box.func["sbroad.explain"]:call({ query })
        end
    )

    if has_err == false then
        return nil, res
    end

    local res_lines = {}
    for line in res:gmatch("[^\r\n]+") do
        table.insert(res_lines, line)
    end
    return res_lines
end

local function query(query, params)
    local has_err, parser_res = pcall(
        function()
            return box.func["sbroad.dispatch_query"]:call({ query, params })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return parser_res[1]
end

local function calculate_bucket_id(space_name, values)
    checks('string', 'table')
    local shard_key = cartridge.config_get_deepcopy().schema.spaces[space_name].sharding_key

    local shard_val = ''
    for _, key in ipairs(shard_key) do
        shard_val = shard_val .. tostring(values[key])
    end

    local bucket_id = box.func["sbroad.calculate_bucket_id"]:call({ shard_val })
    return bucket_id
end

local function calculate_bucket_id_by_dict(space_name, values) -- luacheck: no unused args
    checks('string', 'table')

    local has_err, calc_err = pcall(
        function()
            return box.func["sbroad.calculate_bucket_id_by_dict"]:call({ space_name, values })
        end
    )

    if has_err == false then
        return nil, calc_err
    end

    return calc_err
end

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

local function init(opts) -- luacheck: no unused args
    _G.query = query
    _G.explain = explain
    _G.calculate_bucket_id = calculate_bucket_id
    _G.calculate_bucket_id_by_dict = calculate_bucket_id_by_dict
    _G.sql_execute = query
    _G.set_schema = set_schema

    box.schema.func.create('sbroad.invalidate_coordinator_cache', {
            if_not_exists = true, language = 'C'
    })
    box.schema.func.create('sbroad.calculate_bucket_id', {
            if_not_exists = true, language = 'C'
    })
    box.schema.func.create('sbroad.calculate_bucket_id_by_dict', {
            if_not_exists = true, language = 'C'
    })
    box.schema.func.create('sbroad.dispatch_query', {
            if_not_exists = true, language = 'C'
    })

    box.schema.func.create('sbroad.load_lua_router_functions', {
        if_not_exists = true, language = 'C'
    })

    box.schema.func.create('sbroad.explain', {
            if_not_exists = true, language = 'C'
    })

    box.func["sbroad.load_lua_router_functions"]:call({})

    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    box.func["sbroad.invalidate_coordinator_cache"]:call({})
    return true
end

return {
    role_name = 'app.roles.api',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    dependencies = {'cartridge.roles.vshard-router'},
}
