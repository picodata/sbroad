local vshard = require('vshard')
local cartridge = require('cartridge')

_G.query = nil
_G.calculate_bucket_id = nil
_G.calculate_bucket_id_by_dict = nil
_G.insert_record = nil
_G.sql_execute = nil


local function query(query, params)
    local has_err, parser_res = pcall(
        function()
            return box.func["sbroad.execute_query"]:call({ query, params })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return parser_res
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

local function insert_record(space_name, values)
    print('insert_record')
    local space = box.space[space_name]

    local placeholders = {}
    for v in pairs(values) do
        table.insert(placeholders, "?")
    end
    local placeholders_str = table.concat(placeholders, ",")
    local query = string.format("INSERT INTO \"%s\" VALUES (%s)", space_name, placeholders_str)
    local tuple = space:frommap(values)

    print(string.format("query %s", query))
    local has_err, res = pcall(
        function()
            return box.func["sbroad.execute_query"]:call({ query, tuple })
        end
    )

    if has_err == false then
        return res
    end

    return true
end

local function init(opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end
    _G.query = query
    _G.calculate_bucket_id = calculate_bucket_id
    _G.calculate_bucket_id_by_dict = calculate_bucket_id_by_dict
    _G.insert_record = insert_record
    _G.sql_execute = sql_execute


    box.schema.func.create('sbroad.parse_sql', { 
            if_not_exists = true, language = 'C' 
    })
    box.schema.func.create('sbroad.invalidate_cached_schema', {
            if_not_exists = true, language = 'C' 
    })
    box.schema.func.create('sbroad.calculate_bucket_id', { 
            if_not_exists = true, language = 'C' 
    })
    box.schema.func.create('sbroad.calculate_bucket_id_by_dict', { 
            if_not_exists = true, language = 'C' 
    })
    box.schema.func.create('sbroad.execute_query', { 
            if_not_exists = true, language = 'C' 
    })

    box.schema.func.create('sbroad.load_lua_extra_function', {
        if_not_exists = true, language = 'C'
    })

    box.func["sbroad.load_lua_extra_function"]:call({})

    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end
    box.func["sbroad.invalidate_cached_schema"]:call({})
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
