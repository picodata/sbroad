local vshard = require('vshard')
local cartridge = require('cartridge')
local yaml = require('yaml')

_G.query = nil
_G.insert_record = nil
_G.sql_execute = nil

local function query(q)
    local has_err, parser_res = pcall(
            function()
                return box.func["sbroad.parse_sql"]:call({ q, vshard.router.bucket_count() })
            end
    )

    if has_err == false then
        return nil, parser_res
    end

    local result = nil
    for _, rec in pairs(parser_res) do
        local res = yaml.decode(box.func["sbroad.execute_query"]:call({ rec[1], rec[2] }))

        if result == nil then
            result = res
        else
            for _, item in ipairs(res.rows) do
                table.insert(result.rows, item)
            end
        end
    end

    return result
end

local function insert_record(space_name, values)
    local shard_key = cartridge.config_get_deepcopy().schema.spaces[space_name].sharding_key

    local shard_val = ''
    for _, key in ipairs(shard_key) do
        shard_val = shard_val .. tostring(values[key])
    end

    values['bucket_id'] = box.func["sbroad.calculate_bucket_id"]:call({ shard_val, vshard.router.bucket_count() })
	local res = vshard.router.call(
        values['bucket_id'],
         "write",
        "insert_map",
        { space_name, values }
    )
    return res
end

local function init(opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end
    _G.query = query
    _G.insert_record = insert_record
    _G.sql_execute = sql_execute

    box.schema.func.create('sbroad.parse_sql', { if_not_exists = true, language = 'C' })
    box.schema.func.create('sbroad.invalidate_caching_schema', { if_not_exists = true, language = 'C' })
    box.schema.func.create('sbroad.calculate_bucket_id', { if_not_exists = true, language = 'C' })
    box.schema.func.create('sbroad.execute_query', { if_not_exists = true, language = 'C' })

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
    box.func["sbroad.invalidate_caching_schema"]:call({})
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
