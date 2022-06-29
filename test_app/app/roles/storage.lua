local log = require('log')

local function init(opts) -- luacheck: no unused args

    box.schema.func.create('sbroad.execute_query', {
        if_not_exists = true, language = 'C'
    })

    box.schema.func.create('sbroad.invalidate_segment_cache', {
        if_not_exists = true, language = 'C'
    })
    box.schema.func.create('sbroad.load_lua_storage_functions', {
        if_not_exists = true, language = 'C'
    })

    box.func["sbroad.load_lua_storage_functions"]:call({})

    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    box.func["sbroad.invalidate_segment_cache"]:call({})
    return true
end

return {
    role_name = 'app.roles.storage',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    dependencies = {
        "cartridge.roles.vshard-storage",
    },
}
