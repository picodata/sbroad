local log = require('log')

_G.insert_map = nil

local function insert_map(space_name, values)
    local space = box.space[space_name]
    local tuple = space:frommap(values)
    space:put(tuple)

    return true
end

local function init(opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end
    _G.insert_map = insert_map
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
