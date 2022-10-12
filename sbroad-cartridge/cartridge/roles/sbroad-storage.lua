local sbroad_storage = require('sbroad.storage')

local function init(opts) -- luacheck: no unused args
    sbroad_storage.init()
    sbroad_storage.init_statistics()
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    sbroad_storage.invalidate_cache()
    return true
end

return {
    role_name = 'sbroad-storage',
    init = init,
    apply_config = apply_config,
    dependencies = {
        "cartridge.roles.vshard-storage",
    },
}
