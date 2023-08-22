local sbroad_common = require('sbroad.init')
local sbroad_storage = require('sbroad.storage')

local function init(opts) -- luacheck: no unused args
    if rawget(_G, 'sbroad') == nil then
        rawset(_G, 'sbroad', {})
    end

    _G.sbroad.calculate_bucket_id = sbroad_common.calculate_bucket_id

    sbroad_common.init(opts.is_master)
    sbroad_common.init_statistics()
    sbroad_storage.init(opts.is_master)

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
        'cartridge.roles.vshard-storage',
        'cartridge.roles.vshard-router',
    },
}
