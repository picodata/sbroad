local sbroad_router = require('sbroad.router')

local function init(opts) -- luacheck: no unused args

    if rawget(_G, 'sbroad') == nil then
        rawset(_G, 'sbroad', {})
    end

    _G.sbroad.calculate_bucket_id = sbroad_router.calculate_bucket_id
    _G.sbroad.explain = sbroad_router.explain
    _G.sbroad.execute = sbroad_router.execute
    _G.sbroad.trace = sbroad_router.trace

    sbroad_router.init()

    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    sbroad_router.invalidate_cache()
    return true
end

return {
    role_name = 'sbroad-router',
    init = init,
    apply_config = apply_config,
    dependencies = {'cartridge.roles.vshard-router'},
}
