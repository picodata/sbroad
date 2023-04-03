require('strict').on()
require('sbroad')
require('sbroad.core-storage')

local cartridge = require('cartridge')

_G.get_storage_cache_capacity = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["storage_cache_capacity"] == nil then
        return 200
    end

    return cfg["storage_cache_capacity"]
end

_G.get_storage_cache_size_bytes = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["storage_cache_size_bytes"] == nil then
        return 204800
    end

    return cfg["storage_cache_size_bytes"]
end

local function init()
    box.schema.func.create(
        'libsbroad.execute',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create(
        'libsbroad.invalidate_segment_cache',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create(
        'libsbroad.rpc_mock',
        { if_not_exists = true, language = 'C' }
    )
end

local function invalidate_cache()
    box.func["libsbroad.invalidate_segment_cache"]:call({})
end

return {
    init = init,
    invalidate_cache = invalidate_cache,
}
