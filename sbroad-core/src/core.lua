local helper = require('sbroad.helper')

local function init_bucket()
    local exec_fn = helper.module_name() .. ".calculate_bucket_id"
    box.schema.func.create(
        exec_fn,
        { if_not_exists = true, language = 'C' }
    )
end

local function init_statistics()
    local exec_fn = helper.module_name() .. ".init_statistics"
    box.schema.func.create(
        exec_fn,
        { if_not_exists = true, language = 'C' }
    )
end

local function init()
    init_bucket()
    init_statistics()
end

return {
    init = init,
}
