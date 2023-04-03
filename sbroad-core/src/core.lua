local function init_bucket_id()
    box.schema.func.create(
        'libsbroad.calculate_bucket_id',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create('BUCKET_ID', {
        language = 'Lua',
        body = [[
            function(x)
                return box.func['libsbroad.calculate_bucket_id']:call({ x })
            end
        ]],
        if_not_exists = true,
        param_list = {'string'},
        returns = 'unsigned',
        aggregate = 'none',
        exports = {'SQL'},
    })
end

local function init_statistics()
    box.schema.func.create(
        'libsbroad.init_statistics',
        { if_not_exists = true, language = 'C' }
    )
end

local function init()
    init_bucket_id()
    init_statistics()
end

return {
    init = init,
}
