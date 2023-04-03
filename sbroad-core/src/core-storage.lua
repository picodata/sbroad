_G.prepare = function(pattern)
    local prep, err = box.prepare(pattern)
    if err ~= nil then
        error(string.format("Failed to prepare statement: %s. Error: %s", pattern, err), 1)
    end
    return prep.stmt_id
end

_G.unprepare = function(stmt_id)
    box.unprepare(stmt_id)
end

_G.read = function(stmt_id, stmt, params)
    local res, err = box.execute(stmt_id, params)
    if err ~= nil then
        -- We don't have SQL query for retrying,
        -- so simply return an error
        if stmt == nil or stmt == '' then
            error(err)
        end
        -- The statement can be evicted from the cache,
        -- while we were yielding in Lua. So we execute
        -- it without the cache.
        res, err = box.execute(stmt, params)
        if err ~= nil then
            error(err)
        end
    end

    local result = {}
    result.metadata = res.metadata
    result.rows = {}
    for _, row in ipairs(res.rows) do
        local tuple = {}
        for _, field in ipairs(row) do
            table.insert(tuple, field)
        end
        table.insert(result.rows, tuple)
    end

    return box.tuple.new{result}
end

_G.write = function(stmt_id, stmt, params)
    local res, err = box.execute(stmt_id, params)
    if err ~= nil then
        -- We don't have SQL query for retrying,
        -- so simply return an error
        if stmt == nil or stmt == '' then
            error(err)
        end
        -- The statement can be evicted from the cache,
        -- while we were yielding in Lua. So we execute
        -- it without the cache.
        res, err = box.execute(stmt, params)
        if err ~= nil then
            error(err)
        end
    end

    return box.tuple.new{res}
end
