require('strict').on()
require('sbroad')

local vshard = require('vshard')
local cartridge = require('cartridge')

_G.get_schema = function()
    return cartridge.get_schema()
end

_G.get_waiting_timeout = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["executor_waiting_timeout"] == nil then
        return 0
    end

    return cfg["executor_waiting_timeout"]
end

_G.get_router_cache_capacity = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["router_cache_capacity"] == nil then
        return 50
    end

    return cfg["router_cache_capacity"]
end

_G.get_sharding_column = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["executor_sharding_column"] == nil then
        return "bucket_id"
    end

    return cfg["executor_sharding_column"]
end

_G.group_buckets_by_replicasets = function(buckets)
    local map = {}
    for _, bucket_id in pairs(buckets) do
        local rs = vshard.router.route(bucket_id).uuid
        if map[rs] then
            table.insert(map[rs], bucket_id)
        else
            map[rs] = {bucket_id}
        end
    end

    return map
end

_G.dql_on_some = function(tbl_rs_ir, is_readonly, waiting_timeout)
    local result = nil
    local futures = {}

    for rs_uuid, map in pairs(tbl_rs_ir) do
        local replica = vshard.router.routeall()[rs_uuid]
        local required = map["required"]
        local optional = map["optional"]
        if is_readonly then
            local future, err = replica:callbre(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(error)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(error)
            end
            table.insert(futures, future)
        end
    end

    for _, future in ipairs(futures) do
        future:wait_result(waiting_timeout)
        local res, err = future:result()

        if err ~= nil then
            error(err)
        end

        if result == nil then
            result = res[1][1][1]
        else
            for _, row in ipairs(res[1][1][1].rows) do
                table.insert(result.rows, row)
            end
        end
    end

    return box.tuple.new{result}
end

_G.dml_on_some = function(tbl_rs_ir, is_readonly, waiting_timeout)
    local result = nil
    local futures = {}

    for rs_uuid, map in pairs(tbl_rs_ir) do
        local replica = vshard.router.routeall()[rs_uuid]
        local required = map["required"]
        local optional = map["optional"]
        if is_readonly then
            local future, err = replica:callbre(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(error)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(error)
            end
            table.insert(futures, future)
        end
    end

    for _, future in ipairs(futures) do
        future:wait_result(waiting_timeout)
        local res, err = future:result()

        if err ~= nil then
            error(err)
        end

        if result == nil then
            result = res[1][1][1]
        else
            result.row_count = result.row_count + res[1][1][1].row_count
        end
    end

    return box.tuple.new{result}
end

_G.dql_on_all = function(required, optional, is_readonly, waiting_timeout)
    local replicas = vshard.router.routeall()
    local result = nil
    local futures = {}

    for _, replica in pairs(replicas) do
        if is_readonly then
            local future, err = replica:callbre(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(err)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(err)
            end
            table.insert(futures, future)
        end
    end

    for _, future in ipairs(futures) do
        future:wait_result(waiting_timeout)
        local res, err = future:result()

        if res == nil then
            error(err)
        end

        if result == nil then
            result = res[1][1][1]
        else
            for _, row in ipairs(res[1][1][1].rows) do
                table.insert(result.rows, row)
            end
        end
    end

    return box.tuple.new{result}
end

_G.dml_on_all = function(required, optional, is_readonly, waiting_timeout)
    local replicas = vshard.router.routeall()
    local result = nil
    local futures = {}

    for _, replica in pairs(replicas) do
        if is_readonly then
            local future, err = replica:callbre(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(err)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                "libsbroad.execute",
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(err)
            end
            table.insert(futures, future)
        end
    end

    for _, future in ipairs(futures) do
        future:wait_result(waiting_timeout)
        local res, err = future:result()

        if err ~= nil then
            error(err)
        end

        if result == nil then
            result = res[1][1][1]
        else
            result.row_count = result.row_count + res[1][1][1].row_count
        end
    end

    return box.tuple.new{result}
end

local function init()
    box.schema.func.create(
        'libsbroad.invalidate_coordinator_cache',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create(
        'libsbroad.dispatch_query',
        { if_not_exists = true, language = 'C' }
    )
end

local function invalidate_cache ()
    box.func["libsbroad.invalidate_coordinator_cache"]:call({})
end

local function trace(query, params, context, id)
    local has_err, parser_res = pcall(
        function()
            return box.func["libsbroad.dispatch_query"]:call({ query, params, context, id, true })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return parser_res[1]
end

local function execute(query, params)
    local has_err, parser_res = pcall(
        function()
            return box.func["libsbroad.dispatch_query"]:call({ query, params, box.NULL, box.NULL, false })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return parser_res[1]
end

return {
    init=init,
    invalidate_cache = invalidate_cache,
    execute = execute,
    trace = trace,
}
