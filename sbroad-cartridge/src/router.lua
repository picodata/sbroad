require('strict').on()
require('sbroad')

local vshard = require('vshard')
local cartridge = require('cartridge')
local checks = require('checks')

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

_G.read_on_some = function(tbl_rs_query, waiting_timeout)
    local result = nil
    local futures = {}

    for rs_uuid, query in pairs(tbl_rs_query) do
        local replica = vshard.router.routeall()[rs_uuid]
        local future, err = replica:callbre(
            "libsbroad.execute_query",
            { query['pattern'], query['params'], query['context'], query['id'], false, query['force_trace'] },
            { is_async = true }
        )
        if err ~= nil then
            error(error)
        end
        table.insert(futures, future)
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

_G.write_on_some = function(tbl_rs_query, waiting_timeout)
    local result = nil
    local futures = {}

    for rs_uuid, query in pairs(tbl_rs_query) do
        local replica = vshard.router.routeall()[rs_uuid]
        local future, err = replica:callrw(
            "libsbroad.execute_query",
            { query['pattern'], query['params'], query['context'], query['id'], true, query['force_trace'] },
            { is_async = true }
        )
        if err ~= nil then
            error(error)
        end
        table.insert(futures, future)
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

_G.read_on_all = function(query, waiting_timeout)
    local replicas = vshard.router.routeall()
    local result = nil
    local futures = {}

    for _, replica in pairs(replicas) do
        local future, err = replica:callbre(
            "libsbroad.execute_query",
            { query['pattern'], query['params'], query['context'], query['id'], false, query['force_trace'] },
            { is_async = true }
        )
        if err ~= nil then
            error(err)
        end
        table.insert(futures, future)
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

_G.write_on_all = function(query, waiting_timeout)
    local replicas = vshard.router.routeall()
    local result = nil
    local futures = {}

    for _, replica in pairs(replicas) do
        local future, err = replica:callrw(
            "libsbroad.execute_query",
            { query['pattern'], query['params'], query['context'], query['id'], true, query['force_trace'] },
            { is_async = true }
        )
        if err ~= nil then
            error(error)
        end
        table.insert(futures, future)
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
        'libsbroad.calculate_bucket_id',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create(
        'libsbroad.calculate_bucket_id_by_dict',
        { if_not_exists = true, language = 'C' }
    )

    box.schema.func.create(
        'libsbroad.dispatch_query',
        { if_not_exists = true, language = 'C' }
    )
end

local function calculate_bucket_id(values, space_name) -- luacheck: no unused args
    checks('string|table', '?string')

    local has_err, result = pcall(function ()
       if type(values) == 'table' and space_name == nil then
           return false, error("space_name is required")
       end

       return true
    end)

    if has_err == false then
        return nil, result
    end

    has_err, result = pcall(
        function()
            return box.func["libsbroad.calculate_bucket_id"]:call({ values,  space_name })
        end
    )

    if has_err == false then
        return nil, result
    end

    return result
end

local function invalidate_cache ()
    box.func["libsbroad.invalidate_coordinator_cache"]:call({})
end

local function trace(query, params, context, id)
    local has_err, parser_res = pcall(
        function()
            return box.func["libsbroad.dispatch_query"]:call({ query, params, context, id, false })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return parser_res[1]
end

local function execute(query, params)
    return trace(query, params, box.NULL, box.NULL)
end

return {
    init=init,
    invalidate_cache = invalidate_cache,
    execute = execute,
    trace = trace,
    calculate_bucket_id = calculate_bucket_id
}
