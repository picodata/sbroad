-- Intermediate layer of communication between Sbroad and vshard.
-- Function, described in this file are called from `vshard` Sbroad module.

local vshard = require('vshard')
local helper = require('sbroad.helper')

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

_G.dql_on_some = function(tbl_rs_ir, is_readonly, waiting_timeout, vtable_max_rows)
    local result = nil
    local futures = {}

    for rs_uuid, map in pairs(tbl_rs_ir) do
        local replica = vshard.router.routeall()[rs_uuid]
        local required = map["required"]
        local optional = map["optional"]
        local exec_fn = helper.module_name() .. ".execute"
        if is_readonly then
            local future, err = replica:callbre(
                exec_fn,
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(error)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                exec_fn,
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

        local data = res[1][1][1]
        if result == nil then
            result = data
        else
            local new_vt_rows = #data.rows + #result.rows
            if vtable_max_rows ~= 0 and vtable_max_rows < new_vt_rows then
                error(helper.vtable_limit_exceeded(vtable_max_rows, new_vt_rows))
            end
            for _, row in ipairs(data.rows) do
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
        local exec_fn = helper.module_name() .. ".execute"
        if is_readonly then
            local future, err = replica:callbre(
                exec_fn,
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(error)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                exec_fn,
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

_G.dql_on_all = function(required, optional, is_readonly, waiting_timeout, vtable_max_rows)
    local replicas = vshard.router.routeall()
    local result = nil
    local futures = {}
    local exec_fn = helper.module_name() .. ".execute"

    for _, replica in pairs(replicas) do
        if is_readonly then
            local future, err = replica:callbre(
                exec_fn,
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(err)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                exec_fn,
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

        local data = res[1][1][1]
        if result == nil then
            result = data
        else
            local new_vt_rows = #data.rows + #result.rows
            if vtable_max_rows ~= 0 and vtable_max_rows < new_vt_rows then
                error(helper.vtable_limit_exceeded(vtable_max_rows, new_vt_rows))
            end
            for _, row in ipairs(data.rows) do
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
    local exec_fn = helper.module_name() .. ".execute"

    for _, replica in pairs(replicas) do
        if is_readonly then
            local future, err = replica:callbre(
                exec_fn,
                { required, optional },
                { is_async = true }
            )
            if err ~= nil then
                error(err)
            end
            table.insert(futures, future)
        else
            local future, err = replica:callrw(
                exec_fn,
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
