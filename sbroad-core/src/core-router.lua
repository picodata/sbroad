-- Intermediate layer of communication between Sbroad and vshard.
-- Function, described in this file are called from `vshard` Sbroad module.

local vshard = require('vshard')
local helper = require('sbroad.helper')
local fiber = require('fiber')
local table = require('table')
local ref_id = 0
local DQL_MIN_TIMEOUT = 10
local REF_MIN_TIMEOUT = 5

local function future_wait(cond, timeout)
    local f = function(cond, timeout)
        if timeout and timeout < 0 then
            error("dql timeout exceeded")
        end
        local res, err = cond:wait_result(timeout)
        if err then
            error(err)
        end
        return res
    end
    local ok, res = pcall(f, cond, timeout)
    if ok then
        return res
    end
    return nil, res
end

--
-- Helper function to execute read request on multiple storages,
-- without buckets being moved between the storages by vhard
-- rebalancer. This function is a modified version of `map_callrw`
-- from vshard router api (see its doc for more details).
--
-- To ensure data does not move between storages during execution,
-- there are two stages: ref and map.
-- 1. Ref stage creates a reference with deadline on each specified
-- replicaset's master. While this reference is alive the master
-- will not receive, nor send data.
-- 2. Map stage - after references were created on each master, request to execute
-- the given function is sent to each master. After function was executed the
-- reference is deleted. If the reference expires, the error will be returned
-- to router.
--
-- @param uuid_to_args Mapping between replicaset uuid and function arguments
-- for that replicaset.
-- @param func Name of the function to call.
-- @param vtable_max_rows Maximum number of rows to receive from storages.
-- If the number is exceeded, the results are discarded and error is returned.
-- @param opts Table which may have the following options:
--  1. ref_timeout - number of seconds to keep the reference alive. Shouldn't
--  be too big, because alive reference blocks vshard's rebalancing.
--  2. timeout - timeout for the whole function execution.
--  3. check_bucket_count - whether or not to check that ref stage has covered
--  all buckets. If this is true, the error will be returned if router's bucket
--  count != bucket count covered by ref stage. It may mean that router has
--  outdated configuration (some buckets were added/deleted on storages that router
--  does not know about). This option should be used only if you intend to execute
--  the function on all replicasets and want to ensure that all buckets were covered.
--
local function multi_storage_dql(uuid_to_args, func, vtable_max_rows, opts)
    local replicasets = vshard.router.routeall()
    local timeout, ref_timeout, check_bucket_count
    if opts then
        timeout = opts.map_timeout or DQL_MIN_TIMEOUT
        ref_timeout = opts.ref_timeout or REF_MIN_TIMEOUT
        check_bucket_count = opts.check_bucket_count or false
    else
        timeout = DQL_MIN_TIMEOUT
        ref_timeout = REF_MIN_TIMEOUT
        check_bucket_count = false
    end

    local err, err_uuid, res, ok
    local futures = {}
    local bucket_count = 0
    local opts_ref = {is_async = true}
    local opts_map = {is_async = true}
    local rs_count = 0
    local rid = ref_id
    local deadline = fiber.clock() + timeout
    local result = nil
    ref_id = rid + 1
    -- Nil checks are done explicitly here (== nil instead of 'not'), because
    -- netbox requests return box.NULL instead of nils.

    --
    -- Ref stage: send.
    --
    for uuid, _ in pairs(uuid_to_args) do
        local rs = replicasets[uuid]
        res, err = rs:callrw('vshard.storage._call',
                {'storage_ref', rid, ref_timeout}, opts_ref)
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        futures[uuid] = res
        rs_count = rs_count + 1
    end
    --
    -- Ref stage: collect.
    --
    for uuid, future in pairs(futures) do
        res, err = future_wait(future, timeout)
        -- Handle netbox error first.
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        -- Ref returns nil,err or bucket count.
        res, err = res[1], res[2]
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        bucket_count = bucket_count + res
        timeout = deadline - fiber.clock()
    end

    -- All refs are done but not all buckets are covered. This is odd and can
    -- mean many things. The most possible ones: 1) outdated configuration on
    -- the router and it does not see another replicaset with more buckets,
    -- 2) some buckets are simply lost or duplicated - could happen as a bug, or
    -- if the user does a maintenance of some kind by creating/deleting buckets.
    -- In both cases can't guarantee all the data would be covered by Map calls.
    if check_bucket_count and bucket_count ~= vshard.router.bucket_count() then
       err = "All refs are done but not all buckets are covered"
       goto fail
    end

    -- Map stage: send.
    --
    for uuid, rs_args in pairs(uuid_to_args) do
        local rs = replicasets[uuid]
        local args = {'storage_map', rid, helper.proc_call_fn_name(), {func, rs_args['required'], rs_args['optional']}}
        res, err = rs:callrw('vshard.storage._call', args, opts_map)
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        futures[uuid] = res
    end
    --
    -- Map stage: collect.
    --
    for uuid, f in pairs(futures) do
        res, err = future_wait(f, timeout)
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        -- Map returns true,res or nil,err.
        ok, res = res[1], res[2]
        if ok == nil then
            err = res
            err_uuid = uuid
            goto fail
        end
        if res ~= nil then
            local data = res[1]
            if result == nil then
                result = data
            else
                local new_vt_rows = #data.rows + #result.rows
                if vtable_max_rows ~= 0 and vtable_max_rows < new_vt_rows then
                    err = helper.vtable_limit_exceeded(vtable_max_rows, new_vt_rows)
                    err_uuid = uuid
                    goto fail
                end
                for _, row in ipairs(data.rows) do
                    table.insert(result.rows, row)
                end
            end
        end
        timeout = deadline - fiber.clock()
    end
    do return result end

    ::fail::
    for uuid, f in pairs(futures) do
        f:discard()
        -- Best effort to remove the created refs before exiting. Can help if
        -- the timeout was big and the error happened early.
        f = replicasets[uuid]:callrw('vshard.storage._call',
                {'storage_unref', rid}, opts_ref)
        if f ~= nil then
            -- Don't care waiting for a result - no time for this. But it won't
            -- affect the request sending if the connection is still alive.
            f:discard()
        end
    end
    return nil, err, err_uuid
end

_G.group_buckets_by_replicasets = function(buckets)
    local map = {}
    for _, bucket_id in pairs(buckets) do
        local rs, err = vshard.router.route(bucket_id)
        if err ~= nil then
            return nil, err
        end
        local uuid = rs.uuid
        if map[uuid] then
            table.insert(map[uuid], bucket_id)
        else
            map[uuid] = {bucket_id}
        end
    end

    return map
end

_G.dql_on_some = function(uuid_to_args, is_readonly, waiting_timeout, vtable_max_rows)
    local result
    local call_opts = { is_async = true }

    local exec_fn = helper.proc_fn_name("execute")
    if #uuid_to_args == 1 then
        -- When read request is executed only on one
        -- storage, we don't care about bucket rebalancing.
        local rs_uuid, rs_args = pairs(uuid_to_args).next()
        local replica = vshard.router.routeall()[rs_uuid]
        local future, err
        local args = { rs_args['required'], rs_args['optional'] }
        if is_readonly then
            future, err = replica:callbre(
                    exec_fn,
                    args,
                    call_opts
            )
        else
            future, err = replica:callrw(
                    exec_fn,
                    args,
                    call_opts
            )
        end
        if err ~= nil then
            error(err)
        end
        future:wait_result(waiting_timeout)
        -- vtable_max_rows limit was checked on
        -- storage. No need to check it here.
        local res, err = future:result()
        if err ~= nil then
            error(err)
        end
        -- TODO: explain where this `[1][1]` comes from
        result = helper.unwrap_execute_result(res[1][1])
    else
        local err, err_uuid
        local opts = { map_timeout = waiting_timeout, ref_timeout = waiting_timeout }
        result, err, err_uuid = multi_storage_dql(uuid_to_args, exec_fn, vtable_max_rows, opts)
        if err ~= nil then
            helper.dql_error(err, err_uuid)
        end
    end

    return box.tuple.new{result}
end

_G.dql_on_all = function(required, optional, waiting_timeout, vtable_max_rows)
    local replicasets = vshard.router.routeall()
    local exec_fn = helper.proc_fn_name("execute")
    local uuid_to_args = {}
    for uuid, _ in pairs(replicasets) do
        uuid_to_args[uuid] = { required = required, optional = optional }
    end
    local opts = {
        map_timeout = waiting_timeout,
        ref_timeout = waiting_timeout,
        check_bucket_count = true
    }
    local result, err, err_uuid = multi_storage_dql(uuid_to_args, exec_fn, vtable_max_rows, opts)
    if err ~= nil then
        helper.dql_error(err, err_uuid)
    end

    return box.tuple.new{result}
end

_G.dml_on_some = function(tbl_rs_ir, is_readonly, waiting_timeout)
    local result = nil
    local exec_fn = helper.proc_fn_name("execute")
    local futures = {}

    for rs_uuid, map in pairs(tbl_rs_ir) do
        local replica = vshard.router.routeall()[rs_uuid]
        local required = map["required"]
        local optional = map["optional"]
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

        -- TODO: explain where this `[1][1]` comes from
        local next_result = helper.unwrap_execute_result(res[1][1])
        if result == nil then
            result = next_result
        else
            result.row_count = result.row_count + next_result.row_count
        end
    end

    return box.tuple.new{result}
end


_G.dml_on_all = function(required, optional, is_readonly, waiting_timeout)
    local replicas = vshard.router.routeall()
    local result = nil
    local futures = {}
    local exec_fn = helper.proc_fn_name("execute")

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

        -- TODO: explain where this `[1][1]` comes from
        local next_result = helper.unwrap_execute_result(res[1][1])
        if result == nil then
            result = next_result
        else
            result.row_count = result.row_count + next_result.row_count
        end
    end

    return box.tuple.new{result}
end
