local t = require('luatest')
local arbitrary_projection = t.group('arbitrary_projection')

local helper = require('test.helper.cluster_no_replication')

local fun = require("fun")

local cluster = nil

arbitrary_projection.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

arbitrary_projection.before_each(
    function()
        local api = cluster:server("api-1").net_box

        for i = 1, 10 do
            local r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i*2, i*3, -i, -i, -i, true, "123", i},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
    end
)

arbitrary_projection.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "arithmetic_space"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "arithmetic_space"]] })
    end
)

arbitrary_projection.after_all(function()
    helper.stop_test_cluster()
end)

arbitrary_projection.test_arbitrary_invalid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[select "id" + 1 as "alias" > a from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[
        select id" + 1 as "alias" > "a" is not null from "arithmetic_space"
    ]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[
        select "a" + "b" and true from "arithmetic_space"
    ]], {} })
    t.assert_str_contains(tostring(err), "Type mismatch: can not convert integer(3) to boolean")

    -- projection consisted of arithmetic, bool and cast (function)
    local _, err = api:call("sbroad.execute", { [[
        select cast("id" * 2 > 0 as boolean), cast("id" * 2 > 0 as boolean) as "cast" from "arithmetic_space"
    ]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    -- selection from values without cast
    local _, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "arithmetic_space" WHERE "id" IN (SELECT * FROM (VALUES (1)))
    ]], {} })
    t.assert_str_contains(tostring(err), "Sbroad Error: type any not implemented")
end

arbitrary_projection.test_arbitrary_valid = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- array of {true,true} with length equals to rows amount
    local all_true = fun.map(function()
        return { true, true }
    end, res_all.rows):totable()

    -- array of {false,false} with length equals to rows amount
    local all_false = fun.map(function()
        return { false, false }
    end, res_all.rows):totable()

    -- projection consisted of arithmetic and bool
    local r, err = api:call("sbroad.execute", { [[
        select "id", "id" - 5 > 0, "id" - 5 > 0 as "cmp" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        {
            {name = "id", type = "integer"},
            {name = "COL_1", type = "boolean"},
            {name = "cmp", type = "boolean"},
        }
    )
    for _, v in pairs(r.rows) do
        t.assert_equals(v[2], v[1] - 5 > 0)
        t.assert_equals(v[3], v[2])
    end

    local r, err = api:call("sbroad.execute", { [[
        select "id" + "b" > "id" + "b", "id" + "b" > "id" + "b" as "cmp" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.rows, all_false)

    local r, err = api:call("sbroad.execute", { [[
        select 0 = "id" + "f", 0 = "id" + "f" as "cmp" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.rows, all_true)

    local r, err = api:call("sbroad.execute", { [[
        select 1 > 0, 1 > 0 as "cmp" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        {
            { name = "COL_1", type = "boolean" },
            { name = "cmp", type = "boolean" },
        }
    )
    t.assert_equals(r.rows, all_true)

    local r, err = api:call("sbroad.execute", { [[
        select
            "id" between "id" - 1 and "id" * 4,
            "id" between "id" - 1 and "id" * 4 as "between"
        from
            "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        {
            { name = "COL_1", type = "boolean" },
            { name = "between", type = "boolean" },
        }
    )
    t.assert_equals(r.rows, all_true)

    -- projection consisted of arithmetic and unary
    local r, err = api:call("sbroad.execute", { [[
        select "id" is not null, ("id" + 2) is not null as "cmp" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.rows, all_true)

    -- column selection from values
    -- results in type erasing
    local r, err = api:call("sbroad.execute", { [[
        SELECT COLUMN_1 FROM (VALUES (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "COLUMN_1", type = "any"},
        },
        rows = {
            {1}
        },
    })

    -- column selection from values with cast
    r, err = api:call("sbroad.execute", { [[
        SELECT CAST(COLUMN_1 as int) FROM (VALUES (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "COL_1", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end
