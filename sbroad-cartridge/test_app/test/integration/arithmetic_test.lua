local t = require('luatest')
local g = t.group('arithmetic')
local g1 = t.group('arithmetic.propetries')
local decimal = require("decimal")
local helper = require('test.helper.cluster_no_replication')

local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        for i = 1, 5 do
            local r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i, i, i, i, i, true, "123", decimal.new('4.599999')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})

            r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space2"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i, i, i, i, i, true, "123", decimal.new('4.599999')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
    end
)

g.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage1:call("box.execute", { [[truncate table "arithmetic_space2"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage2:call("box.execute", { [[truncate table "arithmetic_space2"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g1.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g1.before_each(
    function()
        local api = cluster:server("api-1").net_box

        for k = 1,5 do
            local r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {k, k, k*2, k*3, k, k, k, true, "123", decimal.new('4.6')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
    end
)

g1.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "arithmetic_space"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "arithmetic_space"]] })
    end
)

g1.after_all(function()
    helper.stop_test_cluster()
end)


g.test_arithmetic_invalid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" % 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ^ 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ++ 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ** 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" // 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ** 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" +- 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" +* 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    -- arithemic operation on boolean col
    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "boolean_col" + "boolean_col" > 0]], {} })
    t.assert_str_contains(tostring(err), "Type mismatch: can not convert boolean(TRUE) to integer, decimal, double, datetime or interval")

    -- arithemic operation on string col
    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "string_col" + "string_col" > 0]], {} })
    t.assert_str_contains(tostring(err), "Type mismatch: can not convert string('123') to integer, decimal, double, datetime or interval")

    -- arithemic operation on number col
    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "number_col" + "number_col" > 0]], {} })
    t.assert_str_contains(tostring(err), "Type mismatch: can not convert number(4.599999) to integer, decimal, double, datetime or interval")
end

g.test_arithmetic_valid = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    local r, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where 2 + 2 = 4]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        res_all.rows
    )

    -- test several identical operators
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where
            "id" + "id" > 0 and "id" + "id" + "id" > 0
            or ("id" * "id" > 0 and "id" * "id" * "id" > 0)
            or ("id" - "id" < 0 and "id" - "id" - "id" < 0)
            or ("id" / "id" > 0 and "id" / "id" / "id" > 0)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- test several operators with different priority
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where
            "id" + "id" * "id" + "id" > 0
            and "id" - "id" * "id" - "id" > 0
            and "id" + "id" / "id" + "id" > 0
            and "id" - "id" / "id" - "id" > 0
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- test priority
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where cast("a" as decimal) / cast("b" as decimal) * cast("c" as decimal) = (cast("a" as decimal) / cast("b" as decimal)) * cast("c" as decimal)
            and cast("a" as decimal) / cast("b" as decimal) * cast("c" as decimal) = cast("a" as decimal) / (cast("b" as decimal) * cast("c" as decimal))
            and cast("a" as decimal) * cast("b" as decimal) / cast("c" as decimal) = (cast("a" as decimal) * cast("b" as decimal)) / cast("c" as decimal)
            and cast("a" as decimal) * cast("b" as decimal) / cast("c" as decimal) = cast("a" as decimal) * (cast("b" as decimal) / cast("c" as decimal))
            and "id" + "id" * "id" / "id"  = "id" + ("id" * "id" / "id")
            and "id" * "id" / "id" + "id" = ("id" * "id" / "id") + "id"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        res.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     res.rows,
    --     res_all.rows
    -- )
end


g.test_arithmetic_with_bool = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- test arithmetic_expr [comparison operator] number
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" + cast("a" as decimal) >= 0
            and "id" + "c" <= 2
            and "id" + "d" > 0
            and "id" + "e" < 0
            and "id" + cast("a" as decimal) = 2
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- test arithmetic_expr [comparison operator] arithmetic_expr
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" + "a" >= "id" * 1
            and "id" + "c" <= "id" * 1
            and "id" + "d" > "id" * 1
            and "id" + "e" < "id" * 1
            and "id" + "a" = "id" * 1
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- test arithmetic_expr [comparison operator] row
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" + "a" >= "id"
            and "id" + "c" <= "id"
            and "id" + "d" > "id"
            and "id" + "e" < "id"
            and "id" + "a" = "id"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- test number [comparison operator] arithmetic_expr
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where 0 >= "id" + "a"
            and 2 <= "id" + "c"
            and 0 > "id" + "d"
            and 0 < "id" + "e"
            and 2 = "id" + "a"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- test row [comparison operator] arithmetic_expr
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" >= "id" + "a"
            and "id" <=  "id" + "c"
            and "id" > "id" + "d"
            and "id" < "id" + "e"
            and "id" = "id" + "a"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )
end

-- Type mismatch: can not convert scalar(1) to integer, decimal or double
g.test_join_simple_arithmetic = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute",
    { [[
    SELECT "t3"."id", "t3"."a", "t8"."b"
    FROM
        (SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" > 0
        UNION ALL
            SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c"  > 0) AS "t3"
    INNER JOIN
        (SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" < 0
        UNION ALL
        SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" > 0) AS "t8"
        ON "t3"."id" + 1 = "t8"."id1" + 2
    WHERE "t3"."id" = 1]], { } })

    t.assert_equals(err, nil)
    t.assert_equals(
        r.metadata,
        {
            {name = "t3.id", type = "integer"},
            {name = "t3.a", type = "integer"},
            {name = "t8.b", type = "integer"},
        }
    )
    -- t.assert_equals(
    --     r.rows,
    --     {
    --          ...
    --     }
    -- )
end

g.test_selection_simple_arithmetic = function()
    local api = cluster:server("api-1").net_box

    -- check selection with arithmetic expr and `>` comparison operator
    local r, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" + 2 > 0]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- check selection with arithmetic expr and `between` comparison operator
    local r, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" between "id" - 1 and "id" * 4]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- check selection with arithmetic expr and `!=` comparison operator
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" * 2 != "a" * 2
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )

    -- check selection with arithmetic expr and boolean operators
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" * 2 > 2 or "id" * 2 > 0 and "id" * 2 = 0
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    -- t.assert_items_equals(
    --     r.rows,
    --     { ... }
    -- )
end

g1.test_associativity = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- addition and multiplication are associative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "a" + ("b" + "c") = ("a" + "b") + "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "a" * ("b" * "c") = ("a" * "b") * "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    -- subtraction is left-associative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where ("a" - "b") - "c" = "a" - "b" - "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "a" - ("b" - "c" ) = "a" - "b" - "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res.rows, res_all.rows)

    -- division is left-associative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal) = cast("a" as decimal) / cast("b" as decimal) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where cast("a" as decimal) / (cast("b" as decimal) / cast("c" as decimal)) = (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res.rows, res_all.rows)
end

g1.test_commutativity = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- addition and multiplication are commutative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "a" + "b" = "b" + "a"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "a" * "b" = "b" * "a"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    -- subtraction and division are not commutative
    -- and x [-|/] y = y [-|/] x is true only when
    -- x, y have specific condition
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where "a" - "b" = "b" - "a"
        except
        select "id" from "arithmetic_space" where "a" = "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, {})

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where cast("b" as decimal) / cast("a" as decimal) = cast("a" as decimal) / cast("b" as decimal)
        except
        select "id" from "arithmetic_space" where "a" = "b" or "a" = -1 * "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, {})
end

g1.test_distributivity = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- multiplication if left- and right-distributive over addition|subtraction
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
            "a"  * ("b" + "c") = "a" * "b" + "a" * "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
            ("a" + "b") * "c" = "a" * "c" + "b" * "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    -- division is right-distributive over addition|subtraction
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where (cast("a" as decimal) + cast("b" as decimal)) / cast("c" as decimal) = cast("a" as decimal) / cast("c" as decimal) + cast("b" as decimal) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where cast("a" as decimal) / (cast("b" as decimal) + cast("c" as decimal)) = cast("a" as decimal) / cast("b" as decimal) + cast("a" as decimal) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res.rows, res_all.rows)
end