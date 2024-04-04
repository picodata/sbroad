local t = require('luatest')
local g = t.group('integration_api.cte')

local helper = require('test.helper.cluster_no_replication')

g.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
            local api = helper.cluster:server("api-1").net_box

            local r, err = api:call("sbroad.execute", { [[
                    INSERT INTO "t"("id", "a")
                    VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)
            ]], })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 5})
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_cte = function ()
    local r, err
    local api = helper.cluster:server("api-1").net_box

    -- basic cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3)
        SELECT b FROM cte
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "CTE.B", type = "number"} })
    t.assert_items_equals(r["rows"], { {4}, {5} })

    -- nested cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte1 (b) AS (SELECT "a" FROM "t" WHERE "id" > 3),
             cte2 AS (SELECT b FROM cte1)
        SELECT * FROM cte2
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "CTE2.B", type = "number"} })
    t.assert_items_equals(r["rows"], { {4}, {5} })

    -- reuse cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "CTE.B", type = "number"} })
    t.assert_items_equals(r["rows"], { {4}, {5}, {4}, {5} })

    -- join table with cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" = 1 OR "id" = 2)
        SELECT cte.b, "t"."a" FROM cte JOIN "t" ON cte.b = "t"."id"
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r["metadata"],
        { {name = "CTE.B", type = "number"}, {name = "t.a", type = "number"} }
    )
    t.assert_items_equals(r["rows"], { {1, 1}, {2, 2} })

    -- cte in aggregate
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3)
        SELECT count(b) FROM cte
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "COL_1", type = "integer"} })
    t.assert_items_equals(r["rows"], { {2} })

    -- cte in subquery
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" IN (1, 2, 3))
        SELECT * FROM "t" WHERE "a" IN (SELECT b FROM cte)
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r["metadata"],
        { {name = "id", type = "integer"}, {name = "a", type = "number"} }
    )
    t.assert_items_equals(r["rows"], { {1, 1}, {2, 2}, {3, 3} })

    -- values in cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (VALUES (1), (2), (3))
        SELECT b FROM cte
    ]], })
    t.assert_equals(err, nil)
    -- FIXME: metadata type from tarantool is not correct
    t.assert_items_equals(r["metadata"], { {name = "CTE.B", type = "any"} })
    t.assert_items_equals(r["rows"], { {1}, {2}, {3} })

    -- union all in cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" = 1),
        cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" FROM "t" WHERE "id" = 2)
        SELECT b FROM cte2
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "CTE2.B", type = "number"} })
    t.assert_items_equals(r["rows"], { {1}, {2} })

    -- join in cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte (c) AS (
            SELECT t1."a" FROM "t" t1
            JOIN "t" t2 ON t1."a" = t2."id"
            WHERE t1."id" = 1
        )
        SELECT c FROM cte
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "CTE.C", type = "number"} })
    t.assert_items_equals(r["rows"], { {1} })

    -- order by in cte
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3 ORDER BY "a" DESC)
        SELECT b FROM cte
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "CTE.B", type = "number"} })
    t.assert_items_equals(r["rows"], { {5}, {4} })
end

