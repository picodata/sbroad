local t = require('luatest')
local g = t.group('integration_api.explain')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        -- "testing_space" contains:
        -- [1, "123", 1]
        local r, err = api:call("sbroad.execute", {
            [[insert into "testing_space" ("id", "name", "product_units") values (?, ?, ?)]],
            {1, "123", 1}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        -- "testing_space_hist" contains:
        -- [1, "123", 5]
        r, err = api:call("sbroad.execute", {
            [[insert into "testing_space_hist" ("id", "name", "product_units") values (?, ?, ?)]],
            {1, "123", 5}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        -- "space_simple_shard_key" contains:
        -- [1, "ok", 1]
        -- [10, NULL, 0]
        r, err = api:call("sbroad.execute", {
            [[insert into "space_simple_shard_key" ("id", "name", "sysOp") values (?, ?, ?), (?, ?, ?)]],
            {1, "ok", 1, 10, box.NULL, 0}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})

        -- "space_simple_shard_key_hist" contains:
        -- [1, "ok_hist", 3]
        -- [2, "ok_hist_2", 1]
        r, err = api:call("sbroad.execute", {
            [[insert into "space_simple_shard_key_hist" ("id", "name", "sysOp") values (?, ?, ?), (?, ?, ?)]],
            {1, "ok_hist", 3, 2, "ok_hist_2", 1}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})
    end
)

g.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "testing_space"]] })
        storage1:call("box.execute", { [[truncate table "testing_space_hist"]] })
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "testing_space"]] })
        storage2:call("box.execute", { [[truncate table "testing_space_hist"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_motion_explain = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[EXPLAIN SELECT "id", "name" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(
        r,
        {
            "projection (\"testing_space\".\"id\" -> \"id\", \"testing_space\".\"name\" -> \"name\")",
            "    selection ROW(\"testing_space\".\"id\") in ROW($0)",
            "        scan \"testing_space\"",
            "subquery $0:",
            "motion [policy: full, generation: none]",
            "            scan",
            "                projection (\"space_simple_shard_key_hist\".\"id\" -> \"id\")",
            "                    selection ROW(\"space_simple_shard_key_hist\".\"sysOp\") < ROW(0)",
            "                        scan \"space_simple_shard_key_hist\"",
        }
    )
end

g.test_join_explain = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[EXPLAIN SELECT *
FROM
    (SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 1
     UNION ALL
     SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "tid"  FROM "testing_space" where "id" <> 1) AS "t8"
    ON "t3"."id" = "t8"."tid"
WHERE "t3"."name" = '123']], {} })
    t.assert_equals(err, nil)
    t.assert_equals(
        r,
        -- luacheck: max line length 210
        {
            "projection (\"t3\".\"id\" -> \"id\", \"t3\".\"name\" -> \"name\", \"t8\".\"tid\" -> \"tid\")",
            "    selection ROW(\"t3\".\"name\") = ROW('123')",
            "        join on ROW(\"t3\".\"id\") = ROW(\"t8\".\"tid\")",
            "            scan \"t3\"",
            "                union all",
            "                    projection (\"space_simple_shard_key\".\"id\" -> \"id\", \"space_simple_shard_key\".\"name\" -> \"name\")",
            "                        selection ROW(\"space_simple_shard_key\".\"sysOp\") < ROW(1)",
            "                            scan \"space_simple_shard_key\"",
            "                    projection (\"space_simple_shard_key_hist\".\"id\" -> \"id\", \"space_simple_shard_key_hist\".\"name\" -> \"name\")",
            "                        selection ROW(\"space_simple_shard_key_hist\".\"sysOp\") > ROW(0)",
            "                            scan \"space_simple_shard_key_hist\"",
            "            motion [policy: segment([ref(\"tid\")]), generation: none]",
            "                scan \"t8\"",
            "                    projection (\"testing_space\".\"id\" -> \"tid\")",
            "                        selection ROW(\"testing_space\".\"id\") <> ROW(1)",
            "                            scan \"testing_space\"",
        }
    )
end

g.test_valid_explain = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[EXPLAIN SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 0
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(
        r,
        -- luacheck: max line length 210
        {
            "projection (\"t1\".\"id\" -> \"id\", \"t1\".\"name\" -> \"name\")",
            "    selection ROW(\"t1\".\"id\") = ROW(1)",
            "        scan \"t1\"",
            "            union all",
            "                projection (\"space_simple_shard_key\".\"id\" -> \"id\", \"space_simple_shard_key\".\"name\" -> \"name\")",
            "                    selection ROW(\"space_simple_shard_key\".\"sysOp\") < ROW(0)",
            "                        scan \"space_simple_shard_key\"",
            "                projection (\"space_simple_shard_key_hist\".\"id\" -> \"id\", \"space_simple_shard_key_hist\".\"name\" -> \"name\")",
            "                    selection ROW(\"space_simple_shard_key_hist\".\"sysOp\") > ROW(0)",
            "                        scan \"space_simple_shard_key_hist\"",
        }
    )
end