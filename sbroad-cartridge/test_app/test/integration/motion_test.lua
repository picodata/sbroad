local t = require('luatest')
local g = t.group('integration_api.motion')

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

g.test_simple_motion_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT "id", "name" FROM "space_simple_shard_key"
        WHERE "id" in (SELECT "id" FROM "testing_space_hist" WHERE "product_units" > 3)]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {
            { 1, "ok" },
        },
    })
end

g.test_motion_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" > 0
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" in (SELECT "id" FROM (
            SELECT "id", "name" FROM "testing_space" WHERE "product_units" < 3
            UNION ALL
            SELECT "id", "name" FROM "testing_space_hist" WHERE "product_units" > 3
        ) as "t2"
        WHERE "id" = 1.00 and "name" = '123')]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "t1.id", type = "integer"},
            {name = "t1.name", type = "string"},
        },
        rows = {
            { 1, "ok" },
            { 1, "ok_hist" }
        },
    })
end

g.test_join_motion_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute",
    { [[SELECT "t3"."id", "t3"."name", "t8"."product_units"
    FROM
        (SELECT "id", "name"
            FROM "space_simple_shard_key"
            WHERE "sysOp" > ?
        UNION ALL
            SELECT "id", "name"
            FROM "space_simple_shard_key_hist"
            WHERE "sysOp" > ?) AS "t3"
    INNER JOIN
        (SELECT "id" as "id1", "product_units"
        FROM "testing_space"
        WHERE "product_units" < ?
        UNION ALL
        SELECT "id" as "id1", "product_units"
        FROM "testing_space_hist"
        WHERE "product_units" > ?) AS "t8"
        ON "t3"."id" = "t8"."id1"
    WHERE "t3"."id" = ?]], { 0, 0, 0, 0, 1} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "t3.id", type = "integer"},
            {name = "t3.name", type = "string"},
            {name = "t8.product_units", type = "any"},
        },
        rows = {
            { 1, "ok", 5 },
            { 1, "ok_hist", 5 }
        },
    })
end

g.test_empty_motion_result = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT "id", "name" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {},
    })

    r, err = api:call("sbroad.execute", { [[SELECT "id", "name" FROM "testing_space"
    WHERE ("id", "name") in (SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {},
    })


    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0)
        OR "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, "123", 1, 360}
        },
    })
end