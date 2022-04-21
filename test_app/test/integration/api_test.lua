local t = require('luatest')
local fiber = require('fiber')
local g = t.group('integration_api')

local helper = require('test.helper')
local cluster = helper.cluster

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        local r = api:call("insert_record", { "testing_space", { id = 1, name = "123", product_units = 1 } })
        t.assert_equals(r, true)

        r = api:call("insert_record", { "testing_space_hist", { id = 1, name = "123", product_units = 5 } })
        t.assert_equals(r, true)

        r = api:call("insert_record", { "space_simple_shard_key", { id = 1, name = "ok", sysOp = 1 } })
        t.assert_equals(r, true)

        r = api:call("insert_record", { "space_simple_shard_key_hist", { id = 1, name = "ok_hist", sysOp = 3 }})
        r = api:call("insert_record", { "space_simple_shard_key_hist", { id = 2, name = "ok_hist_2", sysOp = 1 }})
        t.assert_equals(r, true)

        local r = api:call("insert_record", { "space_simple_shard_key", { id = 10, name = box.NULL, sysOp = 0 } })
        t.assert_equals(r, true)
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
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
    end
)

g.test_bucket_id_calculation = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("calculate_bucket_id", { "testing_space", { id = 1, name = "123", product_units = 1 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call("calculate_bucket_id_by_dict", { "testing_space", { id = 1, name = "123", product_units = 1 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    _, err = api:call("calculate_bucket_id_by_dict", { "testing_space", { id = 1 }})
    t.assert_equals(err, "CustomError(\"The dict of args missed key/value to calculate bucket_id. Column: name\")")
end

g.test_incorrect_query = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("query", { [[SELECT * FROM "testing_space" INNER JOIN "testing_space"]], {} })
    t.assert_equals(err, "CustomError(\"Parsing error: Error { variant: ParsingError { positives: [SubQuery], negatives: [] }, location: Pos(41), line_col: Pos((1, 42)), path: None, line: \\\"SELECT * FROM \\\\\\\"testing_space\\\\\\\" INNER JOIN \\\\\\\"testing_space\\\\\\\"\\\", continued_line: None }\")")
end

g.test_join_query_is_valid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("query", { [[SELECT * FROM "testing_space"
            INNER JOIN (SELECT "id" AS "inner_id", "name" AS "inner_name" FROM "testing_space") as t
            ON ("testing_space"."id", "testing_space"."name") = (t."inner_id", t."inner_name")
        WHERE "id" = 5 and "name" = '123']], {} })
    t.assert_equals(err, nil)
end

g.test_simple_shard_key_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM "space_simple_shard_key" where "id" = ?]], { 5 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {},
    })

    r, err = api:call("query", { [[SELECT * FROM "space_simple_shard_key" where "id" = ?]], { 1 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            { 1, "ok", 1, 3940 }
        },
    })
end

g.test_simple_shard_key_union_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < ?
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?
        ) as "t1"
        WHERE "id" = ? ]], { 0, 0, 1 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {
            { 1, "ok_hist" },
        },
    })
end

g.test_complex_shard_key_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM "testing_space" where "id" = ? and "name" = ?]], { 1, '457'} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {},
    })

    r, err = api:call("query", { [[SELECT * FROM "testing_space" where "id" = 1 and "name" = '123']], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            { 1, "123", 1, 360 }
        },
    })
end

g.test_complex_shard_key_union_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM (
            SELECT "id", "name", "product_units" FROM "testing_space" WHERE "product_units" < ?
            UNION ALL
            SELECT "id", "name", "product_units" FROM "testing_space_hist" WHERE "product_units" > ?
        ) as "t1"
        WHERE "id" = ? and "name" = ? ]], { 3, 3, 1, '123' } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"}
        },
        rows = {
            { 1, "123", 1 },
            { 1, "123", 5 }
        },
    })
end


g.test_simple_motion_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT "id", "name" FROM "space_simple_shard_key"
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

    local r, err = api:call("query", { [[SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" > 0
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" in (SELECT "id" FROM (
            SELECT "id", "name" FROM "testing_space" WHERE "product_units" < 3
            UNION ALL
            SELECT "id", "name" FROM "testing_space_hist" WHERE "product_units" > 3
        ) as "t2"
        WHERE "id" = 1 and "name" = '123')]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {
            { 1, "ok" },
            { 1, "ok_hist" }
        },
    })
end

g.test_null_col_result = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT "id", "name" FROM "space_simple_shard_key" WHERE "id" = 10]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {
            { 10, box.NULL },
        },
    })
end

g.test_join_motion_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query",
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
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {
            { 1, "ok", 5 },
            { 1, "ok_hist", 5 }
        },
    })
end

g.test_anonymous_cols_naming = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?)
        OR "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?)
    ]], { 0, 0 } })

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

g.test_empty_motion_result = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT "id", "name" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {},
    })

    r, err = api:call("query", { [[SELECT "id", "name" FROM "testing_space"
    WHERE ("id", "name") in (SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {},
    })


    r, err = api:call("query", { [[SELECT * FROM "testing_space"
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
