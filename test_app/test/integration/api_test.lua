local t = require('luatest')
local g = t.group('integration_api')
local fiber = require('fiber')

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

g.test_incorrect_query = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("query", { [[SELECT * FROM "testing_space" as "a"
            INNER JOIN "testing_space" as "b" ON "a"."id" = "b"."a_id"
        WHERE "id" = 5 and "name" = '123']] })
    t.assert_equals(err, "query wasn't implemented")
end

g.test_simple_shard_key_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM "space_simple_shard_key" where "id" = 5]] })
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

    r, err = api:call("query", { [[SELECT * FROM "space_simple_shard_key" where "id" = 1]] })
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
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 0
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1 ]] })
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

    local r, err = api:call("query", { [[SELECT * FROM "testing_space" where "id" = 1 and "name" = '457']] })
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

    r, err = api:call("query", { [[SELECT * FROM "testing_space" where "id" = 1 and "name" = '123']] })
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
            SELECT "id", "name", "product_units" FROM "testing_space" WHERE "product_units" < 3
            UNION ALL
            SELECT "id", "name", "product_units" FROM "testing_space_hist" WHERE "product_units" > 3
        ) as "t1"
        WHERE "id" = 1 and "name" = '123' ]] })
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

