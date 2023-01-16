local t = require('luatest')
local g = t.group('integration_api.operators')

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

        -- "t" contains:
        -- [1, 4.2]
        -- [2, decimal(6.66)]
        r, err = api:call("sbroad.execute", {
            [[insert into "t" ("id", "a") values (?, ?), (?, ?)]],
            {1, 4.2, 2, require('decimal').new(6.66)}
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
        storage1:call("box.execute", { [[truncate table "t"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "testing_space"]] })
        storage2:call("box.execute", { [[truncate table "testing_space_hist"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage2:call("box.execute", { [[truncate table "t"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_not_eq = function()
    local api = cluster:server("api-1").net_box
    -- id=1 already in space
    local r, err = api:call("sbroad.execute", {
        [[insert into "testing_space" ("id", "name", "product_units") values (?, ?, ?), (?, ?, ?)]],
        {2, "123", 2, 3, "123", 3}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})


    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT * FROM "testing_space" where "id" <> 1]],
            {}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {{2, "123", 2}, {3, "123", 3}},
    })


    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT * FROM "testing_space" where "id" <> 1 and "product_units" <> 3]],
            {}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {{2, "123", 2}},
    })
end

g.test_simple_shard_key_union_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < ?
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?
        ) as "t1"
        WHERE "id" = ? ]], { 0, 0, 1 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "t1.id", type = "integer"},
            {name = "t1.name", type = "string"},
        },
        rows = {
            { 1, "ok_hist" },
        },
    })
end

g.test_complex_shard_key_union_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM (
            SELECT "id", "name", "product_units" FROM "testing_space" WHERE "product_units" < ?
            UNION ALL
            SELECT "id", "name", "product_units" FROM "testing_space_hist" WHERE "product_units" > ?
        ) as "t1"
        WHERE "id" = ? and "name" = ? ]], { 3, 3, 1, '123' } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "t1.id", type = "integer"},
            {name = "t1.name", type = "string"},
            {name = "t1.product_units", type = "integer"}
        },
        rows = {
            { 1, "123", 1 },
            { 1, "123", 5 }
        },
    })
end

g.test_compare = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM "t" where "id" < 2 and "a" > 5]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "a", type = "number"},
        },
        rows = {},
    })
end

g.test_except = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[insert into "t" ("id", "a") values (?, ?), (?, ?), (?, ?)]],
        {
            -- After migration to the virtual tables in the executor
            -- we can't mix types in the same number column (as we don't
            -- cast types while population of the virtual tables).
            3, require('decimal').new(777),
            1000001, require('decimal').new(6.66),
            1000002, require('decimal').new(6.66)
        }
    })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 3})

    r, err = api:call("sbroad.execute", { [[
        SELECT "a" FROM "t" where "id" <= 3
        EXCEPT
        SELECT "a" FROM "t" where "id" > 3
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "a", type = "number"},
        },
        rows = {
            {4.2},
            {777}
        },
    })
end

g.test_is_null = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NULL
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {10}
        },
    })
end

g.test_is_not_null_1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NOT NULL and "id" = 10
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
        },
    })
end

g.test_is_not_null_2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NOT NULL
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end

g.test_between1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE
        (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "id" = 2) BETWEEN 1 AND 2
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1},
            {10}
        },
    })
end

g.test_between2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE
        "id" BETWEEN 1 AND 2
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end