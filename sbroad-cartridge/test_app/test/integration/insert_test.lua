local t = require('luatest')
local g = t.group('integration_api.insert')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        -- "space_simple_shard_key" contains:
        -- [1, "ok", 1]
        -- [10, NULL, 0]
        local r, err = api:call("sbroad.execute", {
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
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage1:call("box.execute", { [[truncate table "t"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage2:call("box.execute", { [[truncate table "t"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_insert_1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    SELECT * FROM "space_simple_shard_key_hist" WHERE "id" > ?]], { 1 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, "ok", 1, 3940},
            {10, box.NULL, 0, 11520},
            {2, "ok_hist_2", 1, 22072}
        },
    })
end

g.test_insert_2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("name", "sysOp", "id")
    SELECT 'four', 5, 3 FROM "space_simple_shard_key_hist" WHERE "id" IN (
        SELECT ? FROM "space_simple_shard_key"
    )]], { 1 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, "ok", 1, 3940},
            {10, box.NULL, 0, 11520},
            {3, "four", 5, 21301}
        },
    })
end

g.test_insert_3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id") VALUES (?, ?), (?, ?)]], { 5, 4, 6, 5 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, "ok", 1, 3940},
            {5, box.NULL, 6, 6661},
            {10, box.NULL, 0, 11520},
            {4, box.NULL, 5, 27225},
        },
    })
end

-- check cyrillic consts support
g.test_insert_4 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, 'кириллица'), (?, ?, 'КИРИЛЛИЦА')]], { 5, 4, 6, 5 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, "ok", 1, 3940},
            {5, "КИРИЛЛИЦА", 6, 6661},
            {10, box.NULL, 0, 11520},
            {4, "кириллица", 5, 27225},
        },
    })
end

-- check cyrillic params support
g.test_insert_5 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, ?), (?, ?, ?)]], { 5, 4, "кириллица", 6, 5, "КИРИЛЛИЦА" } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, "ok", 1, 3940},
            {5, "КИРИЛЛИЦА", 6, 6661},
            {10, box.NULL, 0, 11520},
            {4, "кириллица", 5, 27225},
        },
    })
end

-- check big int
g.test_insert_6 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, ?)]], { 7, -9223372036854775808, "bigint" } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            { -9223372036854775808LL, "bigint", 7, 2274 },
            {1, "ok", 1, 3940},
            {10, box.NULL, 0, 11520}
        },
    })
end

-- check empty string
g.test_insert_7 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (8, 8, '')]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, "ok", 1, 3940},
            {8, "", 8, 12104},
            {10, box.NULL, 0, 11520}
        },
    })
end