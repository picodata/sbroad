local t = require('luatest')
local datetime = require("datetime")
local g = t.group('integration_api')

local helper = require('test.helper.cluster_no_replication')
local config_handler = require('test.helper.config_handler')
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

g.after_test("test_unsupported_column", function()
    local default_config = config_handler.get_init_config(helper.root)
    cluster:upload_config(default_config)
end)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_bucket_id_calculation = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123" } })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), "space name is required")

    r, err = api:call("sbroad.calculate_bucket_id", { "1123" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            { id = 1, name = "123", product_units = 1 },
            "testing_space"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call("sbroad.calculate_bucket_id", { box.tuple.new{ 1, "123", 1 }, "testing_space" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123", 1 }, "testing_space" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

     -- calculate bucket id for space which bucket_id field is located in the middle of format
    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            { id = 1, name = "123", product_units = 1 },
            "testing_space_bucket_in_the_middle"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            box.tuple.new{ 1, "123", 1 },
            "testing_space_bucket_in_the_middle"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            { 1, "123", 1 },
            "testing_space_bucket_in_the_middle"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    -- incorrect input
    r, err = api:call("sbroad.calculate_bucket_id", { { 1 }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), [[expected to have 3 filed(s), got 1]])

    -- Test with a "bucket_id" field in the tuple.
    r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123", 1, box.NULL }, "testing_space" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123", 1, 1, 1 }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), [[expected to have 3 filed(s), got 5]])

    -- luacheck: max line length 150
    r, err = api:call("sbroad.calculate_bucket_id", { { id = 1 }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_equals(
        tostring(err),
        [[Sbroad Error: sharding key (quoted) column "\"name\"" in the quoted map {"\"id\"": "id"} (original map: {"id": Integer(1)}) not found]]
    )

    r, err = api:call("sbroad.calculate_bucket_id", { { id = 1, "123" }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_str_contains(
        tostring(err),
        [[expected string, tuple with a space name, or map with a space name as an argument]]
    )

end

g.test_incorrect_query = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space" INNER JOIN "testing_space"]], {} })
    t.assert_str_contains(tostring(err), "parsing error")
end

g.test_query_errored = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "NotFoundSpace"]], {} })
    t.assert_equals(tostring(err), "Sbroad Error: space \"NotFoundSpace\" not found")

    -- luacheck: max line length 140
    local _, err = api:call("sbroad.execute", { [[SELECT "NotFoundColumn" FROM "testing_space"]], {} })
    t.assert_equals(tostring(err), "Sbroad Error: column with name \"NotFoundColumn\" not found")

    local invalid_type_param = datetime.new{
        nsec = 123456789,
        sec = 20,
        min = 25,
        hour = 18,
        day = 20,
        month = 8,
        year = 2021,
        tzoffset  = 180
    }

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space" where "id" = ?]], {invalid_type_param} })
    t.assert_equals(
        tostring(err), "Sbroad Error: pattern with parameters parsing error: " ..
        [[Decode(Syntax("data did not match any variant of untagged enum EncodedValue"))]]
    )

    -- check err when params lenght is less then amount of sign `?`
    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space" where "id" = ?]], {} })
    t.assert_equals(
        tostring(err),
        "Sbroad Error: invalid node: parameter node does not refer to an expression"
    )
end

g.test_unsupported_column = function()
    local api = cluster:server("api-1").net_box

    local config = cluster:download_config()
    local space_with_unsupported_column = {
        format = {
            { type = "integer", name = "id", is_nullable = false },
            { type = "datetime", name = "unsupported_column", is_nullable = false },
            { type = "unsigned", name = "bucket_id", is_nullable = true },
        },
        temporary = false,
        engine = "memtx",
        is_local = false,
        sharding_key = { "id" },
        indexes = {
            {
                unique = true,
                parts = {{ path = "id", type = "integer", is_nullable = false}},
                name = "id",
                type = "TREE"
            },
            {
                unique = false,
                parts = { { path = "bucket_id", type = "unsigned", is_nullable = true } },
                name = "bucket_id",
                type = "TREE"
            }
        }
    }

    config["schema"]["spaces"]["space_with_unsupported_column"] = space_with_unsupported_column
    cluster:upload_config(config)

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "space_with_unsupported_column"]], {} })
    t.assert_str_contains(
        tostring(err),
        "type datetime not implemented"
    )
end

g.test_join_query_is_valid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space"
            INNER JOIN (SELECT "id" AS "inner_id", "name" AS "inner_name" FROM "testing_space") as t
            ON ("testing_space"."id", "testing_space"."name") = (t."inner_id", t."inner_name")
        WHERE "id" = 5 and "name" = '123']], {} })
    t.assert_equals(err, nil)
end

g.test_simple_shard_key_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM "space_simple_shard_key" where "id" = ?]], { 5 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
        },
        rows = {},
    })

    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT *, "bucket_id" FROM "space_simple_shard_key" where "id" = ?]],
            { 1.000 }
        }
    )
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

g.test_complex_shard_key_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT *, "bucket_id" FROM "testing_space" where "id" = ? and "name" = ?]],
            { 1, '457'}
        }
    )
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

    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT *, "bucket_id" FROM "testing_space" where "id" = 1 and "name" = '123']],
            {}
        }
    )
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

g.test_null_col_result = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT "id", "name" FROM "space_simple_shard_key" WHERE "id" = 10]],
            {}
        }
    )
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

g.test_anonymous_cols_naming = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?)
        OR "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?)
    ]], { 0, 0 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {
            {1, "123", 1}
        },
    })
end

g.test_decimal_double = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "t"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "a", type = "number"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, 4.2, 3940},
            {2, 6.66, 22072},
        },
    })
end

g.test_bucket_id_in_join = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[SELECT * FROM "space_simple_shard_key" as "t1" JOIN (SELECT "a" FROM "t") as "t2"
        ON "t1"."id" = "t2"."a"]],
        {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "t1.id", type = "integer"},
            {name = "t1.name", type = "string"},
            {name = "t1.sysOp", type = "integer"},
            {name = "t2.a", type = "number"},
        },
        rows = {},
    })
end

g.test_uppercase1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM broken
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
        },
        rows = {},
    })
end

g.test_uppercase2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM BROKEN
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
        },
        rows = {},
    })
end

g.test_uppercase3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "BROKEN"
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
        },
        rows = {},
    })
end

g.test_trace1 = function()
    local api = cluster:server("api-1").net_box

    -- local trace_id = 42
    -- local parent_id = 666
    -- -- 0 - not sampled, 1 - sampled
    -- local flags = 0

    -- -- W3C format
    -- local supported_version = 0
    -- local traceparent = string.format("%02x-%032x-%016x-%02x",
    --     supported_version, trace_id, parent_id, flags)
    -- local carrier = { ["traceparent"] = traceparent, ["tracestate"] = "foo=bar" }

    -- -- Jaegger format
    -- local depricated_parent_span = 0
    -- local value = string.format("%032x:%016x:%01x:%01x", trace_id, parent_id, depricated_parent_span, flags)
    -- local carrier = { ["uber-trace-id"] = value, ["uberctx-key1"] = "value1" }

    -- No external context, only query id
    local carrier = box.NULL

    api:call("box.cfg", {{log_level = 7}})
    local r, err = api:call("sbroad.trace", { [[
        SELECT "id" FROM "BROKEN" WHERE "id" = ?
    ]], {1}, carrier, "id1" })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
        },
        rows = {},
    })
    api:call("box.cfg", {{log_level = 5}})

    local pattern = 'tracer: Global'
    t.assert_equals(helper.grep_log(pattern), pattern)
end
