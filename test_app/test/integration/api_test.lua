local t = require('luatest')
local g = t.group('integration_api')

local helper = require('test.helper')
local cluster = helper.cluster

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

g.test_bucket_id_calculation = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123" } })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), "space_name is required")

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

    r, err = api:call("sbroad.calculate_bucket_id", { { id = 1 }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), [[Missing quoted sharding key column]])

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
    t.assert_str_contains(tostring(err), "Parsing error")
end

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
            {name = "t2.a", type = "any"},
        },
        rows = {},
    })
end

g.test_invalid_explain = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.explain", { [[SELECT "id", "name" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)]] })

    t.assert_str_contains(tostring(err), "Explain hasn't supported node Motion")
end

g.test_valid_explain = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.explain", { [[SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 0
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1 ]] })

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

g.test_except = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[insert into "t" ("id", "a") values (?, ?), (?, ?), (?, ?)]],
        {
            3, 777,
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
