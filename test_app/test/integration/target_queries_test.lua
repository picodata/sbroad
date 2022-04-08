local t = require('luatest')
local target_queries = t.group('target_queries')

local helper = require('test.helper')
local cluster = helper.cluster

-- datamart_query_types.test_query_type_ = function()
--     local api = cluster:server("api-1").net_box

--     local r, err = api:call("query", { [[
--     ]] })

--     t.assert_equals(err, nil)
--     t.assert_equals(r, {
--         metadata = {},
--         rows = {},
--     })
-- end

target_queries.before_each(
        function()
            local api = cluster:server("api-1").net_box

            api:call("insert_record", {
                "col1_transactions_actual",
                { col1 = 1, amount = 3, account_id = 1, sys_from = 0 }
            })
            api:call("insert_record", {
                "col1_transactions_actual",
                { col1 = 3, amount = 3, account_id = 1, sys_from = 0 }
            })
            api:call("insert_record", {
                "col1_transactions_history",
                { id = 1, col1 = 1, amount = 2, account_id = 1, sys_from = 0, sys_to = 2 }
            })
            api:call("insert_record", {
                "col1_transactions_history",
                { id = 2, col1 = 1, amount = 1, account_id = 1, sys_from = 0, sys_to = 1 }
            })

            api:call("insert_record", {
                "col1_col2_transactions_actual",
                { col1 = 1, col2 = 2, amount = 3, account_id = 1, sys_from = 0 }
            })
            api:call("insert_record", {
                "col1_col2_transactions_actual",
                { col1 = 1, col2 = 1, amount = 3, account_id = 1, sys_from = 0 }
            })
            api:call("insert_record", {
                "col1_col2_transactions_history",
                { id = 1, col1 = 1, col2 = 2, amount = 2, account_id = 1, sys_from = 0, sys_to = 2 }
            })
            api:call("insert_record", {
                "col1_col2_transactions_history",
                { id = 2, col1 = 1, col2 = 2, amount = 1, account_id = 1, sys_from = 0, sys_to = 1 }
            })

            api:call("insert_record", {
                "cola_accounts_actual",
                { id = 1, cola = 1, colb = 3, sys_from = 0 }
            })

            api:call("insert_record", {
                "cola_accounts_actual",
                { id = 1, cola = 2, colb = 3, sys_from = 0 }
            })
            api:call("insert_record", {
                "cola_accounts_history",
                { id = 1, cola = 1, colb = 2, sys_from = 0, sys_to = 2 }
            })

            api:call("insert_record", {
                "cola_colb_accounts_actual",
                { id = 1, cola = 1, colb = 3, sys_from = 0 }
            })
            api:call("insert_record", {
                "cola_colb_accounts_history",
                { id = 1, cola = 1, colb = 2, sys_from = 0, sys_to = 2 }
            })

            api:call("insert_record", {
                "col1_col2_transactions_num_actual",
                { col1 = 1, col2 = 2, amount = 3, account_id = 1, sys_from = 0 }
            })
            api:call("insert_record", {
                "col1_col2_transactions_num_history",
                { id = 1, col1 = 1, col2 = 2, amount = 2, account_id = 1, sys_from = 0, sys_to = 2 }
            })
        end
)

target_queries.after_each(
        function()
            local storage1 = cluster:server("storage-1-1").net_box
            storage1:call("box.execute", { [[truncate table "col1_transactions_actual"]] })
            storage1:call("box.execute", { [[truncate table "col1_transactions_history"]] })
            storage1:call("box.execute", { [[truncate table "col1_col2_transactions_actual"]] })
            storage1:call("box.execute", { [[truncate table "col1_col2_transactions_history"]] })

            local storage2 = cluster:server("storage-2-1").net_box
            storage2:call("box.execute", { [[truncate table "col1_transactions_actual"]] })
            storage2:call("box.execute", { [[truncate table "col1_transactions_history"]] })
            storage1:call("box.execute", { [[truncate table "col1_col2_transactions_actual"]] })
            storage1:call("box.execute", { [[truncate table "col1_col2_transactions_history"]] })
        end
)

target_queries.test_type_1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1]] })

    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        AND "col2" = 2]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        AND ("col2" = 2
        AND "amount" > 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            { name = "col1", type = "integer" },
            { name = "col2", type = "integer" },
            { name = "account_id", type = "integer" },
            { name = "amount", type = "integer" },
        },
        rows = {
            { 1, 2, 1, 3 }
        },
    })
end

target_queries.test_type_4 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1 OR "col1" = 3]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 3, 1, 3 },
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_5 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        AND "col2" = 2
        OR "col1" = 1
        AND "col2" = 1]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 },
        { 1, 1, 1, 3 }
    })
end

target_queries.test_type_6 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        OR ("col1" = 2
        OR "col1" = 3)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 3, 1, 3 },
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_7 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE ("col1" = 1
        OR ("col1" = 2
        OR "col1" = 3))
        AND ("col2" = 1
        OR "col2" = 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3 },
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_8 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE ("col1" = 1
        OR ("col1" = 2
        OR "col1" = 3))
        AND (("col2" = 1
        OR "col2" = 2)
        AND "amount" > 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_9 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
    WHERE "cola" = 1)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_10 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1)
  AND "amount" > 0]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_11 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE ROW("col1", "col2") IN
    (SELECT "id", "cola"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1)
    AND "amount" > 0]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3 }
    })
end

target_queries.test_type_12 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t8"."cola" = 1]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 1, 1, 2 },
        { 1, 1, 1, 1, 1, 3 },
        { 1, 1, 2, 1, 1, 2 },
        { 1, 1, 2, 1, 1, 3 },
        { 1, 1, 3, 1, 1, 2 },
        { 1, 1, 3, 1, 1, 3 },
    })
end

target_queries.test_type_13 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND ("t8"."cola" = 1
        AND "t3"."amount" > 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 3, 1, 1, 2 },
        { 1, 1, 3, 1, 1, 3 },
    })
end

target_queries.test_type_14 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t3"."col2" = 2
AND ("t8"."cola" = 1 AND "t8"."colb" = 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 1, 1, 1, 2 },
        { 1, 2, 1, 2, 1, 1, 2 },
        { 1, 2, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_15 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t3"."col2" = 2
AND ("t8"."cola" = 1 AND ("t8"."colb" = 2 AND "t3"."amount" > 0))]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 1, 1, 1, 2 },
        { 1, 2, 1, 2, 1, 1, 2 },
        { 1, 2, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_17 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t8"."cola" = 2]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 1, 2, 3 },
        { 1, 1, 2, 1, 2, 3 },
        { 1, 1, 3, 1, 2, 3 }
    })
end

target_queries.test_type_18 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_num_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_num_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t3"."col2" = 2 AND ("t8"."cola" = 1 AND "t8"."colb" = 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "number" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2, 1, 1, 2 },
        { 1, 2, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_19 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND ("t3"."col2" = 1 AND "t8"."colb" = 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_20 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."col1" = "t8"."cola"
WHERE "t3"."col1" = 1 AND "t3"."col2" = 1]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3, 1, 1, 2 },
        { 1, 1, 1, 3, 1, 1, 3 }
    })
end

target_queries.test_type_21 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."col1" = "t8"."cola"
WHERE "t3"."col1" = 1]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 1, 1, 2 },
        { 1, 1, 1, 1, 1, 3 },
        { 1, 1, 2, 1, 1, 2 },
        { 1, 1, 2, 1, 1, 3 },
        { 1, 1, 3, 1, 1, 2 },
        { 1, 1, 3, 1, 1, 3 },
    })
end

target_queries.test_type_22 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "account_id" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1)
    AND ("col1" = 1 AND "col2" = 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_23 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "account_id" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_colb_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_colb_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1 AND "colb" = 2)
    AND ("col1" = 1 AND "col2" = 2)]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end