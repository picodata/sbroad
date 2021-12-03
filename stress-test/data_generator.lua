#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local yaml = require('yaml')
local clock = require("clock")

local params = { ... }

if #params ~= 1 then
    print("Не задано кол-во записей для генерации")
    os.exit(1)
end

local start_time = clock.time();

local record_count = params[1]

local api = nb.connect("admin:app-cluster-cookie@localhost:3301")

api:eval("function set_schema(s) local cartridge = require('cartridge'); cartridge.set_schema(s) end")

local config = {
    spaces = {
        VEHICLE_ACTUAL = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "gov_number",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        },
        VEHICLE_HISTORY = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "gov_number",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        },
    }
}

api:call("set_schema", { yaml.encode(config) })
print("table was created")

fiber.sleep(3)
print("loading data was started")

local storage1 = nb.connect("admin:app-cluster-cookie@localhost:3302")
local storage2 = nb.connect("admin:app-cluster-cookie@localhost:3304")

storage1:eval("return box.space.VEHICLE_ACTUAL:truncate();")
storage1:eval("return box.space.VEHICLE_HISTORY:truncate();")
storage2:eval("return box.space.VEHICLE_ACTUAL:truncate();")
storage2:eval("return box.space.VEHICLE_HISTORY:truncate();")

for i = 1, record_count, 1 do
    api:call("insert_record", {
        "VEHICLE_ACTUAL",
        {
            id = i,
            gov_number = "a777a750",
            sys_op = 0
        }
    })

    api:call("insert_record", {
        "VEHICLE_HISTORY",
        {
            id = i,
            gov_number = "a777a750",
            sys_op = 0
        }
    })
end
api:close()

print("data was loaded")
fiber.sleep(3)

print(
        string.format(
                "Storage 1 has %d actual and %d history records",
                storage1:eval("return box.space.VEHICLE_ACTUAL:count();"),
                storage1:eval("return box.space.VEHICLE_HISTORY:count();")
        )
)

print(
        string.format(
                "Storage 2 has %d actual and %d history records",
                storage2:eval("return box.space.VEHICLE_ACTUAL:count();"),
                storage2:eval("return box.space.VEHICLE_HISTORY:count();")
        )
)

print(

)

storage1:close()
storage2:close()

print(string.format("Execution time: %f s", (clock.time() - start_time) ))