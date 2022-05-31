#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local yaml = require('yaml')
local clock = require("clock")

local params = { ... }

if #params ~= 1 then
    print("The amount of records to generate is required")
    os.exit(1)
end

local start_time = clock.time();

local record_count = params[1]

local api = nb.connect("admin:app-cluster-cookie@localhost:3301")

api:eval("function set_schema(s) local cartridge = require('cartridge'); cartridge.set_schema(s) end")

local config = {
    spaces = {
        vehicle_actual = {
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
        vehicle_history = {
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
print("data loading started")

local storage1 = nb.connect("admin:app-cluster-cookie@localhost:3302")
local storage2 = nb.connect("admin:app-cluster-cookie@localhost:3304")

storage1:eval("return box.space.vehicle_actual:truncate();")
storage1:eval("return box.space.vehicle_history:truncate();")
storage2:eval("return box.space.vehicle_actual:truncate();")
storage2:eval("return box.space.vehicle_history:truncate();")

for i = 1, record_count, 1 do
    api:call("query", {
        [[insert into "vehicle_actual" ("id", "gov_number", "sys_op") values (?, ?, ?)]],
        {i, "a777a750", 0}
    })

    api:call("query", {
        [[insert into "vehicle_history" ("id", "gov_number", "sys_op") values (?, ?, ?)]],
        {i, "a777a750", 0}
    })
end
api:close()

print("data loading finished")
fiber.sleep(3)

print(
        string.format(
                "Storage 1 has %d actual and %d history records",
                storage1:eval("return box.space.vehicle_actual:count();"),
                storage1:eval("return box.space.vehicle_history:count();")
        )
)

print(
        string.format(
                "Storage 2 has %d actual and %d history records",
                storage2:eval("return box.space.vehicle_actual:count();"),
                storage2:eval("return box.space.vehicle_history:count();")
        )
)

print(

)

storage1:close()
storage2:close()

print(string.format("Execution time: %f s", (clock.time() - start_time) ))