#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local yaml = require('yaml')
local clock = require("clock")

local start_time = clock.time();

local api = nb.connect("admin:app-cluster-cookie@localhost:3301")

api:eval("function set_schema(s) local cartridge = require('cartridge'); cartridge.set_schema(s) end")

local config = {
    spaces = {
        t = {
            format = {
                {
                    name = "id",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "name",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "product_units",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "memtx",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "string",
                            is_nullable = false,
                        },
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
print("truncate the table on storages")

local storage1 = nb.connect("admin:app-cluster-cookie@localhost:3302")
local storage2 = nb.connect("admin:app-cluster-cookie@localhost:3304")

storage1:eval("return box.space.t:truncate();")
storage2:eval("return box.space.t:truncate();")

api:close()
storage1:close()
storage2:close()

print("truncation finished")
fiber.sleep(3)

print(string.format("Execution time: %f s", (clock.time() - start_time) ))
