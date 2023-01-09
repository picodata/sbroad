local t = require('luatest')
local g = t.group('schema_validate')
local helper = require('test.helper.cluster_no_replication')

g.before_all(
    function()
      local cfg = {
        schema = {
          spaces = {
            t = {
              format = {
                {
                  name = "id",
                  type = "integer",
                  is_nullable = false,
                },
                {
                  name = "a",
                  type = "map",
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
                      type = "integer",
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
      }


      helper.start_test_cluster(cfg)
    end
)

g.after_all(function()
  helper.stop_test_cluster()
end)

g.test_schema_invalid = function ()
  local api = helper.cluster:server("api-1").net_box

  local _, err = api:call("sbroad.execute", { [[select * from "t"]], {}})
  t.assert_str_contains(tostring(err), "Failed to get configuration: type map not implemented")
end
