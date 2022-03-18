-- This file is required automatically by luatest.
-- Add common configuration here.

local fio = require('fio')
local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')

local helper = {}

helper.root = fio.dirname(fio.abspath(package.search('init')))
helper.datadir = fio.pathjoin(helper.root, 'tmp', 'db_test')
helper.server_command = fio.pathjoin(helper.root, 'init.lua')

helper.cluster = cartridge_helpers.Cluster:new({
    server_command = helper.server_command,
    datadir = helper.datadir,
    use_vshard = true,
    replicasets = {
        {
            alias = "api",
            uuid = cartridge_helpers.uuid('a'),
            roles = {'app.roles.api'},
            servers = {
                { instance_uuid = cartridge_helpers.uuid('a', 1) }
            },
        },
        {
            alias = "storage-1",
            uuid = cartridge_helpers.uuid("b"),
            roles = { "app.roles.storage" },
            servers = {
                { instance_uuid = cartridge_helpers.uuid("b", 1) }
            },
        },
        {
            alias = "storage-2",
            uuid = cartridge_helpers.uuid("c"),
            roles = { "app.roles.storage" },
            servers = {
                { instance_uuid = cartridge_helpers.uuid("c", 1) }
            },
        }
    }
})

local config = {
  ["schema"] = {
    spaces = {
      testing_space = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "id",
            type = "integer",
            is_nullable = false
          },
          {
            name = "name",
            type = "string",
            is_nullable = false
          },
          {
            name = "product_units",
            type = "integer",
            is_nullable = false
          },
          {
            name = "bucket_id",
            type = "unsigned",
            is_nullable = true
          }
        },
        indexes = {
          {
            name = "id",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "id",
                is_nullable = false,
                type = "integer"
              }
            }
          },
          {
            name = "bucket_id",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "bucket_id",
                is_nullable = true,
                type = "unsigned"
              }
            }
          }
        },
        sharding_key = {
          "id",
          "name"
        }
      },
      testing_space_hist = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "id",
            type = "integer",
            is_nullable = false
          },
          {
            name = "name",
            type = "string",
            is_nullable = false
          },
          {
            name = "product_units",
            type = "integer",
            is_nullable = false
          },
          {
            name = "bucket_id",
            type = "unsigned",
            is_nullable = true
          }
        },
        indexes = {
          {
            name = "id",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "id",
                is_nullable = false,
                type = "integer"
              }
            }
          },
          {
            name = "bucket_id",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "bucket_id",
                is_nullable = true,
                type = "unsigned"
              }
            }
          }
        },
        sharding_key = {
          "id",
          "name"
        }
      },
      space_simple_shard_key = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "id",
            type = "integer",
            is_nullable = false
          },
          {
            name = "name",
            type = "string",
            is_nullable = true
          },
          {
              name = "sysOp",
              type = "integer",
              is_nullable = false
          },
          {
            name = "bucket_id",
            type = "unsigned",
            is_nullable = true
          }
        },
        indexes = {
          {
            name = "id",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "id",
                is_nullable = false,
                type = "integer"
              }
            }
          },
          {
            name = "bucket_id",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "bucket_id",
                is_nullable = true,
                type = "unsigned"
              }
            }
          }
        },
        sharding_key = {
          "id",
        }
      },
      space_simple_shard_key_hist = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "id",
            type = "integer",
            is_nullable = false
          },
          {
            name = "name",
            type = "string",
            is_nullable = true
          },
          {
              name = "sysOp",
              type = "integer",
              is_nullable = false
          },
          {
            name = "bucket_id",
            type = "unsigned",
            is_nullable = true
          }
        },
        indexes = {
          {
            name = "id",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "id",
                is_nullable = false,
                type = "integer"
              }
            }
          },
          {
            name = "bucket_id",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "bucket_id",
                is_nullable = true,
                type = "unsigned"
              }
            }
          }
        },
        sharding_key = {
          "id"
        }
      }
    }
  }
}

helper.cluster_config = config

t.before_suite(function()
    fio.rmtree(helper.datadir)
    fio.mktree(helper.datadir)
    helper.cluster:start()
    helper.cluster:upload_config(config)
end)

t.after_suite(function()
    helper.cluster:stop()
end)

return helper
