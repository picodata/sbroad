-- This file is required automatically by luatest.
-- Add common configuration here.

local fio = require('fio')
local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')

local helper = {}

-- luacheck: ignore package
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
  ["executor_waiting_timeout"] = 200,
  ["executor_sharding_column"] = "bucket_id",
  ["jaeger_agent_host"] = "127.0.0.1",
  ["jaeger_agent_port"] = 6831,
  ["router_cache_capacity"] = 50,
  ["storage_cache_capacity"] = 200,
  ["storage_cache_size_bytes"] = 204800,
  ["schema"] = {
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
      testing_space_bucket_in_the_middle = {
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
            name = "bucket_id",
            type = "unsigned",
            is_nullable = true
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
      },
      col1_transactions_actual = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "col1",
            type = "integer",
            is_nullable = false
          },
          {
            name = "amount",
            type = "integer",
            is_nullable = true
          },
          {
            name = "account_id",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
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
            name = "col1",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "col1",
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
          "col1",
        }
      },
      col1_transactions_history = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "id",
            type = "integer",
            is_nullable = false,
          },
          {
            name = "col1",
            type = "integer",
            is_nullable = false,
            unique=false,
          },
          {
            name = "amount",
            type = "integer",
            is_nullable = true
          },
          {
            name = "account_id",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
            type = "integer",
            is_nullable = false
          },
          {
            name = "sys_to",
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
            type = "TREE",
            unique = true,
            parts = {
              {
                path = "id",
                is_nullable = false,
                type = "integer"
              }
            }
          },
          {
            name = "col1",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "col1",
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
          "col1",
        }
      },
      col1_col2_transactions_actual = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "col1",
            type = "integer",
            is_nullable = false
          },
          {
            name = "col2",
            type = "integer",
            is_nullable = false
          },
          {
            name = "amount",
            type = "integer",
            is_nullable = true
          },
          {
            name = "account_id",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
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
            name = "col1",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "col1",
                is_nullable = false,
                type = "integer"
              },
              {
                path = "col2",
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
          "col1", "col2"
        }
      },
      col1_col2_transactions_history = {
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
            name = "col1",
            type = "integer",
            is_nullable = false
          },
          {
            name = "col2",
            type = "integer",
            is_nullable = false
          },
          {
            name = "amount",
            type = "integer",
            is_nullable = true
          },
          {
            name = "account_id",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
            type = "integer",
            is_nullable = false
          },
          {
            name = "sys_to",
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
            name = "col1",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "col1",
                is_nullable = false,
                type = "integer"
              },
              {
                path = "col2",
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
          "col1", "col2"
        }
      },
      cola_accounts_actual = {
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
            name = "cola",
            type = "integer",
            is_nullable = false
          },
          {
            name = "colb",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
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
            name = "cola",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "cola",
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
          "cola",
        }
      },
      cola_accounts_history = {
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
            name = "cola",
            type = "integer",
            is_nullable = false
          },
          {
            name = "colb",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
            type = "integer",
            is_nullable = false
          },
          {
            name = "sys_to",
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
            name = "cola",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "cola",
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
          "cola",
        }
      },
      cola_colb_accounts_history = {
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
            name = "cola",
            type = "integer",
            is_nullable = false
          },
          {
            name = "colb",
            type = "integer",
            is_nullable = false
          },
          {
            name = "sys_from",
            type = "integer",
            is_nullable = false
          },
          {
            name = "sys_to",
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
            name = "cola",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "cola",
                is_nullable = false,
                type = "integer"
              },
              {
                path = "colb",
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
          "cola", "colb"
        }
      },
      cola_colb_accounts_actual = {
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
            name = "cola",
            type = "integer",
            is_nullable = false
          },
          {
            name = "colb",
            type = "integer",
            is_nullable = false
          },
          {
            name = "sys_from",
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
            name = "cola",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "cola",
                is_nullable = false,
                type = "integer"
              },
              {
                path = "colb",
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
          "cola", "colb"
        }
      },
      col1_col2_transactions_num_actual = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "col1",
            type = "number",
            is_nullable = false
          },
          {
            name = "col2",
            type = "integer",
            is_nullable = false
          },
          {
            name = "amount",
            type = "integer",
            is_nullable = true
          },
          {
            name = "account_id",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
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
            name = "col1",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "col1",
                is_nullable = false,
                type = "number"
              },
              {
                path = "col2",
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
          "col1", "col2"
        }
      },
      col1_col2_transactions_num_history = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "id",
            type = "number",
            is_nullable = false
          },
          {
            name = "col1",
            type = "number",
            is_nullable = false
          },
          {
            name = "col2",
            type = "integer",
            is_nullable = false
          },
          {
            name = "amount",
            type = "integer",
            is_nullable = true
          },
          {
            name = "account_id",
            type = "integer",
            is_nullable = true
          },
          {
            name = "sys_from",
            type = "integer",
            is_nullable = false
          },
          {
            name = "sys_to",
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
                type = "number"
              }
            }
          },
          {
            name = "col1",
            unique = false,
            type = "TREE",
            parts = {
              {
                path = "col1",
                is_nullable = false,
                type = "number"
              },
              {
                path = "col2",
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
          "col1", "col2"
        }
      },
      space_t1 = {
        is_local = false,
        temporary = false,
        engine = "memtx",
        format = {
          {
            name = "a",
            type = "integer",
            is_nullable = false
          },
          {
            name = "b",
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
            name = "a",
            unique = true,
            type = "TREE",
            parts = {
              {
                path = "a",
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
          "a",
        }
      },
      broken_hot = {
        format = {
          {
            is_nullable = false,
            name = "id",
            type = "number"
          },
          {
            is_nullable = false,
            name = "reqId",
            type = "number"
          },
          {
            is_nullable = false,
            name = "name",
            type = "string"
          },
          {
            is_nullable = false,
            name = "department",
            type = "string"
          },
          {
            is_nullable = false,
            name = "manager",
            type = "string"
          },
          {
            is_nullable = false,
            name = "salary",
            type = "number"
          },
          {
            is_nullable = false,
            name = "sysOp",
            type = "number"
          },
          {
            is_nullable = false,
            name = "bucket_id",
            type = "unsigned"
          }
        },
        temporary = false,
        engine = "memtx",
        is_local = false,
        sharding_key = {
          "id"
        },
        indexes = {
          {
            unique = true,
            parts = {
              {
                path = "id",
                type = "number",
                is_nullable = false
              }
            },
            type = "TREE",
            name = "id"
          },
          {
            unique = false,
            parts = {
              {
                path = "bucket_id",
                type = "unsigned",
                is_nullable = false
              }
            },
            type = "TREE",
            name = "bucket_id"
          }
        }
      },
      BROKEN = {
        format = {
          {
            is_nullable = false,
            name = "id",
            type = "number"
          },
          {
            is_nullable = false,
            name = "reqId",
            type = "number"
          },
          {
            is_nullable = false,
            name = "name",
            type = "string"
          },
          {
            is_nullable = false,
            name = "department",
            type = "string"
          },
          {
            is_nullable = false,
            name = "manager",
            type = "string"
          },
          {
            is_nullable = false,
            name = "salary",
            type = "number"
          },
          {
            is_nullable = false,
            name = "sysOp",
            type = "number"
          },
          {
            is_nullable = false,
            name = "bucket_id",
            type = "unsigned"
          }
        },
        temporary = false,
        engine = "memtx",
        is_local = false,
        indexes = {
          {
            unique = true,
            parts = {
              {
                path = "id",
                type = "number",
                is_nullable = false
              }
            },
            type = "TREE",
            name = "id"
          },
          {
            unique = false,
            parts = {
              {
                path = "bucket_id",
                type = "unsigned",
                is_nullable = false
              }
            },
            type = "TREE",
            name = "bucket_id"
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
