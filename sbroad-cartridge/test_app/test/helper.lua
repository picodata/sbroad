-- This file is required automatically by luatest.
-- Add common configuration here.

local fio = require('fio')
local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
local errno = require('errno')

local helper = {
    cluster = nil
}

-- luacheck: ignore package
helper.root = fio.dirname(fio.abspath(package.search('init')))
helper.datadir = fio.pathjoin(helper.root, 'tmp', 'db_test')
helper.server_command = fio.pathjoin(helper.root, 'init.lua')

helper.grep_log = function(what, bytes)
  local filename = fio.pathjoin(helper.root, 'tmp', 'tarantool.log')
  local file = fio.open(filename, {'O_RDONLY', 'O_NONBLOCK'})

  local function fail(msg)
      local err = errno.strerror()
      if file ~= nil then
          file:close()
      end
      error(string.format("%s: %s: %s", msg, filename, err))
  end

  if file == nil then
      fail("Failed to open log file")
  end
  io.flush() -- attempt to flush stdout == log fd
  local filesize = file:seek(0, 'SEEK_END')
  if filesize == nil then
      fail("Failed to get log file size")
  end
  local bytes = bytes or 65536 -- don't read whole log - it can be huge
  bytes = bytes > filesize and filesize or bytes
  if file:seek(-bytes, 'SEEK_END') == nil then
      fail("Failed to seek log file")
  end
  local found, buf
  repeat -- read file in chunks
      local s = file:read(2048)
      if s == nil then
          fail("Failed to read log file")
      end
      local pos = 1
      repeat -- split read string in lines
          local endpos = string.find(s, '\n', pos)
          endpos = endpos and endpos - 1 -- strip terminating \n
          local line = string.sub(s, pos, endpos)
          if endpos == nil and s ~= '' then
              -- line doesn't end with \n or eof, append it to buffer
              -- to be checked on next iteration
              buf = buf or {}
              table.insert(buf, line)
          else
              if buf ~= nil then -- prepend line with buffered data
                  table.insert(buf, line)
                  line = table.concat(buf)
                  buf = nil
              end
              if string.match(line, "Starting instance") then
                  found = nil -- server was restarted, reset search
              else
                  found = string.match(line, what) or found
              end
          end
          pos = endpos and endpos + 2 -- jump to char after \n
      until pos == nil
  until s == ''
  file:close()
  return found
end

-- helper.cluster = nil

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
        space_for_breake_cache = {
          format = {
              { name = "id", type = "integer", is_nullable = false },
              { name = "field1", type = "number", is_nullable = false },
              { name = "field2", type = "number", is_nullable = false },
              { name = "field3", type = "string", is_nullable = false },
              { name = "field4", type = "boolean", is_nullable = false },
              { name = "field5", type = "integer", is_nullable = false },
              { name = "field6", type = "integer", is_nullable = false },
              { name = "field7", type = "integer", is_nullable = false },
              { name = "field8", type = "integer", is_nullable = false },
              { name = "field9", type = "integer", is_nullable = false },
              { name = "field10", type = "string", is_nullable = false },
              { name = "field11", type = "string", is_nullable = false },
              { name = "field12", type = "integer", is_nullable = false },
              { name = "field13", type = "number", is_nullable = false },
              { name = "bucket_id", type = "unsigned", is_nullable = true },
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

helper.start_test_cluster = function (cfg)
    fio.rmtree(helper.datadir)
    fio.mktree(helper.datadir)

    helper.cluster = cartridge_helpers.Cluster:new({
            server_command = helper.server_command,
            datadir = helper.datadir,
            use_vshard = true,
            cookie='123',
            replicasets = {
                {
                    alias = "api",
                    uuid = cartridge_helpers.uuid('a'),
                    roles = {'app.roles.api'},
                    servers = 1,
                },
                {
                    alias = "storage-1",
                    uuid = cartridge_helpers.uuid("b"),
                    roles = { "app.roles.storage" },
                    all_rw = false,
                    servers = 2,
                    weight = 1
                },
                {
                    alias = "storage-2",
                    uuid = cartridge_helpers.uuid("c"),
                    roles = { "app.roles.storage" },
                    servers = 1,
                }
            }
    })

    helper.cluster:start()
    helper.cluster:upload_config(cfg)
end

helper.stop_test_cluster = function ()
    helper.cluster:stop()
end

t.before_suite(function()
    helper.start_test_cluster(config)
end)

t.after_suite(function()
    helper.stop_test_cluster()
end)

return helper
