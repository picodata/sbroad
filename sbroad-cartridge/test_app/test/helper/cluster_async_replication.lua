-- This file is required automatically by luatest.
-- Add common configuration here.

local fio = require('fio')
local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
local config_handler = require('test.helper.config_handler')

local helper = {
    cluster = nil
}

-- luacheck: ignore package
helper.root = fio.dirname(fio.abspath(package.search('init')))
helper.datadir = fio.pathjoin(helper.root, 'tmp', 'db_test')
helper.server_command = fio.pathjoin(helper.root, 'init.lua')

local config, err = config_handler.get_init_config(helper.root)
if err ~= nil then
  t.fail(err)
end

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

return helper
