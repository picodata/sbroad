local t = require('luatest')
local g = t.group('sbroad_with_acl')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
            cluster = helper.cluster
        end
)

g.after_all(
        function()
            helper.stop_test_cluster()
        end
)

g.test_drop_user = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ DROP USER user ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end