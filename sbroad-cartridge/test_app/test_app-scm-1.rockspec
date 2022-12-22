package = 'test_app'
version = 'scm-1'
source  = {
    url = '/dev/null',
}
-- Put any modules your app depends on here
dependencies = {
    'tarantool',
    'lua >= 5.1',
    'checks == 3.1.0-1',
    'cartridge == 2.7.5-1',
    'metrics == 0.14.0-1',
    'crud == 0.14.0-1',
    'cartridge-cli-extensions == 1.1.1-1',
    'luatest == 0.5.7',
    'luacov'
}
build = {
    type = 'none';
}
