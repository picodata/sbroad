package = 'sbroad'
version = 'scm-1'

source  = {
    url    = 'git+https://git.picodata.io/picodata/picodata/sbroad.git';
    branch = 'master';
}

description = {
    summary  = "Distributed SQL Tarantool Cartridge library";
}

dependencies = {
    'tarantool',
    'lua >= 5.1',
    'cartridge == 2.7.4-1',
    'checks'
}

build = {
    type = 'make',
    install_variables = {
        LIBDIR = "$(LIBDIR)",
        LUADIR = "$(LUADIR)",
        PROJECT_NAME = "sbroad",
    },
    build_target = "build_cartridge_engine",
    install_target = "install_release",
}
