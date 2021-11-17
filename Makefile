all: build

build:
	cargo build

integration_test_app:
	cd test_app && rm -rf tmp/tarantool.log && TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/

test:
	cargo test

lint:
	cargo clippy -- -Dclippy::all -Wclippy::pedantic

build_test_app_osx:
	cd test_app && cartridge build
	cp -rf target/debug/libsbroad.dylib test_app/.rocks/lib/tarantool/sbroad.dylib

test_osx: test build build_test_app_osx integration_test_app
