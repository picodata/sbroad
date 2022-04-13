all: build

OS := $(shell uname -s)
ifeq ($(OS), Linux)
	SRC_LIB = libsbroad.so
	DEST_LIB = sbroad.so
else
	ifeq ($(OS), Darwin)
		SRC_LIB = libsbroad.dylib
		DEST_LIB = sbroad.dylib
	endif
endif
build:
	cargo build --release

integration_test_app:
	cd test_app && rm -rf tmp/tarantool.log && TARANTOOL_LOG_LEVEL=7 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/

test:
	cargo test

lint:
	cargo fmt --all -- --check
	cargo clippy -- -Dclippy::all -Wclippy::pedantic

build_test_app:
	cd test_app && cartridge build
	cp -rf target/release/$(SRC_LIB) test_app/.rocks/lib/tarantool/$(DEST_LIB)

test_all: test build build_test_app integration_test_app
