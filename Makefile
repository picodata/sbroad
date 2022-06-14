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
build: clean build_release

bench_check:
	MOCK_DEC_NUMBER=1 cargo bench --no-run

build_debug:
	MOCK_DEC_NUMBER=0 cargo build

build_release:
	MOCK_DEC_NUMBER=0 cargo build --release

build_test_app:
	cd test_app && cartridge build
	cp -rf target/release/$(SRC_LIB) test_app/.rocks/lib/tarantool/$(DEST_LIB)

clean:
	rm -rf target/release/libsbroad.*
	rm -rf target/debug/libsbroad.*

integration_test_app:
	cd test_app && rm -rf tmp/tarantool.log && TARANTOOL_LOG_LEVEL=7 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/

init:
	git submodule update --init --recursive --remote

lint:
	cargo fmt --all -- --check
	cargo clippy -- -Dclippy::all -Wclippy::pedantic

test:
	MOCK_DEC_NUMBER=1 cargo test

test_all: clean test clean bench_check build build_test_app integration_test_app
