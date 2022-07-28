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

bench:
	make clean
	cargo bench --features mock

bench_check:
	make clean
	cargo bench --no-run --features mock

build:
	make clean
	cargo build --release

build_debug:
	make clean
	cargo build

build_integration:
	make build
	cd test_app && cartridge build
	mkdir -p test_app/.rocks/lib/tarantool
	cp -rf target/release/$(SRC_LIB) test_app/.rocks/lib/tarantool/$(DEST_LIB)

clean:
	rm -rf target/release/libsbroad.*
	rm -rf target/release/build/sbroad-* 
	rm -rf target/release/deps/sbroad-* 
	rm -rf target/release/incremental/sbroad-* 
	rm -rf target/debug/libsbroad.*
	rm -rf target/debug/build/sbroad-* 
	rm -rf target/debug/deps/sbroad-* 
	rm -rf target/debug/incremental/sbroad-* 

init:
	git clean -xfd
	git submodule foreach --recursive git clean -xfd
	git reset --hard
	git submodule foreach --recursive git reset --hard
	git submodule sync --recursive
	git submodule update --init --recursive

lint:
	cargo fmt --all -- --check
	cargo clippy -- -Dclippy::all -Wclippy::pedantic

run_integration:
	cd test_app && rm -rf tmp/tarantool.log && TARANTOOL_LOG_LEVEL=7 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/

test:
	make clean
	MOCK_DEC_NUMBER=1 cargo test --features mock

test_integration:
	make build_integration
	make run_integration

test_all: test bench_check test_integration
