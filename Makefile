all: build

OS := $(shell uname -s)
ifeq ($(OS), Linux)
	SRC_LIB = libsbroad.so
else
	ifeq ($(OS), Darwin)
		SRC_LIB = libsbroad.dylib
	endif
endif
IMAGE_NAME = docker-public.binary.picodata.io/sbroad-builder:0.6.0

bench:
	make clean
	cargo bench -p sbroad-benches

bench_check:
	make clean
	cargo bench -p sbroad-benches --no-run

build:
	make clean
	cargo build --release

build_debug:
	make clean
	cargo build

build_integration:
	cd test_app && cartridge build

clean:
	rm -rf target/release/build/sbroad-* 
	rm -rf target/release/deps/sbroad-* 
	rm -rf target/release/incremental/sbroad-* 
	rm -rf target/debug/build/sbroad-* 
	rm -rf target/debug/deps/sbroad-* 
	rm -rf target/debug/incremental/sbroad-* 

install_release:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf target/release/$(SRC_LIB) $(LIBDIR)
	cp -Rf src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf cartridge $(LUADIR)

lint:
	cargo fmt --all -- --check
	cargo clippy -- -Dclippy::all -Wclippy::pedantic
	cargo audit -f audit.toml
	./deps.sh
	./.rocks/bin/luacheck .

run_integration:
	cd test_app && rm -rf tmp/tarantool.log && TARANTOOL_LOG_LEVEL=7 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/

test:
	make clean
	cargo test --features mock -vv

test_integration:
	make build_integration
	make run_integration

test_all: test bench_check test_integration

update_ci_image:
	docker build -f ci/Dockerfile -t $(IMAGE_NAME) .
	docker push $(IMAGE_NAME)
