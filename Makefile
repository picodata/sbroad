all: build

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

clean:
	rm -rf target/release/build/sbroad-* 
	rm -rf target/release/deps/sbroad-* 
	rm -rf target/release/incremental/sbroad-* 
	rm -rf target/debug/build/sbroad-* 
	rm -rf target/debug/deps/sbroad-* 
	rm -rf target/debug/incremental/sbroad-*

lint:
	cargo fmt --all -- --check
	cargo clippy -- -Dclippy::all -Wclippy::pedantic
	cargo audit -f audit.toml
	./deps.sh
	./.rocks/bin/luacheck .

test:
	cargo test --features mock -vv

test_integration:
	cd sbroad-cartridge && $(MAKE) test_integration

test_all: test bench_check test_integration

update_ci_image:
	docker build -f ci/Dockerfile -t $(IMAGE_NAME) .
	docker push $(IMAGE_NAME)
