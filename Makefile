include sbroad-cartridge/Makefile 

all: build

IMAGE_NAME=docker-public.binary.picodata.io/sbroad-builder:0.6.0
TARGET_ROOT=target
CARTRIDGE_MODULE=sbroad-cartridge

bench:
	make clean
	cargo bench -p sbroad-benches

bench_check:
	make clean
	cargo bench -p sbroad-benches --no-run

build_debug:
	make clean
	cargo build

clean:
	rm -rf $(TARGET_ROOT)/release/build/sbroad-*
	rm -rf $(TARGET_ROOT)/release/deps/sbroad-*
	rm -rf $(TARGET_ROOT)/release/incremental/sbroad-*
	rm -rf $(TARGET_ROOT)/debug/build/sbroad-*
	rm -rf $(TARGET_ROOT)/debug/deps/sbroad-*
	rm -rf $(TARGET_ROOT)/debug/incremental/sbroad-*

lint:
	cargo fmt --all -- --check
	cargo clippy -- -Dclippy::all -Wclippy::pedantic
	cargo audit -f audit.toml
	./deps.sh
	./.rocks/bin/luacheck .

test:
	cargo test --features mock -vv

test_all: test bench_check test_integration

update_ci_image:
	docker build -f ci/Dockerfile -t $(IMAGE_NAME) .
	docker push $(IMAGE_NAME)

release_rock:
	cd $(CARTRIDGE_MODULE) \
	&& echo "Build release ${CI_COMMIT_TAG}" \
	&& tarantoolctl rocks new_version --tag ${CI_COMMIT_TAG} \
	&& tarantoolctl rocks install sbroad-${CI_COMMIT_TAG}-1.rockspec \
	&& tarantoolctl rocks pack sbroad-${CI_COMMIT_TAG}-1.rockspec \
	&& mv sbroad*rock .. \
	&& rm -rf sbroad-${CI_COMMIT_TAG}-1.rockspec
