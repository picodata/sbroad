include sbroad-cartridge/Makefile 

all: build

IMAGE_NAME=docker-public.binary.picodata.io/sbroad-builder:0.6.0
TARGET_ROOT=target
CARTRIDGE_MODULE=sbroad-cartridge

build:
	cargo build --release

build_debug:
	cargo build

bench:
	make clean
	cargo bench -p sbroad-benches

bench_check:
	make clean
	cargo bench -p sbroad-benches --no-run

clean:
	rm -rf $(TARGET_ROOT)/release/libsbroad*
	rm -rf $(TARGET_ROOT)/release/build/libsbroad*
	rm -rf $(TARGET_ROOT)/release/deps/libsbroad*
	rm -rf $(TARGET_ROOT)/release/incremental/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/build/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/deps/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/incremental/libsbroad*

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
