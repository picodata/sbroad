include sbroad-cartridge/Makefile

all: build

IMAGE_NAME=docker-public.binary.picodata.io/sbroad-builder:0.7.0
TARGET_ROOT=target
CARTRIDGE_MODULE=sbroad-cartridge

build:
	cargo build --release

build_debug:
	cargo build

bench:
	make clean
	cargo bench --features mock

bench_check:
	make clean
	cargo bench --features mock --no-run

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
	RUSTFLAGS="-D warnings" cargo clippy -- -Dclippy::all -Wclippy::pedantic
	RUSTDOCFLAGS="-D warnings" cargo doc
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

stress:
	test=$(test) docker-compose -f docker-compose.yml down
	test=$(test) docker-compose -f docker-compose.yml up --abort-on-container-exit --exit-code-from k6
	test=$(test) docker-compose -f docker-compose.yml down

stress_all:
	$(MAKE) stress test=projection
	$(MAKE) stress test=projection_wide
	$(MAKE) stress test=insert
	$(MAKE) stress test=groupby

