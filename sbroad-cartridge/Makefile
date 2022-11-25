OS := $(shell uname -s)
ifeq ($(OS), Linux)
	SRC_LIB = libsbroad_cartridge.so
	DEST_LIB = libsbroad.so
else
	ifeq ($(OS), Darwin)
		SRC_LIB = libsbroad_cartridge.dylib
		DEST_LIB = libsbroad.dylib
	endif
endif

TARGET_ROOT?=../target
CARTRIDGE_MODULE?=.
CORE_MODULE=$(CARTRIDGE_MODULE)/../sbroad-core
TEST_APP=$(CARTRIDGE_MODULE)/test_app

build_cartridge_engine:
	cargo build -p sbroad-cartridge --release

build_cartridge_engine_debug:
	cargo build -p sbroad-cartridge

build_integration:
	cartridge build $(TEST_APP)

test_wo_replication:
	cd $(TEST_APP) && rm -rf tmp/tarantool_integration.log && TARANTOOL_LOG_LEVEL=5 TARANTOOL_LOG=tmp/tarantool_integration.log ./.rocks/bin/luatest --coverage -v test/integration/ && cd ..

test_w_replication:
	cd $(TEST_APP) && rm -rf tmp/tarantool_configuration.log && TARANTOOL_LOG_LEVEL=5 TARANTOOL_LOG=tmp/tarantool_configuration.log ./.rocks/bin/luatest --coverage -v test/configuration/ && cd ..

run_integration:
	@$(MAKE) test_wo_replication;\
	EXIT_CODE_1=$$?;\
	$(MAKE) test_w_replication;\
	EXIT_CODE_2=$$?;\
	exit $$EXIT_CODE_1 && $$EXIT_CODE_2

install_debug:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(TARGET_ROOT)/debug/$(SRC_LIB) $(LIBDIR)/$(DEST_LIB)
	cp -Rf $(CARTRIDGE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CORE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CARTRIDGE_MODULE)/cartridge $(LUADIR)

install_release:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(TARGET_ROOT)/release/$(SRC_LIB) $(LIBDIR)/$(DEST_LIB)
	cp -Rf $(CARTRIDGE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CORE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CARTRIDGE_MODULE)/cartridge $(LUADIR)

test_integration:
	$(MAKE) build_integration && $(MAKE) run_integration
