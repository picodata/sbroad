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
TEST_APP=$(CARTRIDGE_MODULE)/test_app

build_cartridge_engine:
	cargo build -p sbroad-cartridge --release

build_cartridge_engine_debug:
	cargo build -p sbroad-cartridge

build_integration:
	cartridge build $(TEST_APP)

run_integration:
	cd $(TEST_APP) && rm -rf tmp/tarantool.log && TARANTOOL_LOG_LEVEL=5 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/ && cd ..

install_debug:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf ../target/debug/$(SRC_LIB) $(LIBDIR)/$(DEST_LIB)
	cp -Rf src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf cartridge $(LUADIR)

install_release:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(TARGET_ROOT)/release/$(SRC_LIB) $(LIBDIR)/$(DEST_LIB)
	cp -Rf $(CARTRIDGE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CARTRIDGE_MODULE)/cartridge $(LUADIR)

test_integration: 
	$(MAKE) build_integration && $(MAKE) run_integration
