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

build:
	cargo build --release

build_integration:
	cartridge build $(TEST_APP)

run_integration:
	cd $(TEST_APP) && rm -rf tmp/tarantool.log && TARANTOOL_LOG_LEVEL=7 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/ && cd ..

install_release:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(TARGET_ROOT)/release/$(SRC_LIB) $(LIBDIR)/$(DEST_LIB)
	cp -Rf $(CARTRIDGE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CARTRIDGE_MODULE)/cartridge $(LUADIR)

test_integration: 
	$(MAKE) build_integration && $(MAKE) run_integration
