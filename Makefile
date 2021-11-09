all: build

build:
	cargo build

install_osx:
	cp target/debug/libsbroad.dylib ../kafka-tarantool-loader/.rocks/lib/tarantool/sql_parser.dylib

test:
	cargo test

lint:
	cargo clippy -- -Dclippy::all -Wclippy::pedantic
