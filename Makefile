all: build

build:
	cargo build

install_osx:
	cp target/debug/libsbrod.dylib ../kafka-tarantool-loader/.rocks/lib/tarantool/sql_parser.dylib

test:
	cargo test