## Prerequisites
Picodata fork of tarantool and
[MsgPuck](https://github.com/rtsisyk/msgpuck) library installed.

To run tests install python modules from `test-requirements.txt`.
You can do this as in the example below.
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r test-requirements.txt
```

## How to build
Firstly, clone our [fork](https://git.picodata.io/picodata/tarantool) of tarantool and make a build and install of `picodata-support` branch using `CMAKE_INSTALL_PREFIX` as follows:
```bash
git clone https://git.picodata.io/picodata/tarantool
cd tarantool
git checkout picodata-support
git submodule update --init --recursive

mkdir build && cd build
cmake ..										\
	-DCMAKE_INSTALL_PREFIX=~/picodata/install	\
	-DENABLE_DIST=ON							\
	-DBUILD_TESTING=OFF

make install
```

If `MsgPuck` lib was no found, you need to specify where to find
`libmsgpuck.a` and headers via `MSGPUCK_LIBRARY` and `MSGPUCK_INCLUDE_DIR`
variables.

Here is how cmake configuration command changes in this case:
```bash
cmake ..                                              \
	-DCMAKE_INSTALL_PREFIX=~/picodata/install         \
	-DENABLE_DIST=ON                                  \
	-DBUILD_TESTING=OFF                               \
	-DMSGPUCK_LIBRARY=~/install/msgpuck/libmsgpuck.a  \
	-DMSGPUCK_INCLUDE_DIR=~/install/msgpuck/
```

Then build the server as in the following example.
`TARANTOOL_INCLUDE_DIRS` specifies the path to the tarantool public
API headers you have installed, such as `module.h` and etc.

```bash
mkdir build && cd build
cmake .. -DTARANTOOL_INCLUDE_DIRS=~/picodata/install/include/tarantool
make
```

## How to run tests
Tests utilize `test-run` utility that extracts tarantool executable from
`PATH` variable so it should be updated.
```bash
mkdir build && cd build
cmake .. -DTARANTOOL_INCLUDE_DIRS=~/picodata/install/include/tarantool
PATH=~/picodata/install/bin:$PATH make test # or test-verbose
```


> **NOTE**: If tests are failed bacause of missed symbol `mp_*`,
> `msgpuck` library is missed so you should back to *How to build* step
> and configure `MSGPUCK_*` variables.
