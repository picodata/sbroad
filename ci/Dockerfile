FROM docker-public.binary.picodata.io/tarantool:2.10.0

ENV PATH=/usr/local/bin:/root/.cargo/bin:${PATH}
ENV LD_LIBRARY_PATH=/usr/local/lib64:$LD_LIBRARY_PATH

RUN yum -y remove git git-*; \
    yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm; \
    yum install -y git; \
    yum -y update; \
    curl -L https://tarantool.io/UaooCnt/release/2/installer.sh | bash; \
    yum install -y wget openssl-devel cartridge-cli-2.12.2 readline-devel libicu-devel; \
    yum groupinstall -y "Development Tools"; \
    yum clean all; \
    wget https://ftp.gnu.org/gnu/gcc/gcc-4.9.2/gcc-4.9.2.tar.gz; \
    tar -xzf gcc-4.9.2.tar.gz; cd gcc-4.9.2; ./contrib/download_prerequisites; \
    ./configure --enable-checking=release --enable-languages=c,c++ --disable-multilib; \
    make && make install; cd ..; rm -rf  gcc*; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain=1.63.0 -y --profile default; \
    rustup component add rustfmt; \
    cargo install cargo-audit; \
    wget https://cmake.org/files/v3.17/cmake-3.17.3.tar.gz; \
    tar zxvf cmake-3.17.3.tar.gz; cd cmake-3.17.3; ./configure; make; make install; \
    cd ..; rm -rf cmake*
