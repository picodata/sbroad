FROM tarantool/tarantool:2.x-centos7

ENV PATH=/root/.cargo/bin:${PATH}

RUN curl -L https://tarantool.io/UaooCnt/release/2.8/installer.sh | bash; \
    yum -y update && yum -y install -y wget openssl-devel cartridge-cli readline-devel; \
    yum groupinstall -y "Development Tools"; \
    yum clean all; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile default; \
    rustup component add rustfmt; \
    wget https://cmake.org/files/v3.17/cmake-3.17.3.tar.gz; \
    tar zxvf cmake-3.17.3.tar.gz; cd cmake-3.17.3; ./configure; make; make install; \
    cd ..; rm -rf cmake*
