FROM docker-public.binary.picodata.io/tarantool:2.10.0

ENV PATH=/root/.cargo/bin:${PATH}

RUN curl -L https://tarantool.io/UaooCnt/release/2/installer.sh | bash; \
    yum -y remove git git-*; \
    yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm; \
    yum install -y git; \
    yum -y update && yum -y install -y wget openssl-devel cartridge-cli readline-devel libicu-devel; \
    yum groupinstall -y "Development Tools"; \
    yum clean all; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile default; \
    rustup component add rustfmt; \
    wget https://cmake.org/files/v3.17/cmake-3.17.3.tar.gz; \
    tar zxvf cmake-3.17.3.tar.gz; cd cmake-3.17.3; ./configure; make; make install; \
    cd ..; rm -rf cmake*
