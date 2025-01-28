#############################################################################################
#                                     !!! IMPORTANT !!!                                     #
# If you want to bump CI Docker image, please update BUILD_ENV_VERSION in build-ci-image.sh #
# and corresponding rw-build-env version in docker-compose.yml.                             #
#############################################################################################

FROM ubuntu:24.04 AS risingwave-build-env

ENV LANG en_US.utf8

# Use AWS ubuntu mirror
RUN sed -i 's|http://archive.ubuntu.com/ubuntu|http://us-east-2.ec2.archive.ubuntu.com/ubuntu/|g' /etc/apt/sources.list
RUN apt-get update -yy && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install sudo make build-essential cmake protobuf-compiler curl parallel python3 python3-pip python3-venv software-properties-common psmisc \
    openssl libssl-dev libsasl2-dev libcurl4-openssl-dev pkg-config bash openjdk-17-jdk wget unzip git tmux lld postgresql-client kcat netcat-openbsd mysql-client \
    maven zstd libzstd-dev locales \
    python3.12 python3.12-dev \
    golang perl \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/
ENV PYO3_PYTHON=python3.12

# Install nvm and zx
ENV NVM_DIR /root/.nvm
ENV NODE_VERSION 20.11.1
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash \
  && . $NVM_DIR/nvm.sh \
  && nvm install $NODE_VERSION
ENV PATH $NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH
RUN npm install -g zx

SHELL ["/bin/bash", "-c"]

RUN mkdir -p /risingwave

WORKDIR /risingwave

COPY rust-toolchain rust-toolchain

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

ENV PATH /root/.cargo/bin/:$PATH

RUN rustup show && \
    rustup default `rustup show active-toolchain | awk '{print $1}'` && \
    rustup component add rustfmt llvm-tools-preview clippy && \
    rustup target add wasm32-wasi

RUN curl -sSL "https://github.com/bufbuild/buf/releases/download/v1.29.0/buf-$(uname -s)-$(uname -m).tar.gz" | \
    tar -xvzf - -C /usr/local --strip-components 1

# install python dependencies
RUN pip3 install --break-system-packages pyarrow pytest

# Install poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
# Install rpk
RUN if [ "$(uname -m)" = "amd64" ] || [ "$(uname -m)" = "x86_64" ]; then \
        curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip && \
        unzip rpk-linux-amd64.zip -d ~/.local/bin/ && \
        rm rpk-linux-amd64.zip; \
    else \
        curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-arm64.zip && \
        unzip rpk-linux-arm64.zip -d ~/.local/bin/ && \
        rm rpk-linux-arm64.zip; \
    fi
ENV PATH /root/.local/bin:$PATH

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# install build tools
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall -y --locked cargo-llvm-cov cargo-nextest cargo-sort cargo-cache cargo-machete \
    cargo-make@0.37.9 \
    sqllogictest-bin@0.26.3 \
    sccache@0.7.4 \
    && cargo cache -a \
    && rm -rf "/root/.cargo/registry/index" \
    && rm -rf "/root/.cargo/registry/cache" \
    && rm -rf "/root/.cargo/git/db" \
    && cargo uninstall cargo-cache
RUN cargo install cargo-dylint@3.2.1 dylint-link@3.2.1

# install risedev
COPY <<EOF /root/.cargo/bin/risedev
#!/usr/bin/env bash
set -e
cd "/risingwave"
./risedev "\$@"
EOF
RUN chmod +x /root/.cargo/bin/risedev
