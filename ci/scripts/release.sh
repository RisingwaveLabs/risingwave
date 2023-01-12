#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- Check env"
if [ "${BUILDKITE_SOURCE}" != "schedule" ] && [[ -z "${BINARY_NAME+x}" ]]; then
  exit 0
fi

echo "--- Install rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain $(cat ./rust-toolchain) -y
source "$HOME/.cargo/env"
source ci/scripts/common.env.sh

echo "--- Install protoc3"
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
unzip -o protoc-3.15.8-linux-x86_64.zip -d /usr/local bin/protoc

echo "--- Install lld"
yum install -y centos-release-scl-rh
yum install -y llvm-toolset-7.0-lld
source /opt/rh/llvm-toolset-7.0/enable

echo "--- Install aws cli"
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip && ./aws/install && mv /usr/local/bin/aws /bin/aws

echo "--- Build release binary"
cargo build -p risingwave_cmd_all --features "static-link static-log-level" --profile release
cd target/release && chmod +x risingwave

echo "--- Upload nightly binary to s3"
if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
  tar -czvf risingwave-"$(date '+%Y%m%d')"-x86_64-unknown-linux.tar.gz risingwave
  aws s3 cp risingwave-"$(date '+%Y%m%d')"-x86_64-unknown-linux.tar.gz s3://risingwave-nightly-pre-built-binary
elif [[ -n "${BINARY_NAME+x}" ]]; then
    tar -czvf risingwave-${BINARY_NAME}-x86_64-unknown-linux.tar.gz risingwave
    aws s3 cp risingwave-${BINARY_NAME}-x86_64-unknown-linux.tar.gz s3://risingwave-nightly-pre-built-binary
fi

if [[ -n "${BUILDKITE_TAG+x}" ]]; then
  echo "--- Install gh cli"
  yum install -y dnf
  dnf install -y 'dnf-command(config-manager)'
  dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
  dnf install -y gh

  echo "--- Release create"
  gh release create "${BUILDKITE_TAG}" --notes "release ${BUILDKITE_TAG}" -d -p

  echo "--- Release upload asset"
  tar -czvf risingwave-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz risingwave
  gh release upload "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz
fi




