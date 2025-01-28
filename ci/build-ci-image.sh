#!/usr/bin/env bash

set -euo pipefail

export DOCKER_BUILDKIT=1
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

cat ../rust-toolchain
# shellcheck disable=SC2155

# REMEMBER TO ALSO UPDATE ci/docker-compose.yml
export BUILD_ENV_VERSION=v20250124

export BUILD_TAG="public.ecr.aws/w1p7b4n3/rw-build-env:${BUILD_ENV_VERSION}"

echo "+++ Arch"
arch

echo "--- Check docker-compose"
set +e
if ! grep "${BUILD_TAG}" docker-compose.yml; then
    echo "${BUILD_TAG} is not set up for docker-compose, please modify docker-compose.yml."
    exit 1
fi
set -e

echo "--- Docker login"
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/w1p7b4n3

echo "--- Check image existence"
set +e
# remove all local images to ensure we fetch remote images
docker image rm ${BUILD_TAG}
# check manifest
if docker manifest inspect "${BUILD_TAG}"; then
    echo "+++ Image already exists"
    echo "${BUILD_TAG} already exists -- skipping build image"
    exit 0
fi
set -ex

echo "--- Docker build"
if [[ -z ${BUILDKITE} ]]; then
    export DOCKER_BUILD_PROGRESS="--progress=auto"
else
    export DOCKER_BUILD_PROGRESS="--progress=plain"
fi

docker build -t ${BUILD_TAG} ${DOCKER_BUILD_PROGRESS} --no-cache .

echo "--- Docker push"
docker push ${BUILD_TAG}
