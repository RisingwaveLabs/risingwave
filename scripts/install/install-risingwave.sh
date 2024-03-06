#!/bin/sh -e

if [ -z "${OS}" ]; then
  OS=$(uname -s)
fi
if [ -z "${ARCH}" ]; then
  ARCH=$(uname -m)
fi
STATE_STORE_PATH="${HOME}/.risingwave/state_store"
META_STORE_PATH="${HOME}/.risingwave/meta_store"

VERSION="v1.7.0-standalone"
# TODO(kwannoel): re-enable it once we have stable release in latest for single node mode.
#VERSION=$(curl -s https://api.github.com/repos/risingwavelabs/risingwave/releases/latest \
# | grep '.tag_name' \
# | sed -E -n 's/.*(v[0-9]+.[0-9]+.[0-9])\",/\1/p')
HOMEBREW_VERSION="1.7-standalone"

BASE_URL="https://github.com/risingwavelabs/risingwave/releases/download"

if [ "${OS}" = "Linux" ]; then
  if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ]; then
    BASE_ARCHIVE_NAME="risingwave-${VERSION}-x86_64-unknown-linux-all-in-one"
    ARCHIVE_NAME="${BASE_ARCHIVE_NAME}.tar.gz"
    URL="${BASE_URL}/${VERSION}/${ARCHIVE_NAME}"
    USE_BREW=0
  elif [ "${ARCH}" = "arm64" ] || [ "${ARCH}" = "aarch64" ]; then
    BASE_ARCHIVE_NAME="risingwave-${VERSION}-aarch64-unknown-linux-all-in-one"
    ARCHIVE_NAME="${BASE_ARCHIVE_NAME}.tar.gz"
    URL="${BASE_URL}/${VERSION}/${ARCHIVE_NAME}"
    USE_BREW=0
  fi
elif [ "${OS}" = "Darwin" ]; then
  if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ] || [ "${ARCH}" = "aarch64" ] || [ "${ARCH}" = "arm64" ]; then
    USE_BREW=1
  fi
fi

if [ -z "$USE_BREW" ]; then
  echo
  echo "Unsupported OS or Architecture: ${OS}-${ARCH}"
  echo
  echo "Supported OSes: Linux, macOS"
  echo "Supported architectures: x86_64"
  echo
  echo "Please open an issue at <https://github.com/risingwavelabs/risingwave/issues/new/choose>,"
  echo "if you would like to request support for your OS or architecture."
  echo
  exit 1
fi

############# BREW INSTALL
if [ "${USE_BREW}" -eq 1 ]; then
  echo "Installing RisingWave@${HOMEBREW_VERSION} using Homebrew."
  brew tap risingwavelabs/risingwave
  brew install risingwave@${HOMEBREW_VERSION}
  echo
  echo "Successfully installed RisingWave@${HOMEBREW_VERSION} using Homebrew."
  echo
  echo "Run RisingWave:"
  echo
  echo "  risingwave"
  echo
  echo "Start a psql session:"
  echo
  echo "  psql -h localhost -p 4566 -d dev -U root"
  echo
  echo "To start a fresh cluster, you can just delete the data directory contents:"
  echo
  echo "  rm -r ~/.risingwave"
  echo
  echo "To view available options, run:"
  echo
  echo "  risingwave single-node --help"
  echo
  echo
  exit 0
fi

############# BINARY INSTALL
echo
echo "Downloading RisingWave@${VERSION} from ${URL} into ${PWD}."
echo
curl -L "${URL}" | tar -zx || exit 1
chmod +x risingwave
echo
echo "Successfully installed RisingWave@${VERSION} binary."
echo
echo "Run RisingWave:"
echo
echo "  ./risingwave"
echo
echo "Start a psql session:"
echo
echo "  psql -h localhost -p 4566 -d dev -U root"
echo
echo "To start a fresh cluster, you can just delete the data directory contents:"
echo
echo "  rm -r ~/.risingwave"
echo
echo "To view available options, run:"
echo
echo "  risingwave single-node --help"
echo
# TODO(kwannoel): Include link to our docs.