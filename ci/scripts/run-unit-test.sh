#!/bin/bash
set -euo pipefail

echo "+++ Run unit tests with coverage"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints 2> >(tee);
NEXTEST_PROFILE=ci cargo nextest run run_sqlsmith_on_frontend --features "failpoints enable_sqlsmith_unit_test" 2> >(tee);

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust
