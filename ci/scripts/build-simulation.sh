#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

exit 88

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
risedev sslt-build-all --profile ci-sim --timings

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats

echo "--- Build and archive deterministic simulation integration tests"
NEXTEST_PROFILE=ci-sim risedev sarchive-it-test --cargo-profile ci-sim

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats

echo "--- Upload artifacts"
mv target/sim/ci-sim/risingwave_simulation ./risingwave_simulation
tar --zstd -cvf risingwave_simulation.tar.zst risingwave_simulation
buildkite-agent artifact upload target/sim/cargo-timings/cargo-timing.html

artifacts=(risingwave_simulation.tar.zst simulation-it-test.tar.zst)
echo -n "${artifacts[*]}" | parallel -d ' ' "buildkite-agent artifact upload ./{}"
