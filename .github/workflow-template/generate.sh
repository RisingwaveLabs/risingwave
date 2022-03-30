#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

# You will need to install yq >= 4.16 to use this tool.
# brew install yq

HEADER="""
# ================= THIS FILE IS AUTOMATICALLY GENERATED =================
#
# To edit this file, please refer to the instructions in CONTRIBUTING.md.
#
# ========================================================================
"""

# Repalce $[<profile>] -> dev/release; $[<runner>] -> a/c; $[<target>] -> debug/release

sed 's/\$\[<profile>\]/dev/g;s/\$\[<runner>\]/a/g;s/\$\[<target>\]/debug/g' jobs/e2e-risedev.yml > jobs/e2e-risedev-dev.gen.yml
sed 's/\$\[<profile>\]/release/g;s/\$\[<runner>\]/c/g;s/\$\[<target>\]/release/g' jobs/e2e-risedev.yml > jobs/e2e-risedev-release.gen.yml
sed 's/\$\[<profile>\]/dev/g;s/\$\[<runner>\]/a/g;s/\$\[<target>\]/debug/g' jobs/compute-node-build.yml > jobs/compute-node-build-dev.gen.yml
sed 's/\$\[<profile>\]/release/g;s/\$\[<runner>\]/c/g;s/\$\[<target>\]/release/g' jobs/compute-node-build.yml > jobs/compute-node-build-release.gen.yml

# Generate workflow for main branch

jobs_main=(
   "jobs/frontend-check.yml"
   "jobs/e2e-risedev-dev.gen.yml"
   "jobs/e2e-risedev-release.gen.yml"
   "jobs/e2e-source.yml"
   "jobs/compute-node-build-dev.gen.yml"
   "jobs/compute-node-build-release.gen.yml"
   "jobs/compute-node-test.yml"
   "jobs/misc-check.yml"
)

echo "$HEADER" > ../workflows/main.yml
# shellcheck disable=SC2016
yq ea '. as $item ireduce ({}; . * $item )' template.yml main-override.yml "${jobs_main[@]}" | yq eval '... comments=""' - >> ../workflows/main.yml
echo "$HEADER" >> ../workflows/main.yml

# Generate workflow for pull requests

jobs_pr=(
   "jobs/frontend-check.yml"
   "jobs/e2e-risedev-dev.gen.yml"
   "jobs/e2e-risedev-release.gen.yml"
   "jobs/e2e-source.yml"
   "jobs/compute-node-build-dev.gen.yml"
   "jobs/compute-node-build-release.gen.yml"
   "jobs/compute-node-test.yml"
   "jobs/misc-check.yml"
)

echo "$HEADER" > ../workflows/pull-request.yml
# shellcheck disable=SC2016
yq ea '. as $item ireduce ({}; . * $item )' template.yml pr-override.yml "${jobs_pr[@]}" | yq eval '... comments=""' - >> ../workflows/pull-request.yml
echo "$HEADER" >> ../workflows/pull-request.yml

rm jobs/*.gen.yml

if [ "$1" == "--check" ] ; then
 if ! git diff --exit-code; then
    echo "Please run $(tput setaf 4)./risedev apply-ci-template$(tput sgr0) and commit after editing the workflow templates. Refer to CONTRIBUTING.md for more information."
    exit 1
 fi
fi
