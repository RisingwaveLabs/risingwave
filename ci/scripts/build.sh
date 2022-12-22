#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

while getopts 't:p:' opt; do
    case ${opt} in
        t )
            target=$OPTARG
            ;;
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

echo "--- Rust cargo-sort check"
cargo sort --check --workspace

echo "--- Rust cargo-hakari check"
cargo hakari generate --diff

echo "--- Rust format check"
cargo fmt --all -- --check

echo "--- Build Rust components"
cargo build \
    -p risingwave_cmd_all \
    -p risedev \
    -p risingwave_regress_test \
    -p risingwave_sqlsmith \
    -p risingwave_compaction_test \
    -p risingwave_backup_cmd \
    --features "static-link static-log-level" --profile "$profile"

artifacts=(risingwave sqlsmith compaction-test backup-restore risingwave_regress_test risedev-dev delete-range-test)

echo "--- Compress debug info for artifacts"
echo "$artifacts" | xargs -I {} -d ' ' -P 8 objcopy --compress-debug-sections=zlib-gnu target/"$target"/{}

echo "--- Show link info"
ldd target/"$target"/risingwave

echo "--- Upload artifacts"
for artifact in "${artifacts[@]}"; do
    cp target/"$target"/"$artifact" "./$artifact-$profile"
    buildkite-agent artifact upload "./$artifact-$profile"
done
