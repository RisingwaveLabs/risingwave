#!/usr/bin/env bash

set -e

rm -rf tmp_gen
mkdir tmp_gen
cp -a ../proto/*.proto tmp_gen

# Replace some keywords in JavaScript to avoid conflicts: Array, Object.
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i "" -e "s/Array/RwArray/" "tmp_gen/data.proto"
    sed -i "" -e "s/ Object / RwObject /" "tmp_gen/meta.proto"
else
    sed -i -e "s/Array/RwArray/" "tmp_gen/data.proto"
    sed -i -e "s/ Object / RwObject /" "tmp_gen/meta.proto"
fi

mkdir -p proto/gen

protoc --plugin=./node_modules/.bin/protoc-gen-ts_proto \
    --experimental_allow_proto3_optional \
    --ts_proto_out=proto/gen/ \
    --proto_path=tmp_gen \
    --ts_proto_opt=outputServices=false \
    --ts_proto_opt=oneof=unions \
    --ts_proto_opt=enumsAsLiterals=true \
    --ts_proto_opt=outputEncodeMethods=false \
    --ts_proto_opt=outputClientImpl=false \
    --ts_proto_opt=stringEnums=true \
    tmp_gen/*.proto

rm -rf tmp_gen
