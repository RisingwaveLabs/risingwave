// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::{Path, PathBuf};

const DEBUG: bool = false;

macro_rules! debug {
    ($($tokens: tt)*) => {
        if DEBUG {
            println!("cargo:warning={}", format!($($tokens)*))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={}", proto_dir);

    let proto_files = vec![
        "backup_service",
        "batch_plan",
        "catalog",
        "cloud_service",
        "common",
        "compactor",
        "compute",
        "connector_service",
        "data",
        "ddl_service",
        "expr",
        "health",
        "hummock",
        "java_binding",
        "meta",
        "monitor_service",
        "plan_common",
        "source",
        "stream_plan",
        "stream_service",
        "task_service",
        "telemetry",
        "user",
        "secret",
        "frontend_service",
    ];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}.proto", proto_dir, f))
        .collect();

    // Paths to generate `BTreeMap` for protobuf maps.
    let btree_map_paths = [
        ".monitor_service.StackTraceResponse",
        ".plan_common.ExternalTableDesc",
        ".hummock.CompactTask",
        ".catalog.StreamSourceInfo",
        ".secret.SecretRef",
        ".catalog.Source",
        ".catalog.Sink",
        ".catalog.View",
        ".catalog.SinkFormatDesc",
        ".connector_service.ValidateSourceRequest",
        ".connector_service.GetEventStreamRequest",
        ".connector_service.SinkParam",
        ".stream_plan.SinkDesc",
        ".stream_plan.StreamFsFetch",
        ".stream_plan.SourceBackfillNode",
        ".stream_plan.StreamSource",
        ".batch_plan.SourceNode",
        ".batch_plan.IcebergScanNode",
    ];

    // Build protobuf structs.

    // We first put generated files to `OUT_DIR`, then copy them to `/src` only if they are changed.
    // This is to avoid unexpected recompilation due to the timestamps of generated files to be
    // changed by different kinds of builds. See https://github.com/risingwavelabs/risingwave/issues/11449 for details.
    //
    // Put generated files in /src may also benefit IDEs https://github.com/risingwavelabs/risingwave/pull/2581
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR envvar is missing"));
    let file_descriptor_set_path: PathBuf = out_dir.join("file_descriptor_set.bin");

    let tonic_config = tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_set_path.as_path())
        .compile_well_known_types(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(prost_helpers::AnyPB)]")
        .type_attribute(
            "node_body",
            "#[derive(::enum_as_inner::EnumAsInner, ::strum::Display, ::strum::EnumDiscriminants)]",
        )
        .type_attribute("rex_node", "#[derive(::enum_as_inner::EnumAsInner)]")
        .type_attribute(
            "meta.PausedReason",
            "#[derive(::enum_as_inner::EnumAsInner)]",
        )
        .type_attribute(
            "stream_plan.Barrier.BarrierKind",
            "#[derive(::enum_as_inner::EnumAsInner)]",
        )
        .btree_map(btree_map_paths)
        // node body is a very large enum, so we box it to avoid stack overflow.
        // TODO: ideally we should box all enum variants automatically https://github.com/tokio-rs/prost/issues/1209
        .boxed(".stream_plan.StreamNode.node_body.source")
        .boxed(".stream_plan.StreamNode.node_body.project")
        .boxed(".stream_plan.StreamNode.node_body.filter")
        .boxed(".stream_plan.StreamNode.node_body.materialize")
        .boxed(".stream_plan.StreamNode.node_body.stateless_simple_agg")
        .boxed(".stream_plan.StreamNode.node_body.simple_agg")
        .boxed(".stream_plan.StreamNode.node_body.hash_agg")
        .boxed(".stream_plan.StreamNode.node_body.append_only_top_n")
        .boxed(".stream_plan.StreamNode.node_body.hash_join")
        .boxed(".stream_plan.StreamNode.node_body.top_n")
        .boxed(".stream_plan.StreamNode.node_body.hop_window")
        .boxed(".stream_plan.StreamNode.node_body.merge")
        .boxed(".stream_plan.StreamNode.node_body.exchange")
        .boxed(".stream_plan.StreamNode.node_body.stream_scan")
        .boxed(".stream_plan.StreamNode.node_body.batch_plan")
        .boxed(".stream_plan.StreamNode.node_body.lookup")
        .boxed(".stream_plan.StreamNode.node_body.arrange")
        .boxed(".stream_plan.StreamNode.node_body.lookup_union")
        .boxed(".stream_plan.StreamNode.node_body.delta_index_join")
        .boxed(".stream_plan.StreamNode.node_body.sink")
        .boxed(".stream_plan.StreamNode.node_body.expand")
        .boxed(".stream_plan.StreamNode.node_body.dynamic_filter")
        .boxed(".stream_plan.StreamNode.node_body.project_set")
        .boxed(".stream_plan.StreamNode.node_body.group_top_n")
        .boxed(".stream_plan.StreamNode.node_body.sort")
        .boxed(".stream_plan.StreamNode.node_body.watermark_filter")
        .boxed(".stream_plan.StreamNode.node_body.dml")
        .boxed(".stream_plan.StreamNode.node_body.row_id_gen")
        .boxed(".stream_plan.StreamNode.node_body.now")
        .boxed(".stream_plan.StreamNode.node_body.append_only_group_top_n")
        .boxed(".stream_plan.StreamNode.node_body.temporal_join")
        .boxed(".stream_plan.StreamNode.node_body.barrier_recv")
        .boxed(".stream_plan.StreamNode.node_body.values")
        .boxed(".stream_plan.StreamNode.node_body.append_only_dedup")
        .boxed(".stream_plan.StreamNode.node_body.eowc_over_window")
        .boxed(".stream_plan.StreamNode.node_body.over_window")
        .boxed(".stream_plan.StreamNode.node_body.stream_fs_fetch")
        .boxed(".stream_plan.StreamNode.node_body.stream_cdc_scan")
        .boxed(".stream_plan.StreamNode.node_body.cdc_filter")
        .boxed(".stream_plan.StreamNode.node_body.source_backfill")
        .boxed(".stream_plan.StreamNode.node_body.changelog")
        .boxed(".stream_plan.StreamNode.node_body.local_approx_percentile")
        .boxed(".stream_plan.StreamNode.node_body.global_approx_percentile")
        .boxed(".stream_plan.StreamNode.node_body.row_merge")
        .boxed(".stream_plan.StreamNode.node_body.as_of_join")
        .boxed(".stream_plan.StreamNode.node_body.sync_log_store")
        // `Udf` is 248 bytes, while 2nd largest field is 32 bytes.
        .boxed(".expr.ExprNode.rex_node.udf")
        // Eq + Hash are for plan nodes to do common sub-plan detection.
        // The requirement is from Source node -> SourceCatalog -> WatermarkDesc -> expr
        .type_attribute("catalog.WatermarkDesc", "#[derive(Eq, Hash)]")
        .type_attribute("catalog.StreamSourceInfo", "#[derive(Eq, Hash)]")
        .type_attribute("catalog.WebhookSourceInfo", "#[derive(Eq, Hash)]")
        .type_attribute("secret.SecretRef", "#[derive(Eq, Hash)]")
        .type_attribute("catalog.IndexColumnProperties", "#[derive(Eq, Hash)]")
        .type_attribute("expr.ExprNode", "#[derive(Eq, Hash)]")
        .type_attribute("data.DataType", "#[derive(Eq, Hash)]")
        .type_attribute("expr.ExprNode.rex_node", "#[derive(Eq, Hash)]")
        .type_attribute("expr.ExprNode.NowRexNode", "#[derive(Eq, Hash)]")
        .type_attribute("expr.InputRef", "#[derive(Eq, Hash)]")
        .type_attribute("expr.UserDefinedFunctionMetadata", "#[derive(Eq, Hash)]")
        .type_attribute("data.Datum", "#[derive(Eq, Hash)]")
        .type_attribute("expr.FunctionCall", "#[derive(Eq, Hash)]")
        .type_attribute("expr.UserDefinedFunction", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.ColumnDesc.generated_or_default_column",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.GeneratedColumnDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.DefaultColumnDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.Cardinality", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.ExternalTableDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.ColumnDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumn", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalColumn.column_type",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AdditionalColumnNormal", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumnKey", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalColumnPartition",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AdditionalColumnPayload", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalColumnTimestamp",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute(
            "plan_common.AdditionalColumnFilename",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AdditionalColumnHeader", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumnHeaders", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumnOffset", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalDatabaseName", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalSchemaName", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalTableName", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalSubject", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalCollectionName",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AsOfJoinDesc", "#[derive(Eq, Hash)]")
        .type_attribute("common.ColumnOrder", "#[derive(Eq, Hash)]")
        .type_attribute("common.OrderType", "#[derive(Eq, Hash)]")
        .type_attribute("common.Buffer", "#[derive(Eq)]")
        // Eq is required to derive `FromJsonQueryResult` for models in risingwave_meta_model.
        .type_attribute("hummock.TableStats", "#[derive(Eq)]")
        .type_attribute("hummock.SstableInfo", "#[derive(Eq)]")
        .type_attribute("hummock.KeyRange", "#[derive(Eq)]")
        .type_attribute("hummock.CompactionConfig", "#[derive(Eq)]")
        .type_attribute("hummock.GroupDelta.delta_type", "#[derive(Eq)]")
        .type_attribute("hummock.IntraLevelDelta", "#[derive(Eq)]")
        .type_attribute("hummock.GroupConstruct", "#[derive(Eq)]")
        .type_attribute("hummock.GroupDestroy", "#[derive(Eq)]")
        .type_attribute("hummock.GroupMetaChange", "#[derive(Eq)]")
        .type_attribute("hummock.GroupTableChange", "#[derive(Eq)]")
        .type_attribute("hummock.GroupMerge", "#[derive(Eq)]")
        .type_attribute("hummock.GroupDelta", "#[derive(Eq)]")
        .type_attribute("hummock.NewL0SubLevel", "#[derive(Eq)]")
        .type_attribute("hummock.LevelHandler.RunningCompactTask", "#[derive(Eq)]")
        .type_attribute("hummock.LevelHandler", "#[derive(Eq)]")
        .type_attribute("hummock.TableOption", "#[derive(Eq)]")
        .type_attribute("hummock.InputLevel", "#[derive(Eq)]")
        .type_attribute("hummock.TableSchema", "#[derive(Eq)]")
        .type_attribute("hummock.CompactTask", "#[derive(Eq)]")
        .type_attribute("hummock.TableWatermarks", "#[derive(Eq)]")
        .type_attribute("hummock.VnodeWatermark", "#[derive(Eq)]")
        .type_attribute(
            "hummock.TableWatermarks.EpochNewWatermarks",
            "#[derive(Eq)]",
        )
        // proto version enums
        .type_attribute("stream_plan.AggNodeVersion", "#[derive(prost_helpers::Version)]")
        .type_attribute(
            "plan_common.ColumnDescVersion",
            "#[derive(prost_helpers::Version)]",
        )
        .type_attribute(
            "hummock.CompatibilityVersion",
            "#[derive(prost_helpers::Version)]",
        )
        // end
        ;

    // If any configuration for `prost_build` is not exposed by `tonic_build`, specify it here.
    let mut prost_config = prost_build::Config::new();
    prost_config.skip_debug([
        "meta.SystemParams",
        "plan_common.ColumnDesc",
        "data.DataType",
        // TODO:
        //"stream_plan.StreamNode"
    ]);
    // Compile the proto files.
    tonic_config
        .out_dir(out_dir.as_path())
        .compile_with_config(prost_config, &protos, &[proto_dir.to_owned()])
        .expect("Failed to compile grpc!");

    // Implement `serde::Serialize` on those structs.
    let descriptor_set = fs_err::read(file_descriptor_set_path)?;
    pbjson_build::Builder::new()
        .btree_map(btree_map_paths)
        .register_descriptors(&descriptor_set)?
        .out_dir(out_dir.as_path())
        .build(&["."])
        .expect("Failed to compile serde");

    // Tweak the serde files so that they can be compiled in our project.
    // By adding a `use crate::module::*`
    let rewrite_files = proto_files;
    for serde_proto_file in &rewrite_files {
        let out_file = out_dir.join(format!("{}.serde.rs", serde_proto_file));
        let file_content = String::from_utf8(fs_err::read(&out_file)?)?;
        let module_path_id = serde_proto_file.replace('.', "::");
        fs_err::write(
            &out_file,
            format!("use crate::{}::*;\n{}", module_path_id, file_content),
        )?;
    }

    compare_and_copy(&out_dir, &PathBuf::from("./src")).unwrap_or_else(|_| {
        panic!(
            "Failed to copy generated files from {} to ./src",
            out_dir.display()
        )
    });

    Ok(())
}

/// Copy all files from `src_dir` to `dst_dir` only if they are changed.
fn compare_and_copy(src_dir: &Path, dst_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    debug!(
        "copying files from {} to {}",
        src_dir.display(),
        dst_dir.display()
    );
    let mut updated = false;
    let t1 = std::time::Instant::now();
    for entry in walkdir::WalkDir::new(src_dir) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let src_file = entry.path();
            let dst_file = dst_dir.join(src_file.strip_prefix(src_dir)?);
            if dst_file.exists() {
                let src_content = fs_err::read(src_file)?;
                let dst_content = fs_err::read(dst_file.as_path())?;
                if src_content == dst_content {
                    continue;
                }
            }
            updated = true;
            debug!("copying {} to {}", src_file.display(), dst_file.display());
            fs_err::create_dir_all(dst_file.parent().unwrap())?;
            fs_err::copy(src_file, dst_file)?;
        }
    }
    debug!(
        "Finished generating risingwave_prost in {:?}{}",
        t1.elapsed(),
        if updated { "" } else { ", no file is updated" }
    );

    Ok(())
}
