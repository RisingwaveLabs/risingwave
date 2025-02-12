#![allow(clippy::enum_variant_names)]

pub use sea_orm_migration::prelude::*;
pub use sea_orm_migration::MigrationStatus;
mod m20230908_072257_init;
mod m20231008_020431_hummock;
mod m20240304_074901_subscription;
mod m20240410_082733_with_version_column_migration;
mod m20240410_154406_session_params;
mod m20240417_062305_subscription_internal_table_name;
mod m20240418_142249_function_runtime;
mod m20240506_112555_subscription_partial_ckpt;
mod m20240525_090457_secret;
mod m20240617_070131_index_column_properties;
mod m20240617_071625_sink_into_table_column;
mod m20240618_072634_function_compressed_binary;
mod m20240630_131430_remove_parallel_unit;
mod m20240701_060504_hummock_time_travel;
mod m20240702_080451_system_param_value;
mod m20240702_084927_unnecessary_fk;
mod m20240726_063833_auto_schema_change;
mod m20240806_143329_add_rate_limit_to_source_catalog;
mod m20240820_081248_add_time_travel_per_table_epoch;
mod m20240911_083152_variable_vnode_count;
mod m20241001_013810_webhook_source;
mod m20241016_065621_hummock_gc_history;
mod m20241022_072553_node_label;
mod m20241025_062548_singleton_vnode_count;
mod m20241115_085007_remove_function_type;
mod m20241120_182555_hummock_add_time_travel_sst_index;
mod m20241121_101830_table_engine;
mod m20241125_043732_connection_params;
mod m20241202_071413_resource_group;
mod m20241226_074013_clean_watermark_index_in_pk;
mod m20250210_170743_function_options;
mod utils;

pub struct Migrator;

#[macro_export]
macro_rules! assert_not_has_tables {
    ($manager:expr, $( $table:ident ),+) => {
        $(
            assert!(
                !$manager
                    .has_table($table::Table.to_string())
                    .await?,
                "Table `{}` already exists",
                $table::Table.to_string()
            );
        )+
    };
}

#[macro_export]
macro_rules! drop_tables {
    ($manager:expr, $( $table:ident ),+) => {
        $(
            $manager
                .drop_table(
                    sea_orm_migration::prelude::Table::drop()
                        .table($table::Table)
                        .if_exists()
                        .cascade()
                        .to_owned(),
                )
                .await?;
        )+
    };
}

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230908_072257_init::Migration),
            Box::new(m20231008_020431_hummock::Migration),
            Box::new(m20240304_074901_subscription::Migration),
            Box::new(m20240410_082733_with_version_column_migration::Migration),
            Box::new(m20240410_154406_session_params::Migration),
            Box::new(m20240417_062305_subscription_internal_table_name::Migration),
            Box::new(m20240418_142249_function_runtime::Migration),
            Box::new(m20240506_112555_subscription_partial_ckpt::Migration),
            Box::new(m20240525_090457_secret::Migration),
            Box::new(m20240617_070131_index_column_properties::Migration),
            Box::new(m20240617_071625_sink_into_table_column::Migration),
            Box::new(m20240618_072634_function_compressed_binary::Migration),
            Box::new(m20240630_131430_remove_parallel_unit::Migration),
            Box::new(m20240701_060504_hummock_time_travel::Migration),
            Box::new(m20240702_080451_system_param_value::Migration),
            Box::new(m20240702_084927_unnecessary_fk::Migration),
            Box::new(m20240726_063833_auto_schema_change::Migration),
            Box::new(m20240806_143329_add_rate_limit_to_source_catalog::Migration),
            Box::new(m20240820_081248_add_time_travel_per_table_epoch::Migration),
            Box::new(m20240911_083152_variable_vnode_count::Migration),
            Box::new(m20241001_013810_webhook_source::Migration),
            Box::new(m20241016_065621_hummock_gc_history::Migration),
            Box::new(m20241022_072553_node_label::Migration),
            Box::new(m20241025_062548_singleton_vnode_count::Migration),
            Box::new(m20241115_085007_remove_function_type::Migration),
            Box::new(m20241120_182555_hummock_add_time_travel_sst_index::Migration),
            Box::new(m20241125_043732_connection_params::Migration),
            Box::new(m20241121_101830_table_engine::Migration),
            Box::new(m20241202_071413_resource_group::Migration),
            Box::new(m20241226_074013_clean_watermark_index_in_pk::Migration),
            Box::new(m20250210_170743_function_options::Migration),
        ]
    }
}
