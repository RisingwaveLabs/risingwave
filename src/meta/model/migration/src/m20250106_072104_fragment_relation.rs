use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::*;

use crate::m20230908_072257_init::Object;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts

        manager
            .create_table(
                Table::create()
                    .table(FragmentRelation::Table)
                    .col(
                        ColumnDef::new(FragmentRelation::SourceFragmentId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FragmentRelation::TargetFragmentId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FragmentRelation::DispatcherType)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FragmentRelation::DistKeyIndices)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FragmentRelation::OutputIndices)
                            .json_binary()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(FragmentRelation::SourceFragmentId)
                            .col(FragmentRelation::TargetFragmentId),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_fragment_relation_source_oid")
                            .from(FragmentRelation::Table, FragmentRelation::SourceFragmentId)
                            .to(Fragment::Table, Fragment::FragmentId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_fragment_relation_target_oid")
                            .from(FragmentRelation::Table, FragmentRelation::TargetFragmentId)
                            .to(Fragment::Table, Fragment::FragmentId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts

        manager
            .drop_table(Table::drop().table(FragmentRelation::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum FragmentRelation {
    Table,
    SourceFragmentId,
    TargetFragmentId,
    DispatcherType,
    DistKeyIndices,
    OutputIndices,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
}
