use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

pub const DEFAULT_RESOURCE_GROUP: &str = "default";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .add_column(ColumnDef::new(WorkerProperty::ResourceGroup).string())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .add_column(ColumnDef::new(StreamingJob::SpecificResourceGroup).string())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Database::Table)
                    .add_column(
                        ColumnDef::new(Database::ResourceGroup)
                            .string()
                            .default(DEFAULT_RESOURCE_GROUP.to_string())
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .drop_column(WorkerProperty::ResourceGroup)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .drop_column(StreamingJob::SpecificResourceGroup)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Database::Table)
                    .drop_column(Database::ResourceGroup)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum WorkerProperty {
    Table,
    ResourceGroup,
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    SpecificResourceGroup,
}

#[derive(DeriveIden)]
enum Database {
    Table,
    ResourceGroup,
}
