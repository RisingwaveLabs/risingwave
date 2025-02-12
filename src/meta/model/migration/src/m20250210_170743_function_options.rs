use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .add_column(ColumnDef::new(Function::Options).json_binary())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .drop_column(Alias::new(Function::Options.to_string()))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    Options,
}
