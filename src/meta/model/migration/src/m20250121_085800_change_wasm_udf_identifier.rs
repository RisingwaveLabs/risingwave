use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .rename_column(Function::Identifier, Function::NameInRuntime)
                    .to_owned(),
            )
            .await?;

        let stmt = Query::update()
            .table(Function::Table)
            .value(Function::NameInRuntime, Expr::col(Function::Name))
            .and_where(Expr::col(Function::Language).is_in(vec!["wasm", "rust"]))
            .to_owned();
        manager.exec_stmt(stmt).await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "cannot rollback wasm udf identifier migration".to_owned(),
        ))?
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    Name,
    Identifier,
    NameInRuntime,
    Language,
}
