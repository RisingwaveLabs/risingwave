use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let stmt = Query::update()
            .table(Function::Table)
            .value(Function::Identifier, Expr::col(Function::Name))
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
    Language,
}
