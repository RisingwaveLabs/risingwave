use sea_orm::{FromJsonQueryResult, FromQueryResult, Statement};
use sea_orm_migration::prelude::*;
use serde::{Deserialize, Serialize};

use crate::drop_tables;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
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

        fulfill_fragment_relation(manager).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, FragmentRelation);
        Ok(())
    }
}

// Fulfill the FragmentRelation table with data from the Actor and ActorDispatcher tables
async fn fulfill_fragment_relation(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let connection = manager.get_connection();

    let database_backend = connection.get_database_backend();

    let (sql, values) = Query::select()
        .distinct_on([
            FragmentRelation::SourceFragmentId,
            FragmentRelation::TargetFragmentId,
        ])
        .expr_as(
            Expr::col((Actor::Table, Actor::FragmentId)),
            FragmentRelation::SourceFragmentId,
        )
        .expr_as(
            Expr::col((ActorDispatcher::Table, ActorDispatcher::DispatcherId)),
            FragmentRelation::TargetFragmentId,
        )
        .columns([
            (ActorDispatcher::Table, ActorDispatcher::DispatcherType),
            (ActorDispatcher::Table, ActorDispatcher::DistKeyIndices),
            (ActorDispatcher::Table, ActorDispatcher::OutputIndices),
        ])
        .from(Actor::Table)
        .join(
            JoinType::InnerJoin,
            ActorDispatcher::Table,
            Expr::col((Actor::Table, Actor::ActorId))
                .equals((ActorDispatcher::Table, ActorDispatcher::ActorId)),
        )
        .to_owned()
        .build_any(&*database_backend.get_query_builder());

    let rows = connection
        .query_all(Statement::from_sql_and_values(
            database_backend,
            sql,
            values,
        ))
        .await?;

    for row in rows {
        let FragmentRelationEntity {
            source_fragment_id,
            target_fragment_id,
            dispatcher_type,
            dist_key_indices,
            output_indices,
        } = FragmentRelationEntity::from_query_result(&row, "")?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table(FragmentRelation::Table)
                    .columns([
                        FragmentRelation::SourceFragmentId,
                        FragmentRelation::TargetFragmentId,
                        FragmentRelation::DispatcherType,
                        FragmentRelation::DistKeyIndices,
                        FragmentRelation::OutputIndices,
                    ])
                    .values_panic([
                        source_fragment_id.into(),
                        target_fragment_id.into(),
                        dispatcher_type.into(),
                        dist_key_indices.into(),
                        output_indices.into(),
                    ])
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize, Default)]
pub struct I32Array(pub Vec<i32>);

#[derive(Debug, FromQueryResult)]
#[sea_orm(entity = "FragmentRelation")]
pub struct FragmentRelationEntity {
    source_fragment_id: i32,
    target_fragment_id: i32,
    dispatcher_type: String,
    dist_key_indices: I32Array,
    output_indices: I32Array,
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

#[derive(DeriveIden)]
enum Actor {
    Table,
    ActorId,
    FragmentId,
}

#[derive(DeriveIden)]
enum ActorDispatcher {
    Table,
    ActorId,
    DispatcherType,
    DistKeyIndices,
    OutputIndices,
    DispatcherId,
}
