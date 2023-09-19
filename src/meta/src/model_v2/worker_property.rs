//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.2

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "worker_property")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub worker_id: i32,
    pub parallel_unit_ids: Json,
    pub is_streaming: bool,
    pub is_serving: bool,
    pub is_unschedulable: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::worker::Entity",
        from = "Column::WorkerId",
        to = "super::worker::Column::WorkerId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Worker,
}

impl Related<super::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
