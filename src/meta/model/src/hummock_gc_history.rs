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

use std::fmt::Formatter;
use sea_orm::entity::prelude::*;
use sea_orm::{DeriveEntityModel, DeriveRelation, EnumIter};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::HummockSstableObjectId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "hummock_gc_history")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub object_id: HummockSstableObjectId,
    pub mark_delete_at: DateTime,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

const FIELDS: [&str; 2] = [
    "_id",
    "mark_delete_at",
];

pub struct MongoDb {
    hummock_gc_history: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.hummock_gc_history.object_id)?;
        state.serialize_field("mark_delete_at", &self.hummock_gc_history.mark_delete_at)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for MongoDb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MongoDbVisitor;
        impl<'de> Visitor<'de> for MongoDbVisitor {
            type Value = MongoDb;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("MongoDb")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut context_id: Option<HummockSstableObjectId> = None;
                let mut mark_delete_at: Option<DateTime> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => context_id = Some(i64::deserialize(value).unwrap()),
                        "mark_delete_at" => mark_delete_at = Some(DateTime::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_gc_history = Model {
                    object_id: context_id.ok_or_else(|| Error::missing_field("_id"))?,
                    mark_delete_at: mark_delete_at.ok_or_else(|| Error::missing_field("mark_delete_at"))?,
                };
                Ok(Self::Value {hummock_gc_history})
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
