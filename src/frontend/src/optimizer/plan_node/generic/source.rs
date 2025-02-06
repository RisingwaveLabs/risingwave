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

use std::rc::Rc;

use educe::Educe;
use risingwave_common::catalog::{ColumnCatalog, Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::WithPropertiesExt;
use risingwave_sqlparser::ast::AsOf;

use super::super::utils::TableCatalogBuilder;
use super::GenericPlanNode;
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::TableCatalog;

/// In which scnario the source node is created
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[expect(clippy::enum_variant_names)]
pub enum SourceNodeKind {
    /// `CREATE TABLE` with a connector.
    CreateTable,
    /// `CREATE SOURCE` with a streaming job (shared source).
    CreateSharedSource,
    /// `CREATE MATERIALIZED VIEW` or batch scan from a source.
    ///
    /// Note:
    /// - For non-shared source, `CREATE SOURCE` will not create a source node, and `CREATE MATERIALIZE VIEW` will create a `StreamSource`.
    /// - For shared source, `CREATE MATERIALIZE VIEW` will create `StreamSourceScan` instead of `StreamSource`.
    CreateMViewOrBatch,
}

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Source {
    /// If there is an external stream source, `catalog` will be `Some`. Otherwise, it is `None`.
    pub catalog: Option<Rc<SourceCatalog>>,

    // NOTE: Here we store `column_catalog` and `row_id_index`
    // because they are needed when `catalog` is None.
    // When `catalog` is Some, they are the same as these fields in `catalog`.
    pub column_catalog: Vec<ColumnCatalog>,
    pub row_id_index: Option<usize>,

    pub kind: SourceNodeKind,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,

    pub as_of: Option<AsOf>,
}

impl GenericPlanNode for Source {
    fn schema(&self) -> Schema {
        let fields = self
            .column_catalog
            .iter()
            .map(|c| (&c.column_desc).into())
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        // FIXME: output col idx is not set. But iceberg source can prune cols.
        // XXX: there's a RISINGWAVE_ICEBERG_ROW_ID. Should we use it?
        self.row_id_index.map(|idx| vec![idx])
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.stream_key();
        match pk_indices {
            Some(pk_indices) => {
                FunctionalDependencySet::with_key(self.column_catalog.len(), &pk_indices)
            }
            None => FunctionalDependencySet::new(self.column_catalog.len()),
        }
    }
}

impl Source {
    pub fn is_new_fs_connector(&self) -> bool {
        self.catalog
            .as_ref()
            .is_some_and(|catalog| catalog.with_properties.is_new_fs_connector())
    }

    pub fn is_iceberg_connector(&self) -> bool {
        self.catalog
            .as_ref()
            .is_some_and(|catalog| catalog.with_properties.is_iceberg_connector())
    }

    pub fn is_kafka_connector(&self) -> bool {
        self.catalog
            .as_ref()
            .is_some_and(|catalog| catalog.with_properties.is_kafka_connector())
    }

    /// Currently, only iceberg source supports time travel.
    pub fn support_time_travel(&self) -> bool {
        self.is_iceberg_connector()
    }

    /// The columns in stream/batch source node indicate the actual columns it will produce,
    /// instead of the columns defined in source catalog. The difference is generated columns.
    pub fn exclude_generated_columns(mut self) -> (Self, Option<usize>) {
        let original_row_id_index = self.row_id_index;
        // minus the number of generated columns before row_id_index.
        self.row_id_index = original_row_id_index.map(|idx| {
            let mut cnt = 0;
            for col in self.column_catalog.iter().take(idx + 1) {
                if col.is_generated() {
                    cnt += 1;
                }
            }
            idx - cnt
        });
        self.column_catalog.retain(|c| !c.is_generated());
        (self, original_row_id_index)
    }

    /// Source's state table is `partition_id -> offset_info`.
    /// Its schema is irrelevant to the data's schema.
    ///
    /// ## Notes on the distribution of the state table (`is_distributed`)
    ///
    /// Source executors are always distributed, but their state tables are special.
    ///
    /// ### `StreamSourceExecutor`: singleton (only one vnode)
    ///
    /// Its states are not sharded by consistent hash.
    ///
    /// Each actor accesses (point get) some partitions (a.k.a splits).
    /// They are assigned by `SourceManager` in meta,
    /// instead of `vnode` computed from the `partition_id`.
    ///
    /// ### `StreamFsFetch`: distributed by `partition_id`
    ///
    /// Each actor accesses (range scan) splits according to the `vnode`
    /// computed from `partition_id`.
    /// This is a normal distributed table.
    pub fn infer_internal_table_catalog(is_distributed: bool) -> TableCatalog {
        let mut builder = TableCatalogBuilder::default();

        let key = Field {
            data_type: DataType::Varchar,
            name: "partition_id".to_owned(),
            sub_fields: vec![],
            type_name: "".to_owned(),
        };
        let value = Field {
            data_type: DataType::Jsonb,
            name: "offset_info".to_owned(),
            sub_fields: vec![],
            type_name: "".to_owned(),
        };

        let ordered_col_idx = builder.add_column(&key);
        builder.add_column(&value);
        builder.add_order_column(ordered_col_idx, OrderType::ascending());

        builder.build(
            if is_distributed {
                vec![ordered_col_idx]
            } else {
                vec![]
            },
            1,
        )
    }
}
