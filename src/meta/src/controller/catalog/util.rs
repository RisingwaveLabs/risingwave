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

use risingwave_meta_model::fragment_relation;

use super::*;

pub(crate) async fn update_internal_tables(
    txn: &DatabaseTransaction,
    object_id: i32,
    column: object::Column,
    new_value: Value,
    objects_to_notify: &mut Vec<PbObjectInfo>,
) -> MetaResult<()> {
    let internal_tables = get_internal_tables_by_id(object_id, txn).await?;

    if !internal_tables.is_empty() {
        Object::update_many()
            .col_expr(column, SimpleExpr::Value(new_value))
            .filter(object::Column::Oid.is_in(internal_tables.clone()))
            .exec(txn)
            .await?;

        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(internal_tables))
            .all(txn)
            .await?;
        for (table, table_obj) in table_objs {
            objects_to_notify.push(PbObjectInfo::Table(
                ObjectModel(table, table_obj.unwrap()).into(),
            ));
        }
    }
    Ok(())
}

impl CatalogController {
    pub(crate) async fn init(&self) -> MetaResult<()> {
        self.table_catalog_cdc_table_id_update().await?;
        Ok(())
    }

    /// Fill in the `cdc_table_id` field for Table with empty `cdc_table_id` and parent Source job.
    /// NOTES: We assume Table with a parent Source job is a CDC table
    pub(crate) async fn table_catalog_cdc_table_id_update(&self) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        // select Tables which cdc_table_id is empty and has a parent Source job
        let table_and_source_id: Vec<(TableId, String, SourceId)> = Table::find()
            .join(JoinType::InnerJoin, table::Relation::ObjectDependency.def())
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Source.def(),
            )
            .select_only()
            .columns([table::Column::TableId, table::Column::Definition])
            .columns([source::Column::SourceId])
            .filter(
                table::Column::TableType.eq(TableType::Table).and(
                    table::Column::CdcTableId
                        .is_null()
                        .or(table::Column::CdcTableId.eq("")),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        // return directly if the result set is empty.
        if table_and_source_id.is_empty() {
            return Ok(());
        }

        info!(table_and_source_id = ?table_and_source_id, "cdc table with empty cdc_table_id");

        let mut cdc_table_ids = HashMap::new();
        for (table_id, definition, source_id) in table_and_source_id {
            match extract_external_table_name_from_definition(&definition) {
                None => {
                    tracing::warn!(
                        table_id = table_id,
                        definition = definition,
                        "failed to extract cdc table name from table definition.",
                    )
                }
                Some(external_table_name) => {
                    cdc_table_ids.insert(
                        table_id,
                        build_cdc_table_id(source_id as u32, &external_table_name),
                    );
                }
            }
        }

        for (table_id, cdc_table_id) in cdc_table_ids {
            table::ActiveModel {
                table_id: Set(table_id as _),
                cdc_table_id: Set(Some(cdc_table_id)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn list_object_dependencies(
        &self,
        include_creating: bool,
    ) -> MetaResult<Vec<PbObjectDependencies>> {
        let inner = self.inner.read().await;

        let dependencies: Vec<(ObjectId, ObjectId)> = {
            let filter = if include_creating {
                Expr::value(true)
            } else {
                streaming_job::Column::JobStatus.eq(JobStatus::Created)
            };
            ObjectDependency::find()
                .select_only()
                .columns([
                    object_dependency::Column::Oid,
                    object_dependency::Column::UsedBy,
                ])
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        let mut obj_dependencies = dependencies
            .into_iter()
            .map(|(oid, used_by)| PbObjectDependencies {
                object_id: used_by as _,
                referenced_object_id: oid as _,
            })
            .collect_vec();

        let view_dependencies: Vec<(ObjectId, ObjectId)> = ObjectDependency::find()
            .select_only()
            .columns([
                object_dependency::Column::Oid,
                object_dependency::Column::UsedBy,
            ])
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Object1.def(),
            )
            .join(JoinType::InnerJoin, object::Relation::View.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        obj_dependencies.extend(view_dependencies.into_iter().map(|(view_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id as _,
                referenced_object_id: view_id as _,
            }
        }));

        let sink_dependencies: Vec<(SinkId, TableId)> = {
            let filter = if include_creating {
                sink::Column::TargetTable.is_not_null()
            } else {
                streaming_job::Column::JobStatus
                    .eq(JobStatus::Created)
                    .and(sink::Column::TargetTable.is_not_null())
            };
            Sink::find()
                .select_only()
                .columns([sink::Column::SinkId, sink::Column::TargetTable])
                .join(JoinType::InnerJoin, sink::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(sink_dependencies.into_iter().map(|(sink_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id as _,
                referenced_object_id: sink_id as _,
            }
        }));

        let subscription_dependencies: Vec<(SubscriptionId, TableId)> = {
            let filter = if include_creating {
                subscription::Column::DependentTableId.is_not_null()
            } else {
                subscription::Column::SubscriptionState
                    .eq(Into::<i32>::into(SubscriptionState::Created))
                    .and(subscription::Column::DependentTableId.is_not_null())
            };
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::DependentTableId,
                ])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(subscription_dependencies.into_iter().map(
            |(subscription_id, table_id)| PbObjectDependencies {
                object_id: subscription_id as _,
                referenced_object_id: table_id as _,
            },
        ));

        Ok(obj_dependencies)
    }

    pub(crate) async fn log_cleaned_dirty_jobs(
        &self,
        dirty_objs: &[PartialObject],
        txn: &DatabaseTransaction,
    ) -> MetaResult<()> {
        // Record cleaned streaming jobs in event logs.
        let mut dirty_table_ids = vec![];
        let mut dirty_source_ids = vec![];
        let mut dirty_sink_ids = vec![];
        for dirty_job_obj in dirty_objs {
            let job_id = dirty_job_obj.oid;
            let job_type = dirty_job_obj.obj_type;
            match job_type {
                ObjectType::Table | ObjectType::Index => dirty_table_ids.push(job_id),
                ObjectType::Source => dirty_source_ids.push(job_id),
                ObjectType::Sink => dirty_sink_ids.push(job_id),
                _ => unreachable!("unexpected streaming job type"),
            }
        }

        let mut event_logs = vec![];
        if !dirty_table_ids.is_empty() {
            let table_info: Vec<(TableId, String, String)> = Table::find()
                .select_only()
                .columns([
                    table::Column::TableId,
                    table::Column::Name,
                    table::Column::Definition,
                ])
                .filter(table::Column::TableId.is_in(dirty_table_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (table_id, name, definition) in table_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: table_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_source_ids.is_empty() {
            let source_info: Vec<(SourceId, String, String)> = Source::find()
                .select_only()
                .columns([
                    source::Column::SourceId,
                    source::Column::Name,
                    source::Column::Definition,
                ])
                .filter(source::Column::SourceId.is_in(dirty_source_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (source_id, name, definition) in source_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: source_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_sink_ids.is_empty() {
            let sink_info: Vec<(SinkId, String, String)> = Sink::find()
                .select_only()
                .columns([
                    sink::Column::SinkId,
                    sink::Column::Name,
                    sink::Column::Definition,
                ])
                .filter(sink::Column::SinkId.is_in(dirty_sink_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (sink_id, name, definition) in sink_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: sink_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        self.env.event_log_manager_ref().add_event_logs(event_logs);
        Ok(())
    }

    pub(crate) async fn clean_dirty_sink_downstreams(
        txn: &DatabaseTransaction,
    ) -> MetaResult<bool> {
        // clean incoming sink from (table)
        // clean upstream fragment ids from (fragment)
        // clean stream node from (fragment)
        // clean upstream actor ids from (actor)
        let all_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .columns(vec![fragment::Column::FragmentId])
            .into_tuple()
            .all(txn)
            .await?;

        let all_fragment_ids: HashSet<_> = all_fragment_ids.into_iter().collect();

        let table_sink_ids: Vec<ObjectId> = Sink::find()
            .select_only()
            .column(sink::Column::SinkId)
            .filter(sink::Column::TargetTable.is_not_null())
            .into_tuple()
            .all(txn)
            .await?;

        let all_table_with_incoming_sinks: Vec<(ObjectId, I32Array)> = Table::find()
            .select_only()
            .columns(vec![table::Column::TableId, table::Column::IncomingSinks])
            .into_tuple()
            .all(txn)
            .await?;

        let table_incoming_sinks_to_update = all_table_with_incoming_sinks
            .into_iter()
            .filter(|(_, incoming_sinks)| {
                let inner_ref = incoming_sinks.inner_ref();
                !inner_ref.is_empty()
                    && inner_ref
                        .iter()
                        .any(|sink_id| !table_sink_ids.contains(sink_id))
            })
            .collect_vec();

        let new_table_incoming_sinks = table_incoming_sinks_to_update
            .into_iter()
            .map(|(table_id, incoming_sinks)| {
                let new_incoming_sinks = incoming_sinks
                    .into_inner()
                    .extract_if(|id| table_sink_ids.contains(id))
                    .collect_vec();
                (table_id, I32Array::from(new_incoming_sinks))
            })
            .collect_vec();

        // no need to update, returning
        if new_table_incoming_sinks.is_empty() {
            return Ok(false);
        }

        for (table_id, new_incoming_sinks) in new_table_incoming_sinks {
            tracing::info!("cleaning dirty table sink downstream table {}", table_id);
            Table::update_many()
                .col_expr(table::Column::IncomingSinks, new_incoming_sinks.into())
                .filter(table::Column::TableId.eq(table_id))
                .exec(txn)
                .await?;

            let fragments: Vec<(FragmentId, StreamNode, i32)> = Fragment::find()
                .select_only()
                .columns(vec![
                    fragment::Column::FragmentId,
                    fragment::Column::StreamNode,
                    fragment::Column::FragmentTypeMask,
                ])
                .filter(fragment::Column::JobId.eq(table_id))
                .into_tuple()
                .all(txn)
                .await?;

            let fragment_ids = fragments
                .iter()
                .map(|(fragment_id, _, _)| *fragment_id)
                .collect_vec();

            let fragment_relations: Vec<(FragmentId, FragmentId)> = FragmentRelation::find()
                .select_only()
                .columns([
                    fragment_relation::Column::SourceFragmentId,
                    fragment_relation::Column::TargetFragmentId,
                ])
                .filter(fragment_relation::Column::TargetFragmentId.is_in(fragment_ids))
                .into_tuple()
                .all(txn)
                .await?;

            let mut fragment_upstreams = HashMap::new();

            for (src, dst) in fragment_relations {
                fragment_upstreams.entry(dst).or_insert(vec![]).push(src);
            }

            for (fragment_id, stream_node, fragment_mask) in fragments {
                let mut upstream_fragment_ids = fragment_upstreams
                    .get(&fragment_id)
                    .cloned()
                    .unwrap_or(vec![]);

                let dirty_upstream_fragment_ids = upstream_fragment_ids
                    .extract_if(|id| !all_fragment_ids.contains(id))
                    .collect_vec();

                if !dirty_upstream_fragment_ids.is_empty() {
                    // dirty downstream should be materialize fragment of table
                    assert!(fragment_mask & FragmentTypeFlag::Mview as i32 > 0);

                    tracing::info!(
                        "cleaning dirty table sink fragment {:?} from downstream fragment {}",
                        dirty_upstream_fragment_ids,
                        fragment_id
                    );

                    let mut pb_stream_node = stream_node.to_protobuf();

                    visit_stream_node_cont_mut(&mut pb_stream_node, |node| {
                        if let Some(NodeBody::Union(_)) = node.node_body {
                            node.input.retain_mut(|input| {
                                if let Some(NodeBody::Merge(merge_node)) = &mut input.node_body
                                    && all_fragment_ids
                                        .contains(&(merge_node.upstream_fragment_id as i32))
                                {
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        true
                    });

                    FragmentRelation::delete_many()
                        .filter(
                            fragment_relation::Column::TargetFragmentId
                                .eq(fragment_id)
                                .and(
                                    fragment_relation::Column::SourceFragmentId
                                        .is_in(dirty_upstream_fragment_ids),
                                ),
                        )
                        .exec(txn)
                        .await?;

                    Fragment::update_many()
                        .col_expr(
                            fragment::Column::StreamNode,
                            StreamNode::from(&pb_stream_node).into(),
                        )
                        .filter(fragment::Column::FragmentId.eq(fragment_id))
                        .exec(txn)
                        .await?;

                    let actors: Vec<(ActorId, ActorUpstreamActors)> = Actor::find()
                        .select_only()
                        .columns(vec![
                            actor::Column::ActorId,
                            actor::Column::UpstreamActorIds,
                        ])
                        .filter(actor::Column::FragmentId.eq(fragment_id))
                        .into_tuple()
                        .all(txn)
                        .await?;

                    for (actor_id, upstream_actor_ids) in actors {
                        let mut upstream_actor_ids = upstream_actor_ids.into_inner();

                        let dirty_actor_upstreams = upstream_actor_ids
                            .extract_if(|id, _| !all_fragment_ids.contains(id))
                            .map(|(id, _)| id)
                            .collect_vec();

                        if !dirty_actor_upstreams.is_empty() {
                            tracing::debug!(
                                "cleaning dirty table sink fragment {:?} from downstream fragment {} actor {}",
                                dirty_actor_upstreams,
                                fragment_id,
                                actor_id,
                            );

                            Actor::update_many()
                                .col_expr(
                                    actor::Column::UpstreamActorIds,
                                    ActorUpstreamActors::from(upstream_actor_ids).into(),
                                )
                                .filter(actor::Column::ActorId.eq(actor_id))
                                .exec(txn)
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    pub async fn has_any_streaming_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = streaming_job::Entity::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    pub async fn find_creating_streaming_job_ids(
        &self,
        infos: Vec<PbCreatingJobInfo>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;

        type JobKey = (DatabaseId, SchemaId, String);

        // Index table is already included if we still assign the same name for index table as the index.
        let creating_tables: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_sinks: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, sink::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_subscriptions: Vec<(ObjectId, String, DatabaseId, SchemaId)> =
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::Name,
                ])
                .columns([object::Column::DatabaseId, object::Column::SchemaId])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
                .into_tuple()
                .all(&inner.db)
                .await?;

        let mut job_mapping: HashMap<JobKey, ObjectId> = creating_tables
            .into_iter()
            .chain(creating_sinks.into_iter())
            .chain(creating_subscriptions.into_iter())
            .map(|(id, name, database_id, schema_id)| ((database_id, schema_id, name), id))
            .collect();

        Ok(infos
            .into_iter()
            .flat_map(|info| {
                job_mapping.remove(&(
                    info.database_id as _,
                    info.schema_id as _,
                    info.name.clone(),
                ))
            })
            .collect())
    }
}
