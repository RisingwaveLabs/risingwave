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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::stream_graph_visitor::{visit_stream_node, visit_stream_node_mut};
use risingwave_common::{bail, current_cluster_version};
use risingwave_connector::WithPropertiesExt;
use risingwave_meta_model::actor::ActorStatus;
use risingwave_meta_model::actor_dispatcher::DispatcherType;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{
    Actor, ActorDispatcher, Fragment, Index, Object, ObjectDependency, Sink, Source,
    StreamingJob as StreamingJobModel, Table,
};
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::{
    actor, actor_dispatcher, fragment, index, object, object_dependency, sink, source,
    streaming_job, table, ActorId, ActorUpstreamActors, ColumnCatalogArray, CreateType, DatabaseId,
    ExprNodeArray, FragmentId, I32Array, IndexId, JobStatus, ObjectId, SchemaId, SinkId, SourceId,
    StreamNode, StreamingParallelism, UserId,
};
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::table::PbOptionalAssociatedSourceId;
use risingwave_pb::catalog::{PbCreateType, PbTable};
use risingwave_pb::meta::list_rate_limits_response::RateLimitInfo;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Info, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::{PbFragmentWorkerSlotMapping, PbObject, PbObjectGroup};
use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::{
    PbDispatcher, PbDispatcherType, PbFragmentTypeFlag, PbStreamActor, PbStreamNode,
};
use sea_orm::sea_query::{BinOper, Expr, Query, SimpleExpr};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, IntoActiveModel,
    IntoSimpleExpr, JoinType, ModelTrait, NotSet, PaginatorTrait, QueryFilter, QuerySelect,
    RelationTrait, TransactionTrait,
};

use crate::barrier::{ReplaceStreamJobPlan, Reschedule};
use crate::controller::catalog::CatalogController;
use crate::controller::rename::ReplaceTableExprRewriter;
use crate::controller::utils::{
    build_object_group_for_delete, check_relation_name_duplicate, check_sink_into_table_cycle,
    ensure_object_id, ensure_user_id, get_fragment_actor_ids, get_fragment_mappings,
    get_internal_tables_by_id, rebuild_fragment_mapping_from_actors, PartialObject,
};
use crate::controller::ObjectModel;
use crate::manager::{NotificationVersion, StreamingJob, StreamingJobType};
use crate::model::{StreamContext, StreamJobFragments, TableParallelism};
use crate::stream::{JobReschedulePostUpdates, SplitAssignment};
use crate::{MetaError, MetaResult};

impl CatalogController {
    pub async fn create_streaming_job_obj(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        create_type: PbCreateType,
        ctx: &StreamContext,
        streaming_parallelism: StreamingParallelism,
        max_parallelism: usize,
        specific_resource_group: Option<String>, // todo: can we move it to StreamContext?
    ) -> MetaResult<ObjectId> {
        let obj = Self::create_object(txn, obj_type, owner_id, database_id, schema_id).await?;
        let job = streaming_job::ActiveModel {
            job_id: Set(obj.oid),
            job_status: Set(JobStatus::Initial),
            create_type: Set(create_type.into()),
            timezone: Set(ctx.timezone.clone()),
            parallelism: Set(streaming_parallelism),
            max_parallelism: Set(max_parallelism as _),
            specific_resource_group: Set(specific_resource_group),
        };
        job.insert(txn).await?;

        Ok(obj.oid)
    }

    /// Create catalogs for the streaming job, then notify frontend about them if the job is a
    /// materialized view.
    ///
    /// Some of the fields in the given streaming job are placeholders, which will
    /// be updated later in `prepare_streaming_job` and notify again in `finish_streaming_job`.
    pub async fn create_job_catalog(
        &self,
        streaming_job: &mut StreamingJob,
        ctx: &StreamContext,
        parallelism: &Option<Parallelism>,
        max_parallelism: usize,
        mut dependencies: HashSet<ObjectId>,
        specific_resource_group: Option<String>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let create_type = streaming_job.create_type();

        let streaming_parallelism = match (parallelism, self.env.opts.default_parallelism) {
            (None, DefaultParallelism::Full) => StreamingParallelism::Adaptive,
            (None, DefaultParallelism::Default(n)) => StreamingParallelism::Fixed(n.get()),
            (Some(n), _) => StreamingParallelism::Fixed(n.parallelism as _),
        };

        ensure_user_id(streaming_job.owner() as _, &txn).await?;
        ensure_object_id(ObjectType::Database, streaming_job.database_id() as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, streaming_job.schema_id() as _, &txn).await?;
        check_relation_name_duplicate(
            &streaming_job.name(),
            streaming_job.database_id() as _,
            streaming_job.schema_id() as _,
            &txn,
        )
        .await?;

        // TODO(rc): pass all dependencies uniformly, deprecate `dependent_relations` and `dependent_secret_ids`.
        dependencies.extend(
            streaming_job
                .dependent_relations()
                .into_iter()
                .map(|id| id as ObjectId),
        );

        // check if any dependency is in altering status.
        if !dependencies.is_empty() {
            let altering_cnt = ObjectDependency::find()
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(
                    object_dependency::Column::Oid
                        .is_in(dependencies.clone())
                        .and(object::Column::ObjType.eq(ObjectType::Table))
                        .and(streaming_job::Column::JobStatus.ne(JobStatus::Created))
                        .and(
                            // It means the referring table is just dummy for altering.
                            object::Column::Oid.not_in_subquery(
                                Query::select()
                                    .column(table::Column::TableId)
                                    .from(Table)
                                    .to_owned(),
                            ),
                        ),
                )
                .count(&txn)
                .await?;
            if altering_cnt != 0 {
                return Err(MetaError::permission_denied(
                    "some dependent relations are being altered",
                ));
            }
        }

        match streaming_job {
            StreamingJob::MaterializedView(table) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Table,
                    table.owner as _,
                    Some(table.database_id as _),
                    Some(table.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                table.id = job_id as _;
                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
            }
            StreamingJob::Sink(sink, _) => {
                if let Some(target_table_id) = sink.target_table {
                    if check_sink_into_table_cycle(
                        target_table_id as ObjectId,
                        dependencies.iter().cloned().collect(),
                        &txn,
                    )
                    .await?
                    {
                        bail!("Creating such a sink will result in circular dependency.");
                    }
                }

                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Sink,
                    sink.owner as _,
                    Some(sink.database_id as _),
                    Some(sink.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                sink.id = job_id as _;
                let sink_model: sink::ActiveModel = sink.clone().into();
                Sink::insert(sink_model).exec(&txn).await?;
            }
            StreamingJob::Table(src, table, _) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Table,
                    table.owner as _,
                    Some(table.database_id as _),
                    Some(table.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                table.id = job_id as _;
                if let Some(src) = src {
                    let src_obj = Self::create_object(
                        &txn,
                        ObjectType::Source,
                        src.owner as _,
                        Some(src.database_id as _),
                        Some(src.schema_id as _),
                    )
                    .await?;
                    src.id = src_obj.oid as _;
                    src.optional_associated_table_id =
                        Some(PbOptionalAssociatedTableId::AssociatedTableId(job_id as _));
                    table.optional_associated_source_id = Some(
                        PbOptionalAssociatedSourceId::AssociatedSourceId(src_obj.oid as _),
                    );
                    let source: source::ActiveModel = src.clone().into();
                    Source::insert(source).exec(&txn).await?;
                }
                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
            }
            StreamingJob::Index(index, table) => {
                ensure_object_id(ObjectType::Table, index.primary_table_id as _, &txn).await?;
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Index,
                    index.owner as _,
                    Some(index.database_id as _),
                    Some(index.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                // to be compatible with old implementation.
                index.id = job_id as _;
                index.index_table_id = job_id as _;
                table.id = job_id as _;

                ObjectDependency::insert(object_dependency::ActiveModel {
                    oid: Set(index.primary_table_id as _),
                    used_by: Set(table.id as _),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;

                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
                let index_model: index::ActiveModel = index.clone().into();
                Index::insert(index_model).exec(&txn).await?;
            }
            StreamingJob::Source(src) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Source,
                    src.owner as _,
                    Some(src.database_id as _),
                    Some(src.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                src.id = job_id as _;
                let source_model: source::ActiveModel = src.clone().into();
                Source::insert(source_model).exec(&txn).await?;
            }
        }

        // collect dependent secrets.
        dependencies.extend(
            streaming_job
                .dependent_secret_ids()?
                .into_iter()
                .map(|secret_id| secret_id as ObjectId),
        );
        // collect dependent connection
        dependencies.extend(
            streaming_job
                .dependent_connection_ids()?
                .into_iter()
                .map(|conn_id| conn_id as ObjectId),
        );

        // record object dependency.
        if !dependencies.is_empty() {
            ObjectDependency::insert_many(dependencies.into_iter().map(|oid| {
                object_dependency::ActiveModel {
                    oid: Set(oid),
                    used_by: Set(streaming_job.id() as _),
                    ..Default::default()
                }
            }))
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        Ok(())
    }

    /// Create catalogs for internal tables, then notify frontend about them if the job is a
    /// materialized view.
    ///
    /// Some of the fields in the given "incomplete" internal tables are placeholders, which will
    /// be updated later in `prepare_streaming_job` and notify again in `finish_streaming_job`.
    ///
    /// Returns a mapping from the temporary table id to the actual global table id.
    pub async fn create_internal_table_catalog(
        &self,
        job: &StreamingJob,
        mut incomplete_internal_tables: Vec<PbTable>,
    ) -> MetaResult<HashMap<u32, u32>> {
        let job_id = job.id() as ObjectId;
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let mut table_id_map = HashMap::new();
        for table in &mut incomplete_internal_tables {
            let table_id = Self::create_object(
                &txn,
                ObjectType::Table,
                table.owner as _,
                Some(table.database_id as _),
                Some(table.schema_id as _),
            )
            .await?
            .oid;
            table_id_map.insert(table.id, table_id as u32);
            table.id = table_id as _;
            table.job_id = Some(job_id as _);

            let table_model = table::ActiveModel {
                table_id: Set(table_id as _),
                belongs_to_job_id: Set(Some(job_id)),
                fragment_id: NotSet,
                ..table.clone().into()
            };
            Table::insert(table_model).exec(&txn).await?;
        }
        txn.commit().await?;

        Ok(table_id_map)
    }

    // TODO: In this function, we also update the `Table` model in the meta store.
    // Given that we've ensured the tables inside `TableFragments` are complete, shall we consider
    // making them the source of truth and performing a full replacement for those in the meta store?
    /// Insert fragments and actors to meta store. Used both for creating new jobs and replacing jobs.
    pub async fn prepare_streaming_job(
        &self,
        stream_job_fragments: &StreamJobFragments,
        streaming_job: &StreamingJob,
        for_replace: bool,
    ) -> MetaResult<()> {
        let is_materialized_view = streaming_job.is_materialized_view();
        let fragment_actors =
            Self::extract_fragment_and_actors_from_fragments(stream_job_fragments)?;
        let all_tables = stream_job_fragments.all_tables();
        let inner = self.inner.write().await;

        let mut objects = vec![];
        let txn = inner.db.begin().await?;

        // Add fragments.
        let (fragments, actor_with_dispatchers): (Vec<_>, Vec<_>) = fragment_actors
            .into_iter()
            .map(|(fragment, actors, actor_dispatchers)| (fragment, (actors, actor_dispatchers)))
            .unzip();
        for fragment in fragments {
            let fragment_id = fragment.fragment_id;
            let state_table_ids = fragment.state_table_ids.inner_ref().clone();

            let fragment = fragment.into_active_model();
            Fragment::insert(fragment).exec(&txn).await?;

            // Fields including `fragment_id` and `vnode_count` were placeholder values before.
            // After table fragments are created, update them for all tables.
            if !for_replace {
                for state_table_id in state_table_ids {
                    // Table's vnode count is not always the fragment's vnode count, so we have to
                    // look up the table from `TableFragments`.
                    // See `ActorGraphBuilder::new`.
                    let table = all_tables
                        .get(&(state_table_id as u32))
                        .unwrap_or_else(|| panic!("table {} not found", state_table_id));
                    assert_eq!(table.id, state_table_id as u32);
                    assert_eq!(table.fragment_id, fragment_id as u32);
                    let vnode_count = table.vnode_count();

                    table::ActiveModel {
                        table_id: Set(state_table_id as _),
                        fragment_id: Set(Some(fragment_id)),
                        vnode_count: Set(vnode_count as _),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;

                    if is_materialized_view {
                        objects.push(PbObject {
                            object_info: Some(PbObjectInfo::Table(table.clone())),
                        });
                    }
                }
            }
        }

        // Add actors and actor dispatchers.
        for (actors, actor_dispatchers) in actor_with_dispatchers {
            for actor in actors {
                let actor = actor.into_active_model();
                Actor::insert(actor).exec(&txn).await?;
            }
            for (_, actor_dispatchers) in actor_dispatchers {
                for actor_dispatcher in actor_dispatchers {
                    let mut actor_dispatcher = actor_dispatcher.into_active_model();
                    actor_dispatcher.id = NotSet;
                    ActorDispatcher::insert(actor_dispatcher).exec(&txn).await?;
                }
            }
        }

        if !for_replace {
            // Update dml fragment id.
            if let StreamingJob::Table(_, table, ..) = streaming_job {
                Table::update(table::ActiveModel {
                    table_id: Set(table.id as _),
                    dml_fragment_id: Set(table.dml_fragment_id.map(|id| id as _)),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;
            }
        }

        txn.commit().await?;

        if !objects.is_empty() {
            self.notify_frontend(Operation::Add, Info::ObjectGroup(PbObjectGroup { objects }))
                .await;
        }

        Ok(())
    }

    /// `try_abort_creating_streaming_job` is used to abort the job that is under initial status or in `FOREGROUND` mode.
    /// It returns (true, _) if the job is not found or aborted.
    /// It returns (_, Some(`database_id`)) is the `database_id` of the `job_id` exists
    pub async fn try_abort_creating_streaming_job(
        &self,
        job_id: ObjectId,
        is_cancelled: bool,
    ) -> MetaResult<(bool, Option<DatabaseId>)> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj = Object::find_by_id(job_id).one(&txn).await?;
        let Some(obj) = obj else {
            tracing::warn!(
                id = job_id,
                "streaming job not found when aborting creating, might be cleaned by recovery"
            );
            return Ok((true, None));
        };
        let database_id = obj
            .database_id
            .ok_or_else(|| anyhow!("obj has no database id: {:?}", obj))?;

        if !is_cancelled {
            let streaming_job = streaming_job::Entity::find_by_id(job_id).one(&txn).await?;
            if let Some(streaming_job) = streaming_job {
                assert_ne!(streaming_job.job_status, JobStatus::Created);
                if streaming_job.create_type == CreateType::Background
                    && streaming_job.job_status == JobStatus::Creating
                {
                    // If the job is created in background and still in creating status, we should not abort it and let recovery to handle it.
                    tracing::warn!(
                        id = job_id,
                        "streaming job is created in background and still in creating status"
                    );
                    return Ok((false, Some(database_id)));
                }
            }
        }

        let internal_table_ids = get_internal_tables_by_id(job_id, &txn).await?;

        // Get the notification info if the job is a materialized view.
        let table_obj = Table::find_by_id(job_id).one(&txn).await?;
        let mut objs = vec![];
        if let Some(table) = &table_obj
            && table.table_type == TableType::MaterializedView
        {
            let obj: Option<PartialObject> = Object::find_by_id(job_id)
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .into_partial_model()
                .one(&txn)
                .await?;
            let obj =
                obj.ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;
            objs.push(obj);
            let internal_table_objs: Vec<PartialObject> = Object::find()
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .join(JoinType::InnerJoin, object::Relation::Table.def())
                .filter(table::Column::BelongsToJobId.eq(job_id))
                .into_partial_model()
                .all(&txn)
                .await?;
            objs.extend(internal_table_objs);
        }

        Object::delete_by_id(job_id).exec(&txn).await?;
        if !internal_table_ids.is_empty() {
            Object::delete_many()
                .filter(object::Column::Oid.is_in(internal_table_ids))
                .exec(&txn)
                .await?;
        }
        if let Some(t) = &table_obj
            && let Some(source_id) = t.optional_associated_source_id
        {
            Object::delete_by_id(source_id).exec(&txn).await?;
        }

        for tx in inner
            .creating_table_finish_notifier
            .remove(&job_id)
            .into_iter()
            .flatten()
        {
            let err = if is_cancelled {
                MetaError::cancelled(format!("streaming job {job_id} is cancelled"))
            } else {
                MetaError::catalog_id_not_found(
                    "stream job",
                    format!("streaming job {job_id} failed"),
                )
            };
            let _ = tx.send(Err(err));
        }
        txn.commit().await?;

        if !objs.is_empty() {
            // We also have notified the frontend about these objects,
            // so we need to notify the frontend to delete them here.
            self.notify_frontend(Operation::Delete, build_object_group_for_delete(objs))
                .await;
        }
        Ok((true, Some(database_id)))
    }

    pub async fn post_collect_job_fragments(
        &self,
        job_id: ObjectId,
        actor_ids: Vec<crate::model::ActorId>,
        new_actor_dispatchers: &HashMap<
            crate::model::FragmentId,
            HashMap<crate::model::ActorId, Vec<PbDispatcher>>,
        >,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        Actor::update_many()
            .col_expr(
                actor::Column::Status,
                SimpleExpr::from(ActorStatus::Running.into_value()),
            )
            .filter(
                actor::Column::ActorId
                    .is_in(actor_ids.into_iter().map(|id| id as ActorId).collect_vec()),
            )
            .exec(&txn)
            .await?;

        for splits in split_assignment.values() {
            for (actor_id, splits) in splits {
                let splits = splits.iter().map(PbConnectorSplit::from).collect_vec();
                let connector_splits = &PbConnectorSplits { splits };
                actor::ActiveModel {
                    actor_id: Set(*actor_id as _),
                    splits: Set(Some(connector_splits.into())),
                    ..Default::default()
                }
                .update(&txn)
                .await?;
            }
        }

        let mut actor_dispatchers = vec![];
        for (actor_id, dispatchers) in new_actor_dispatchers.values().flatten() {
            for dispatcher in dispatchers {
                let mut actor_dispatcher =
                    actor_dispatcher::Model::from((*actor_id, dispatcher.clone()))
                        .into_active_model();
                actor_dispatcher.id = NotSet;
                actor_dispatchers.push(actor_dispatcher);
            }
        }

        if !actor_dispatchers.is_empty() {
            ActorDispatcher::insert_many(actor_dispatchers)
                .exec(&txn)
                .await?;
        }

        // Mark job as CREATING.
        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Creating),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }

    pub async fn create_job_catalog_for_replace(
        &self,
        streaming_job: &StreamingJob,
        ctx: &StreamContext,
        specified_parallelism: &Option<NonZeroUsize>,
        max_parallelism: usize,
    ) -> MetaResult<ObjectId> {
        let id = streaming_job.id();
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // 1. check version.
        streaming_job.verify_version_for_replace(&txn).await?;
        // 2. check concurrent replace.
        let referring_cnt = ObjectDependency::find()
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Object1.def(),
            )
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(
                object_dependency::Column::Oid
                    .eq(id as ObjectId)
                    .and(object::Column::ObjType.eq(ObjectType::Table))
                    .and(streaming_job::Column::JobStatus.ne(JobStatus::Created)),
            )
            .count(&txn)
            .await?;
        if referring_cnt != 0 {
            return Err(MetaError::permission_denied(
                "job is being altered or referenced by some creating jobs",
            ));
        }

        // 3. check parallelism.
        let original_max_parallelism: i32 = StreamingJobModel::find_by_id(id as ObjectId)
            .select_only()
            .column(streaming_job::Column::MaxParallelism)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(streaming_job.job_type_str(), id))?;

        if original_max_parallelism != max_parallelism as i32 {
            // We already override the max parallelism in `StreamFragmentGraph` before entering this function.
            // This should not happen in normal cases.
            bail!(
                "cannot use a different max parallelism \
                 when replacing streaming job, \
                 original: {}, new: {}",
                original_max_parallelism,
                max_parallelism
            );
        }

        let parallelism = match specified_parallelism {
            None => StreamingParallelism::Adaptive,
            Some(n) => StreamingParallelism::Fixed(n.get() as _),
        };

        // 4. create streaming object for new replace table.
        let new_obj_id = Self::create_streaming_job_obj(
            &txn,
            streaming_job.object_type(),
            streaming_job.owner() as _,
            Some(streaming_job.database_id() as _),
            Some(streaming_job.schema_id() as _),
            streaming_job.create_type(),
            ctx,
            parallelism,
            max_parallelism,
            None,
        )
        .await?;

        // 5. record dependency for new replace table.
        ObjectDependency::insert(object_dependency::ActiveModel {
            oid: Set(id as _),
            used_by: Set(new_obj_id as _),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(new_obj_id)
    }

    /// `finish_streaming_job` marks job related objects as `Created` and notify frontend.
    pub async fn finish_streaming_job(
        &self,
        job_id: ObjectId,
        replace_stream_job_info: Option<ReplaceStreamJobPlan>,
    ) -> MetaResult<()> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let job_type = Object::find_by_id(job_id)
            .select_only()
            .column(object::Column::ObjType)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;

        // update `created_at` as now() and `created_at_cluster_version` as current cluster version.
        let res = Object::update_many()
            .col_expr(object::Column::CreatedAt, Expr::current_timestamp().into())
            .col_expr(
                object::Column::CreatedAtClusterVersion,
                current_cluster_version().into(),
            )
            .filter(object::Column::Oid.eq(job_id))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("streaming job", job_id));
        }

        // mark the target stream job as `Created`.
        let job = streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Created),
            ..Default::default()
        };
        job.update(&txn).await?;

        // notify frontend: job, internal tables.
        let internal_table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::BelongsToJobId.eq(job_id))
            .all(&txn)
            .await?;
        let mut objects = internal_table_objs
            .iter()
            .map(|(table, obj)| PbObject {
                object_info: Some(PbObjectInfo::Table(
                    ObjectModel(table.clone(), obj.clone().unwrap()).into(),
                )),
            })
            .collect_vec();
        let mut notification_op = NotificationOperation::Add;

        match job_type {
            ObjectType::Table => {
                let (table, obj) = Table::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", job_id))?;
                if table.table_type == TableType::MaterializedView {
                    notification_op = NotificationOperation::Update;
                }

                if let Some(source_id) = table.optional_associated_source_id {
                    let (src, obj) = Source::find_by_id(source_id)
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Source(
                            ObjectModel(src, obj.unwrap()).into(),
                        )),
                    });
                }
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Table(ObjectModel(table, obj.unwrap()).into())),
                });
            }
            ObjectType::Sink => {
                let (sink, obj) = Sink::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Sink(ObjectModel(sink, obj.unwrap()).into())),
                });
            }
            ObjectType::Index => {
                let (index, obj) = Index::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("index", job_id))?;
                {
                    let (table, obj) = Table::find_by_id(index.index_table_id)
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("table", index.index_table_id)
                        })?;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Table(
                            ObjectModel(table, obj.unwrap()).into(),
                        )),
                    });
                }
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(ObjectModel(index, obj.unwrap()).into())),
                });
            }
            ObjectType::Source => {
                let (source, obj) = Source::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Source(
                        ObjectModel(source, obj.unwrap()).into(),
                    )),
                });
            }
            _ => unreachable!("invalid job type: {:?}", job_type),
        }

        let fragment_mapping = get_fragment_mappings(&txn, job_id).await?;

        let replace_table_mapping_update = match replace_stream_job_info {
            Some(ReplaceStreamJobPlan {
                streaming_job,
                merge_updates,
                tmp_id,
                ..
            }) => {
                let incoming_sink_id = job_id;

                let (relations, fragment_mapping) = Self::finish_replace_streaming_job_inner(
                    tmp_id as ObjectId,
                    merge_updates,
                    None,
                    SinkIntoTableContext {
                        creating_sink_id: Some(incoming_sink_id as _),
                        dropping_sink_id: None,
                        updated_sink_catalogs: vec![],
                    },
                    &txn,
                    streaming_job,
                )
                .await?;

                Some((relations, fragment_mapping))
            }
            None => None,
        };

        txn.commit().await?;

        self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
            .await;

        let mut version = self
            .notify_frontend(
                notification_op,
                NotificationInfo::ObjectGroup(PbObjectGroup { objects }),
            )
            .await;

        if let Some((objects, fragment_mapping)) = replace_table_mapping_update {
            self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
                .await;
            version = self
                .notify_frontend(
                    NotificationOperation::Update,
                    NotificationInfo::ObjectGroup(PbObjectGroup { objects }),
                )
                .await;
        }
        if let Some(txs) = inner.creating_table_finish_notifier.remove(&job_id) {
            for tx in txs {
                let _ = tx.send(Ok(version));
            }
        }

        Ok(())
    }

    pub async fn finish_replace_streaming_job(
        &self,
        tmp_id: ObjectId,
        streaming_job: StreamingJob,
        merge_updates: HashMap<crate::model::FragmentId, Vec<MergeUpdate>>,
        col_index_mapping: Option<ColIndexMapping>,
        sink_into_table_context: SinkIntoTableContext,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let (objects, fragment_mapping) = Self::finish_replace_streaming_job_inner(
            tmp_id,
            merge_updates,
            col_index_mapping,
            sink_into_table_context,
            &txn,
            streaming_job,
        )
        .await?;

        txn.commit().await?;

        // FIXME: Do not notify frontend currently, because frontend nodes might refer to old table
        // catalog and need to access the old fragment. Let frontend nodes delete the old fragment
        // when they receive table catalog change.
        // self.notify_fragment_mapping(NotificationOperation::Delete, old_fragment_mappings)
        //     .await;
        self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
            .await;
        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup { objects }),
            )
            .await;

        Ok(version)
    }

    pub async fn finish_replace_streaming_job_inner(
        tmp_id: ObjectId,
        merge_updates: HashMap<crate::model::FragmentId, Vec<MergeUpdate>>,
        col_index_mapping: Option<ColIndexMapping>,
        SinkIntoTableContext {
            creating_sink_id,
            dropping_sink_id,
            updated_sink_catalogs,
        }: SinkIntoTableContext,
        txn: &DatabaseTransaction,
        streaming_job: StreamingJob,
    ) -> MetaResult<(Vec<PbObject>, Vec<PbFragmentWorkerSlotMapping>)> {
        let original_job_id = streaming_job.id() as ObjectId;
        let job_type = streaming_job.job_type();

        // Update catalog
        match streaming_job {
            StreamingJob::Table(_source, table, _table_job_type) => {
                // The source catalog should remain unchanged

                let original_table_catalogs = Table::find_by_id(original_job_id)
                    .select_only()
                    .columns([table::Column::Columns])
                    .into_tuple::<ColumnCatalogArray>()
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", original_job_id))?;

                // For sinks created in earlier versions, we need to set the original_target_columns.
                for sink_id in updated_sink_catalogs {
                    sink::ActiveModel {
                        sink_id: Set(sink_id as _),
                        original_target_columns: Set(Some(original_table_catalogs.clone())),
                        ..Default::default()
                    }
                    .update(txn)
                    .await?;
                }
                // Update the table catalog with the new one. (column catalog is also updated here)
                let mut table = table::ActiveModel::from(table);
                let mut incoming_sinks = table.incoming_sinks.as_ref().inner_ref().clone();
                if let Some(sink_id) = creating_sink_id {
                    debug_assert!(!incoming_sinks.contains(&{ sink_id }));
                    incoming_sinks.push(sink_id as _);
                }

                if let Some(sink_id) = dropping_sink_id {
                    let drained = incoming_sinks.extract_if(|id| *id == sink_id).collect_vec();
                    debug_assert_eq!(drained, vec![sink_id]);
                }

                table.incoming_sinks = Set(incoming_sinks.into());
                table.update(txn).await?;
            }
            StreamingJob::Source(source) => {
                // Update the source catalog with the new one.
                let source = source::ActiveModel::from(source);
                source.update(txn).await?;
            }
            _ => unreachable!(
                "invalid streaming job type: {:?}",
                streaming_job.job_type_str()
            ),
        }

        // 0. update internal tables
        // Fields including `fragment_id` were placeholder values before.
        // After table fragments are created, update them for all internal tables.
        let fragment_info: Vec<(FragmentId, I32Array)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::StateTableIds,
            ])
            .filter(fragment::Column::JobId.eq(tmp_id))
            .into_tuple()
            .all(txn)
            .await?;
        for (fragment_id, state_table_ids) in fragment_info {
            for state_table_id in state_table_ids.into_inner() {
                table::ActiveModel {
                    table_id: Set(state_table_id as _),
                    fragment_id: Set(Some(fragment_id)),
                    // No need to update `vnode_count` because it must remain the same.
                    ..Default::default()
                }
                .update(txn)
                .await?;
            }
        }

        // 1. replace old fragments/actors with new ones.
        Fragment::delete_many()
            .filter(fragment::Column::JobId.eq(original_job_id))
            .exec(txn)
            .await?;
        Fragment::update_many()
            .col_expr(fragment::Column::JobId, SimpleExpr::from(original_job_id))
            .filter(fragment::Column::JobId.eq(tmp_id))
            .exec(txn)
            .await?;

        // 2. update merges.
        // 2.1 update downstream actor's upstream_actor_ids
        for merge_update in merge_updates.values().flatten() {
            assert!(merge_update.removed_upstream_actor_id.is_empty());
            assert!(merge_update.new_upstream_fragment_id.is_some());
            let (actor_id, mut upstream_actors) =
                Actor::find_by_id(merge_update.actor_id as ActorId)
                    .select_only()
                    .columns([actor::Column::ActorId, actor::Column::UpstreamActorIds])
                    .into_tuple::<(ActorId, ActorUpstreamActors)>()
                    .one(txn)
                    .await?
                    .ok_or_else(|| {
                        MetaError::catalog_id_not_found("actor", merge_update.actor_id)
                    })?;

            assert!(upstream_actors
                .0
                .remove(&(merge_update.upstream_fragment_id as FragmentId))
                .is_some());
            upstream_actors.0.insert(
                merge_update.new_upstream_fragment_id.unwrap() as _,
                merge_update
                    .added_upstream_actor_id
                    .iter()
                    .map(|id| *id as _)
                    .collect(),
            );
            actor::ActiveModel {
                actor_id: Set(actor_id),
                upstream_actor_ids: Set(upstream_actors),
                ..Default::default()
            }
            .update(txn)
            .await?;
        }

        // 2.2 update downstream fragment's Merge node, and upstream_fragment_id
        for (fragment_id, merge_updates) in merge_updates {
            let (fragment_id, mut stream_node, mut upstream_fragment_id) =
                Fragment::find_by_id(fragment_id as FragmentId)
                    .select_only()
                    .columns([
                        fragment::Column::FragmentId,
                        fragment::Column::StreamNode,
                        fragment::Column::UpstreamFragmentId,
                    ])
                    .into_tuple::<(FragmentId, StreamNode, I32Array)>()
                    .one(txn)
                    .await?
                    .map(|(id, node, upstream)| (id, node.to_protobuf(), upstream))
                    .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;
            let fragment_replace_map: HashMap<_, _> = merge_updates
                .iter()
                .map(|update| {
                    (
                        update.upstream_fragment_id,
                        update.new_upstream_fragment_id.unwrap(),
                    )
                })
                .collect();

            visit_stream_node_mut(&mut stream_node, |body| {
                if let PbNodeBody::Merge(m) = body
                    && let Some(new_fragment_id) = fragment_replace_map.get(&m.upstream_fragment_id)
                {
                    m.upstream_fragment_id = *new_fragment_id;
                }
            });
            for fragment_id in &mut upstream_fragment_id.0 {
                if let Some(new_fragment_id) = fragment_replace_map.get(&(*fragment_id as _)) {
                    *fragment_id = *new_fragment_id as _;
                }
            }
            fragment::ActiveModel {
                fragment_id: Set(fragment_id),
                stream_node: Set(StreamNode::from(&stream_node)),
                upstream_fragment_id: Set(upstream_fragment_id),
                ..Default::default()
            }
            .update(txn)
            .await?;
        }

        // 3. remove dummy object.
        Object::delete_by_id(tmp_id).exec(txn).await?;

        // 4. update catalogs and notify.
        let mut objects = vec![];
        match job_type {
            StreamingJobType::Table(_) => {
                let (table, table_obj) = Table::find_by_id(original_job_id)
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", original_job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Table(
                        ObjectModel(table, table_obj.unwrap()).into(),
                    )),
                })
            }
            StreamingJobType::Source => {
                let (source, source_obj) = Source::find_by_id(original_job_id)
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", original_job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Source(
                        ObjectModel(source, source_obj.unwrap()).into(),
                    )),
                })
            }
            _ => unreachable!("invalid streaming job type for replace: {:?}", job_type),
        }
        if let Some(table_col_index_mapping) = col_index_mapping {
            let expr_rewriter = ReplaceTableExprRewriter {
                table_col_index_mapping,
            };

            let index_items: Vec<(IndexId, ExprNodeArray)> = Index::find()
                .select_only()
                .columns([index::Column::IndexId, index::Column::IndexItems])
                .filter(index::Column::PrimaryTableId.eq(original_job_id))
                .into_tuple()
                .all(txn)
                .await?;
            for (index_id, nodes) in index_items {
                let mut pb_nodes = nodes.to_protobuf();
                pb_nodes
                    .iter_mut()
                    .for_each(|x| expr_rewriter.rewrite_expr(x));
                let index = index::ActiveModel {
                    index_id: Set(index_id),
                    index_items: Set(pb_nodes.into()),
                    ..Default::default()
                }
                .update(txn)
                .await?;
                let index_obj = index
                    .find_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", index.index_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(ObjectModel(index, index_obj).into())),
                });
            }
        }

        let fragment_mapping: Vec<_> = get_fragment_mappings(txn, original_job_id as _).await?;

        Ok((objects, fragment_mapping))
    }

    /// Abort the replacing streaming job by deleting the temporary job object.
    pub async fn try_abort_replacing_streaming_job(&self, tmp_job_id: ObjectId) -> MetaResult<()> {
        let inner = self.inner.write().await;
        Object::delete_by_id(tmp_job_id).exec(&inner.db).await?;
        Ok(())
    }

    // edit the `rate_limit` of the `Source` node in given `source_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_source_rate_limit_by_source_id(
        &self,
        source_id: SourceId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        {
            let active_source = source::ActiveModel {
                source_id: Set(source_id),
                rate_limit: Set(rate_limit.map(|v| v as i32)),
                ..Default::default()
            };
            active_source.update(&txn).await?;
        }

        let (source, obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;

        let is_fs_source = source.with_properties.inner_ref().is_new_fs_connector();
        let streaming_job_ids: Vec<ObjectId> =
            if let Some(table_id) = source.optional_associated_table_id {
                vec![table_id]
            } else if let Some(source_info) = &source.source_info
                && source_info.to_protobuf().is_shared()
            {
                vec![source_id]
            } else {
                ObjectDependency::find()
                    .select_only()
                    .column(object_dependency::Column::UsedBy)
                    .filter(object_dependency::Column::Oid.eq(source_id))
                    .into_tuple()
                    .all(&txn)
                    .await?
            };

        if streaming_job_ids.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "source id {source_id} not used by any streaming job"
            )));
        }

        let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment::Column::JobId.is_in(streaming_job_ids))
            .into_tuple()
            .all(&txn)
            .await?;
        let mut fragments = fragments
            .into_iter()
            .map(|(id, mask, stream_node)| (id, mask, stream_node.to_protobuf()))
            .collect_vec();

        fragments.retain_mut(|(_, fragment_type_mask, stream_node)| {
            let mut found = false;
            if *fragment_type_mask & PbFragmentTypeFlag::Source as i32 != 0 {
                visit_stream_node_mut(stream_node, |node| {
                    if let PbNodeBody::Source(node) = node {
                        if let Some(node_inner) = &mut node.source_inner
                            && node_inner.source_id == source_id as u32
                        {
                            node_inner.rate_limit = rate_limit;
                            found = true;
                        }
                    }
                });
            }
            if is_fs_source {
                // in older versions, there's no fragment type flag for `FsFetch` node,
                // so we just scan all fragments for StreamFsFetch node if using fs connector
                visit_stream_node_mut(stream_node, |node| {
                    if let PbNodeBody::StreamFsFetch(node) = node {
                        *fragment_type_mask |= PbFragmentTypeFlag::FsFetch as i32;
                        if let Some(node_inner) = &mut node.node_inner
                            && node_inner.source_id == source_id as u32
                        {
                            node_inner.rate_limit = rate_limit;
                            found = true;
                        }
                    }
                });
            }
            found
        });

        assert!(
            !fragments.is_empty(),
            "source id should be used by at least one fragment"
        );
        let fragment_ids = fragments.iter().map(|(id, _, _)| *id).collect_vec();
        for (id, fragment_type_mask, stream_node) in fragments {
            fragment::ActiveModel {
                fragment_id: Set(id),
                fragment_type_mask: Set(fragment_type_mask),
                stream_node: Set(StreamNode::from(&stream_node)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        let fragment_actors = get_fragment_actor_ids(&txn, fragment_ids).await?;

        txn.commit().await?;

        let relation_info = PbObjectInfo::Source(ObjectModel(source, obj.unwrap()).into());
        let _version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(relation_info),
                    }],
                }),
            )
            .await;

        Ok(fragment_actors)
    }

    // edit the content of fragments in given `table_id`
    // return the actor_ids to be applied
    async fn mutate_fragments_by_job_id(
        &self,
        job_id: ObjectId,
        // returns true if the mutation is applied
        mut fragments_mutation_fn: impl FnMut(&mut i32, &mut PbStreamNode) -> bool,
        // error message when no relevant fragments is found
        err_msg: &'static str,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment::Column::JobId.eq(job_id))
            .into_tuple()
            .all(&txn)
            .await?;
        let mut fragments = fragments
            .into_iter()
            .map(|(id, mask, stream_node)| (id, mask, stream_node.to_protobuf()))
            .collect_vec();

        fragments.retain_mut(|(_, fragment_type_mask, stream_node)| {
            fragments_mutation_fn(fragment_type_mask, stream_node)
        });
        if fragments.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "job id {job_id}: {}",
                err_msg
            )));
        }

        let fragment_ids = fragments.iter().map(|(id, _, _)| *id).collect_vec();
        for (id, _, stream_node) in fragments {
            fragment::ActiveModel {
                fragment_id: Set(id),
                stream_node: Set(StreamNode::from(&stream_node)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        let fragment_actors = get_fragment_actor_ids(&txn, fragment_ids).await?;

        txn.commit().await?;

        Ok(fragment_actors)
    }

    // edit the `rate_limit` of the `Chain` node in given `table_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_backfill_rate_limit_by_job_id(
        &self,
        job_id: ObjectId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let update_backfill_rate_limit =
            |fragment_type_mask: &mut i32, stream_node: &mut PbStreamNode| {
                let mut found = false;
                if *fragment_type_mask & PbFragmentTypeFlag::backfill_rate_limit_fragments() != 0 {
                    visit_stream_node_mut(stream_node, |node| match node {
                        PbNodeBody::StreamCdcScan(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::StreamScan(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::SourceBackfill(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::Sink(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        _ => {}
                    });
                }
                found
            };

        self.mutate_fragments_by_job_id(
            job_id,
            update_backfill_rate_limit,
            "stream scan node or source node not found",
        )
        .await
    }

    // edit the `rate_limit` of the `Sink` node in given `table_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_sink_rate_limit_by_job_id(
        &self,
        job_id: ObjectId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let update_sink_rate_limit =
            |fragment_type_mask: &mut i32, stream_node: &mut PbStreamNode| {
                let mut found = false;
                if *fragment_type_mask & PbFragmentTypeFlag::sink_rate_limit_fragments() != 0 {
                    visit_stream_node_mut(stream_node, |node| {
                        if let PbNodeBody::Sink(node) = node {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                    });
                }
                found
            };

        self.mutate_fragments_by_job_id(job_id, update_sink_rate_limit, "sink node not found")
            .await
    }

    pub async fn update_dml_rate_limit_by_job_id(
        &self,
        job_id: ObjectId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let update_dml_rate_limit =
            |fragment_type_mask: &mut i32, stream_node: &mut PbStreamNode| {
                let mut found = false;
                if *fragment_type_mask & PbFragmentTypeFlag::dml_rate_limit_fragments() != 0 {
                    visit_stream_node_mut(stream_node, |node| {
                        if let PbNodeBody::Dml(node) = node {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                    });
                }
                found
            };

        self.mutate_fragments_by_job_id(job_id, update_dml_rate_limit, "dml node not found")
            .await
    }

    pub async fn post_apply_reschedules(
        &self,
        reschedules: HashMap<FragmentId, Reschedule>,
        post_updates: &JobReschedulePostUpdates,
    ) -> MetaResult<()> {
        fn update_actors(
            actors: &mut Vec<ActorId>,
            to_remove: &HashSet<ActorId>,
            to_create: &Vec<ActorId>,
        ) {
            let actor_id_set: HashSet<_> = actors.iter().copied().collect();
            for actor_id in to_create {
                debug_assert!(!actor_id_set.contains(actor_id));
            }
            for actor_id in to_remove {
                debug_assert!(actor_id_set.contains(actor_id));
            }

            actors.retain(|actor_id| !to_remove.contains(actor_id));
            actors.extend_from_slice(to_create);
        }

        let new_created_actors: HashSet<_> = reschedules
            .values()
            .flat_map(|reschedule| {
                reschedule
                    .added_actors
                    .values()
                    .flatten()
                    .map(|actor_id| *actor_id as ActorId)
            })
            .collect();

        let inner = self.inner.write().await;

        let txn = inner.db.begin().await?;

        let mut fragment_mapping_to_notify = vec![];

        // for assert only
        let mut assert_dispatcher_update_checker = HashSet::new();

        for (
            fragment_id,
            Reschedule {
                added_actors,
                removed_actors,
                vnode_bitmap_updates,
                actor_splits,
                newly_created_actors,
                upstream_fragment_dispatcher_ids,
                upstream_dispatcher_mapping,
                downstream_fragment_ids,
            },
        ) in reschedules
        {
            // drop removed actors
            Actor::delete_many()
                .filter(
                    actor::Column::ActorId
                        .is_in(removed_actors.iter().map(|id| *id as ActorId).collect_vec()),
                )
                .exec(&txn)
                .await?;

            // add new actors
            for (
                (
                    PbStreamActor {
                        actor_id,
                        fragment_id,
                        dispatcher,
                        vnode_bitmap,
                        expr_context,
                        ..
                    },
                    node_actor_upstreams,
                ),
                actor_status,
            ) in newly_created_actors
            {
                let mut actor_upstreams = BTreeMap::<FragmentId, BTreeSet<ActorId>>::new();
                let mut new_actor_dispatchers = vec![];

                for (fragment_id, upstream_actor_ids) in node_actor_upstreams {
                    actor_upstreams
                        .entry(fragment_id as FragmentId)
                        .or_default()
                        .extend(upstream_actor_ids.iter().map(|id| *id as ActorId));
                }

                let actor_upstreams: BTreeMap<FragmentId, Vec<ActorId>> = actor_upstreams
                    .into_iter()
                    .map(|(k, v)| (k, v.into_iter().collect()))
                    .collect();

                let actor_upstreams = ActorUpstreamActors(actor_upstreams);

                let splits = actor_splits
                    .get(&actor_id)
                    .map(|splits| splits.iter().map(PbConnectorSplit::from).collect_vec());

                Actor::insert(actor::ActiveModel {
                    actor_id: Set(actor_id as _),
                    fragment_id: Set(fragment_id as _),
                    status: Set(ActorStatus::Running),
                    splits: Set(splits.map(|splits| (&PbConnectorSplits { splits }).into())),
                    worker_id: Set(actor_status.worker_id() as _),
                    upstream_actor_ids: Set(actor_upstreams),
                    vnode_bitmap: Set(vnode_bitmap.as_ref().map(|bitmap| bitmap.into())),
                    expr_context: Set(expr_context.as_ref().unwrap().into()),
                })
                .exec(&txn)
                .await?;

                for PbDispatcher {
                    r#type: dispatcher_type,
                    dist_key_indices,
                    output_indices,
                    hash_mapping,
                    dispatcher_id,
                    downstream_actor_id,
                } in dispatcher
                {
                    new_actor_dispatchers.push(actor_dispatcher::ActiveModel {
                        id: Default::default(),
                        actor_id: Set(actor_id as _),
                        dispatcher_type: Set(PbDispatcherType::try_from(dispatcher_type)
                            .unwrap()
                            .into()),
                        dist_key_indices: Set(dist_key_indices.into()),
                        output_indices: Set(output_indices.into()),
                        hash_mapping: Set(hash_mapping.as_ref().map(|mapping| mapping.into())),
                        dispatcher_id: Set(dispatcher_id as _),
                        downstream_actor_ids: Set(downstream_actor_id.into()),
                    })
                }
                if !new_actor_dispatchers.is_empty() {
                    ActorDispatcher::insert_many(new_actor_dispatchers)
                        .exec(&txn)
                        .await?;
                }
            }

            // actor update
            for (actor_id, bitmap) in vnode_bitmap_updates {
                let actor = Actor::find_by_id(actor_id as ActorId)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("actor", actor_id))?;

                let mut actor = actor.into_active_model();
                actor.vnode_bitmap = Set(Some((&bitmap.to_protobuf()).into()));
                actor.update(&txn).await?;
            }

            // Update actor_splits for existing actors
            for (actor_id, splits) in actor_splits {
                if new_created_actors.contains(&(actor_id as ActorId)) {
                    continue;
                }

                let actor = Actor::find_by_id(actor_id as ActorId)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("actor", actor_id))?;

                let mut actor = actor.into_active_model();
                let splits = splits.iter().map(PbConnectorSplit::from).collect_vec();
                actor.splits = Set(Some((&PbConnectorSplits { splits }).into()));
                actor.update(&txn).await?;
            }

            // fragment update
            let fragment = Fragment::find_by_id(fragment_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;

            let job_actors = fragment
                .find_related(Actor)
                .all(&txn)
                .await?
                .into_iter()
                .map(|actor| {
                    (
                        fragment_id,
                        fragment.distribution_type,
                        actor.actor_id,
                        actor.vnode_bitmap,
                        actor.worker_id,
                        actor.status,
                    )
                })
                .collect_vec();

            fragment_mapping_to_notify.extend(rebuild_fragment_mapping_from_actors(job_actors));

            // for downstream and upstream
            let removed_actor_ids: HashSet<_> = removed_actors
                .iter()
                .map(|actor_id| *actor_id as ActorId)
                .collect();

            let added_actor_ids = added_actors
                .values()
                .flatten()
                .map(|actor_id| *actor_id as ActorId)
                .collect_vec();

            // first step, upstream fragment
            for (upstream_fragment_id, dispatcher_id) in upstream_fragment_dispatcher_ids {
                let upstream_fragment = Fragment::find_by_id(upstream_fragment_id as FragmentId)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;

                let all_dispatchers = actor_dispatcher::Entity::find()
                    .join(JoinType::InnerJoin, actor_dispatcher::Relation::Actor.def())
                    .filter(actor::Column::FragmentId.eq(upstream_fragment.fragment_id))
                    .filter(actor_dispatcher::Column::DispatcherId.eq(dispatcher_id as i32))
                    .all(&txn)
                    .await?;

                for dispatcher in all_dispatchers {
                    debug_assert!(assert_dispatcher_update_checker.insert(dispatcher.id));
                    if new_created_actors.contains(&dispatcher.actor_id) {
                        continue;
                    }

                    let mut dispatcher = dispatcher.into_active_model();

                    // Only hash dispatcher needs mapping
                    if dispatcher.dispatcher_type.as_ref() == &DispatcherType::Hash {
                        dispatcher.hash_mapping = Set(upstream_dispatcher_mapping
                            .as_ref()
                            .map(|m| risingwave_meta_model::ActorMapping::from(&m.to_protobuf())));
                    }

                    let mut new_downstream_actor_ids =
                        dispatcher.downstream_actor_ids.as_ref().inner_ref().clone();

                    update_actors(
                        new_downstream_actor_ids.as_mut(),
                        &removed_actor_ids,
                        &added_actor_ids,
                    );

                    dispatcher.downstream_actor_ids = Set(new_downstream_actor_ids.into());
                    dispatcher.update(&txn).await?;
                }
            }

            // second step, downstream fragment
            for downstream_fragment_id in downstream_fragment_ids {
                let actors = Actor::find()
                    .filter(actor::Column::FragmentId.eq(downstream_fragment_id as FragmentId))
                    .all(&txn)
                    .await?;

                for actor in actors {
                    if new_created_actors.contains(&actor.actor_id) {
                        continue;
                    }

                    let mut actor = actor.into_active_model();

                    let mut new_upstream_actor_ids =
                        actor.upstream_actor_ids.as_ref().inner_ref().clone();

                    update_actors(
                        new_upstream_actor_ids.get_mut(&fragment_id).unwrap(),
                        &removed_actor_ids,
                        &added_actor_ids,
                    );

                    actor.upstream_actor_ids = Set(new_upstream_actor_ids.into());

                    actor.update(&txn).await?;
                }
            }
        }

        let JobReschedulePostUpdates {
            parallelism_updates,
            resource_group_updates,
        } = post_updates;

        for (table_id, parallelism) in parallelism_updates {
            let mut streaming_job = StreamingJobModel::find_by_id(table_id.table_id() as ObjectId)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?
                .into_active_model();

            streaming_job.parallelism = Set(match parallelism {
                TableParallelism::Adaptive => StreamingParallelism::Adaptive,
                TableParallelism::Fixed(n) => StreamingParallelism::Fixed(*n as _),
                TableParallelism::Custom => StreamingParallelism::Custom,
            });

            if let Some(resource_group) =
                resource_group_updates.get(&(table_id.table_id() as ObjectId))
            {
                streaming_job.specific_resource_group = Set(resource_group.to_owned());
            }

            streaming_job.update(&txn).await?;
        }

        txn.commit().await?;
        self.notify_fragment_mapping(Operation::Update, fragment_mapping_to_notify)
            .await;

        Ok(())
    }

    /// Note: `FsFetch` created in old versions are not included.
    /// Since this is only used for debugging, it should be fine.
    pub async fn list_rate_limits(&self) -> MetaResult<Vec<RateLimitInfo>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments: Vec<(FragmentId, ObjectId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment_type_mask_intersects(
                PbFragmentTypeFlag::rate_limit_fragments(),
            ))
            .into_tuple()
            .all(&txn)
            .await?;

        let mut rate_limits = Vec::new();
        for (fragment_id, job_id, fragment_type_mask, stream_node) in fragments {
            let stream_node = stream_node.to_protobuf();
            let mut rate_limit = None;
            let mut node_name = None;

            visit_stream_node(&stream_node, |node| {
                match node {
                    // source rate limit
                    PbNodeBody::Source(node) => {
                        if let Some(node_inner) = &node.source_inner {
                            debug_assert!(
                                rate_limit.is_none(),
                                "one fragment should only have 1 rate limit node"
                            );
                            rate_limit = node_inner.rate_limit;
                            node_name = Some("SOURCE");
                        }
                    }
                    PbNodeBody::StreamFsFetch(node) => {
                        if let Some(node_inner) = &node.node_inner {
                            debug_assert!(
                                rate_limit.is_none(),
                                "one fragment should only have 1 rate limit node"
                            );
                            rate_limit = node_inner.rate_limit;
                            node_name = Some("FS_FETCH");
                        }
                    }
                    // backfill rate limit
                    PbNodeBody::SourceBackfill(node) => {
                        debug_assert!(
                            rate_limit.is_none(),
                            "one fragment should only have 1 rate limit node"
                        );
                        rate_limit = node.rate_limit;
                        node_name = Some("SOURCE_BACKFILL");
                    }
                    PbNodeBody::StreamScan(node) => {
                        debug_assert!(
                            rate_limit.is_none(),
                            "one fragment should only have 1 rate limit node"
                        );
                        rate_limit = node.rate_limit;
                        node_name = Some("STREAM_SCAN");
                    }
                    PbNodeBody::StreamCdcScan(node) => {
                        debug_assert!(
                            rate_limit.is_none(),
                            "one fragment should only have 1 rate limit node"
                        );
                        rate_limit = node.rate_limit;
                        node_name = Some("STREAM_CDC_SCAN");
                    }
                    PbNodeBody::Sink(node) => {
                        debug_assert!(
                            rate_limit.is_none(),
                            "one fragment should only have 1 rate limit node"
                        );
                        rate_limit = node.rate_limit;
                        node_name = Some("SINK");
                    }
                    _ => {}
                }
            });

            if let Some(rate_limit) = rate_limit {
                rate_limits.push(RateLimitInfo {
                    fragment_id: fragment_id as u32,
                    job_id: job_id as u32,
                    fragment_type_mask: fragment_type_mask as u32,
                    rate_limit,
                    node_name: node_name.unwrap().to_owned(),
                });
            }
        }

        Ok(rate_limits)
    }
}

fn bitflag_intersects(column: SimpleExpr, value: i32) -> SimpleExpr {
    column
        .binary(BinOper::Custom("&"), value)
        .binary(BinOper::NotEqual, 0)
}

fn fragment_type_mask_intersects(value: i32) -> SimpleExpr {
    bitflag_intersects(fragment::Column::FragmentTypeMask.into_simple_expr(), value)
}

pub struct SinkIntoTableContext {
    /// For creating sink into table, this is `Some`, otherwise `None`.
    pub creating_sink_id: Option<SinkId>,
    /// For dropping sink into table, this is `Some`, otherwise `None`.
    pub dropping_sink_id: Option<SinkId>,
    /// For alter table (e.g., add column), this is the list of existing sink ids
    /// otherwise empty.
    pub updated_sink_catalogs: Vec<SinkId>,
}
