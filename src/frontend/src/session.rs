// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use bytes::Bytes;
use either::Either;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use pgwire::error::{PsqlError, PsqlResult};
use pgwire::net::{Address, AddressRef};
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_message::TransactionStatus;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::pg_server::{
    BoxedError, ExecContext, ExecContextGuard, Session, SessionId, SessionManager,
    UserAuthenticator,
};
use pgwire::types::{Format, FormatIterator};
use rand::RngCore;
use risingwave_batch::monitor::{BatchSpillMetrics, GLOBAL_BATCH_SPILL_METRICS};
use risingwave_batch::spill::spill_op::SpillOp;
use risingwave_batch::task::{ShutdownSender, ShutdownToken};
use risingwave_batch::worker_manager::worker_node_manager::{
    WorkerNodeManager, WorkerNodeManagerRef,
};
use risingwave_common::acl::AclMode;
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
#[cfg(test)]
use risingwave_common::catalog::{
    DEFAULT_DATABASE_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID,
};
use risingwave_common::config::{
    load_config, BatchConfig, MetaConfig, MetricLevel, StreamingConfig,
};
use risingwave_common::memory::MemoryContext;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::session_config::{ConfigReporter, SessionConfig, VisibilityMode};
use risingwave_common::system_param::local_manager::{
    LocalSystemParamsManager, LocalSystemParamsManagerRef,
};
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_common::types::DataType;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::cluster_limit;
use risingwave_common::util::cluster_limit::ActorCountPerParallelism;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::pretty_bytes::convert;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_heap_profiling::HeapProfiler;
use risingwave_common_service::{MetricsManager, ObserverManager};
use risingwave_connector::source::monitor::{SourceMetrics, GLOBAL_SOURCE_METRICS};
use risingwave_pb::common::worker_node::Property as AddWorkerNodeProperty;
use risingwave_pb::common::WorkerType;
use risingwave_pb::frontend_service::frontend_service_server::FrontendServiceServer;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::user::auth_info::EncryptionType;
use risingwave_pb::user::grant_privilege::Object;
use risingwave_rpc_client::{ComputeClientPool, ComputeClientPoolRef, MetaClient};
use risingwave_sqlparser::ast::{ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::oneshot::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::info;
use tracing::log::error;

use self::cursor_manager::CursorManager;
use crate::binder::{Binder, BoundStatement, ResolveQualifiedNameError};
use crate::catalog::catalog_service::{CatalogReader, CatalogWriter, CatalogWriterImpl};
use crate::catalog::connection_catalog::ConnectionCatalog;
use crate::catalog::root_catalog::{Catalog, SchemaPath};
use crate::catalog::secret_catalog::SecretCatalog;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::catalog::{
    check_schema_writable, CatalogError, DatabaseId, OwnedByUserCatalog, SchemaId, TableId,
};
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::describe::infer_describe;
use crate::handler::extended_handle::{
    handle_bind, handle_execute, handle_parse, Portal, PrepareStatement,
};
use crate::handler::privilege::ObjectCheckItem;
use crate::handler::show::{infer_show_create_object, infer_show_object};
use crate::handler::util::to_pg_field;
use crate::handler::variable::infer_show_variable;
use crate::handler::{handle, RwPgResponse};
use crate::health_service::HealthServiceImpl;
use crate::meta_client::{FrontendMetaClient, FrontendMetaClientImpl};
use crate::monitor::{CursorMetrics, FrontendMetrics, GLOBAL_FRONTEND_METRICS};
use crate::observer::FrontendObserverNode;
use crate::rpc::FrontendServiceImpl;
use crate::scheduler::streaming_manager::{StreamingJobTracker, StreamingJobTrackerRef};
use crate::scheduler::{
    DistributedQueryMetrics, HummockSnapshotManager, HummockSnapshotManagerRef, QueryManager,
    GLOBAL_DISTRIBUTED_QUERY_METRICS,
};
use crate::telemetry::FrontendTelemetryCreator;
use crate::user::user_authentication::md5_hash_with_salt;
use crate::user::user_manager::UserInfoManager;
use crate::user::user_service::{UserInfoReader, UserInfoWriter, UserInfoWriterImpl};
use crate::user::UserId;
use crate::{FrontendOpts, PgResponseStream, TableCatalog};

pub(crate) mod current;
pub(crate) mod cursor_manager;
pub(crate) mod transaction;

/// The global environment for the frontend server.
#[derive(Clone)]
pub struct FrontendEnv {
    // Different session may access catalog at the same time and catalog is protected by a
    // RwLock.
    meta_client: Arc<dyn FrontendMetaClient>,
    catalog_writer: Arc<dyn CatalogWriter>,
    catalog_reader: CatalogReader,
    user_info_writer: Arc<dyn UserInfoWriter>,
    user_info_reader: UserInfoReader,
    worker_node_manager: WorkerNodeManagerRef,
    query_manager: QueryManager,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    system_params_manager: LocalSystemParamsManagerRef,
    session_params: Arc<RwLock<SessionConfig>>,

    server_addr: HostAddr,
    client_pool: ComputeClientPoolRef,

    /// Each session is identified by (`process_id`,
    /// `secret_key`). When Cancel Request received, find corresponding session and cancel all
    /// running queries.
    sessions_map: SessionMapRef,

    pub frontend_metrics: Arc<FrontendMetrics>,

    pub cursor_metrics: Arc<CursorMetrics>,

    source_metrics: Arc<SourceMetrics>,

    /// Batch spill metrics
    spill_metrics: Arc<BatchSpillMetrics>,

    batch_config: BatchConfig,
    meta_config: MetaConfig,
    streaming_config: StreamingConfig,

    /// Track creating streaming jobs, used to cancel creating streaming job when cancel request
    /// received.
    creating_streaming_job_tracker: StreamingJobTrackerRef,

    /// Runtime for compute intensive tasks in frontend, e.g. executors in local mode,
    /// root stage in mpp mode.
    compute_runtime: Arc<BackgroundShutdownRuntime>,

    /// Memory context used for batch executors in frontend.
    mem_context: MemoryContext,
}

/// Session map identified by `(process_id, secret_key)`
pub type SessionMapRef = Arc<RwLock<HashMap<(i32, i32), Arc<SessionImpl>>>>;

/// The proportion of frontend memory used for batch processing.
const FRONTEND_BATCH_MEMORY_PROPORTION: f64 = 0.5;

impl FrontendEnv {
    pub fn mock() -> Self {
        use crate::test_utils::{MockCatalogWriter, MockFrontendMetaClient, MockUserInfoWriter};

        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let meta_client = Arc::new(MockFrontendMetaClient {});
        let hummock_snapshot_manager = Arc::new(HummockSnapshotManager::new(meta_client.clone()));
        let catalog_writer = Arc::new(MockCatalogWriter::new(
            catalog.clone(),
            hummock_snapshot_manager.clone(),
        ));
        let catalog_reader = CatalogReader::new(catalog);
        let user_info_manager = Arc::new(RwLock::new(UserInfoManager::default()));
        let user_info_writer = Arc::new(MockUserInfoWriter::new(user_info_manager.clone()));
        let user_info_reader = UserInfoReader::new(user_info_manager);
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(vec![]));
        let system_params_manager = Arc::new(LocalSystemParamsManager::for_test());
        let compute_client_pool = Arc::new(ComputeClientPool::for_test());
        let query_manager = QueryManager::new(
            worker_node_manager.clone(),
            compute_client_pool,
            catalog_reader.clone(),
            Arc::new(DistributedQueryMetrics::for_test()),
            None,
            None,
        );
        let server_addr = HostAddr::try_from("127.0.0.1:4565").unwrap();
        let client_pool = Arc::new(ComputeClientPool::for_test());
        let creating_streaming_tracker = StreamingJobTracker::new(meta_client.clone());
        let compute_runtime = Arc::new(BackgroundShutdownRuntime::from(
            Builder::new_multi_thread()
                .worker_threads(
                    load_config("", FrontendOpts::default())
                        .batch
                        .frontend_compute_runtime_worker_threads,
                )
                .thread_name("rw-batch-local")
                .enable_all()
                .build()
                .unwrap(),
        ));
        let sessions_map = Arc::new(RwLock::new(HashMap::new()));
        Self {
            meta_client,
            catalog_writer,
            catalog_reader,
            user_info_writer,
            user_info_reader,
            worker_node_manager,
            query_manager,
            hummock_snapshot_manager,
            system_params_manager,
            session_params: Default::default(),
            server_addr,
            client_pool,
            sessions_map: sessions_map.clone(),
            frontend_metrics: Arc::new(FrontendMetrics::for_test()),
            cursor_metrics: Arc::new(CursorMetrics::for_test()),
            batch_config: BatchConfig::default(),
            meta_config: MetaConfig::default(),
            streaming_config: StreamingConfig::default(),
            source_metrics: Arc::new(SourceMetrics::default()),
            spill_metrics: BatchSpillMetrics::for_test(),
            creating_streaming_job_tracker: Arc::new(creating_streaming_tracker),
            compute_runtime,
            mem_context: MemoryContext::none(),
        }
    }

    pub async fn init(opts: FrontendOpts) -> Result<(Self, Vec<JoinHandle<()>>, Vec<Sender<()>>)> {
        let config = load_config(&opts.config_path, &opts);
        info!("Starting frontend node");
        info!("> config: {:?}", config);
        info!(
            "> debug assertions: {}",
            if cfg!(debug_assertions) { "on" } else { "off" }
        );
        info!("> version: {} ({})", RW_VERSION, GIT_SHA);

        let frontend_address: HostAddr = opts
            .advertise_addr
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("advertise addr is not specified, defaulting to listen_addr");
                &opts.listen_addr
            })
            .parse()
            .unwrap();
        info!("advertise addr is {}", frontend_address);

        let rpc_addr: HostAddr = opts.frontend_rpc_listener_addr.parse().unwrap();
        let internal_rpc_host_addr = HostAddr {
            // Use the host of advertise address for the frontend rpc address.
            host: frontend_address.host.clone(),
            port: rpc_addr.port,
        };
        // Register in meta by calling `AddWorkerNode` RPC.
        let (meta_client, system_params_reader) = MetaClient::register_new(
            opts.meta_addr,
            WorkerType::Frontend,
            &frontend_address,
            AddWorkerNodeProperty {
                internal_rpc_host_addr: internal_rpc_host_addr.to_string(),
                ..Default::default()
            },
            &config.meta,
        )
        .await;

        let worker_id = meta_client.worker_id();
        info!("Assigned worker node id {}", worker_id);

        let (heartbeat_join_handle, heartbeat_shutdown_sender) = MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
        );
        let mut join_handles = vec![heartbeat_join_handle];
        let mut shutdown_senders = vec![heartbeat_shutdown_sender];

        let frontend_meta_client = Arc::new(FrontendMetaClientImpl(meta_client.clone()));
        let hummock_snapshot_manager =
            Arc::new(HummockSnapshotManager::new(frontend_meta_client.clone()));

        let (catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let catalog_writer = Arc::new(CatalogWriterImpl::new(
            meta_client.clone(),
            catalog_updated_rx,
            hummock_snapshot_manager.clone(),
        ));
        let catalog_reader = CatalogReader::new(catalog.clone());

        let worker_node_manager = Arc::new(WorkerNodeManager::new());

        let compute_client_pool = Arc::new(ComputeClientPool::new(
            config.batch_exchange_connection_pool_size(),
        ));
        let query_manager = QueryManager::new(
            worker_node_manager.clone(),
            compute_client_pool.clone(),
            catalog_reader.clone(),
            Arc::new(GLOBAL_DISTRIBUTED_QUERY_METRICS.clone()),
            config.batch.distributed_query_limit,
            config.batch.max_batch_queries_per_frontend_node,
        );

        let user_info_manager = Arc::new(RwLock::new(UserInfoManager::default()));
        let (user_info_updated_tx, user_info_updated_rx) = watch::channel(0);
        let user_info_reader = UserInfoReader::new(user_info_manager.clone());
        let user_info_writer = Arc::new(UserInfoWriterImpl::new(
            meta_client.clone(),
            user_info_updated_rx,
        ));

        let system_params_manager =
            Arc::new(LocalSystemParamsManager::new(system_params_reader.clone()));

        LocalSecretManager::init(
            opts.temp_secret_file_dir,
            meta_client.cluster_id().to_owned(),
            worker_id,
        );

        // This `session_params` should be initialized during the initial notification in `observer_manager`
        let session_params = Arc::new(RwLock::new(SessionConfig::default()));
        let sessions_map: SessionMapRef = Arc::new(RwLock::new(HashMap::new()));
        let cursor_metrics = Arc::new(CursorMetrics::init(sessions_map.clone()));

        let frontend_observer_node = FrontendObserverNode::new(
            worker_node_manager.clone(),
            catalog,
            catalog_updated_tx,
            user_info_manager,
            user_info_updated_tx,
            hummock_snapshot_manager.clone(),
            system_params_manager.clone(),
            session_params.clone(),
            compute_client_pool.clone(),
        );
        let observer_manager =
            ObserverManager::new_with_meta_client(meta_client.clone(), frontend_observer_node)
                .await;
        let observer_join_handle = observer_manager.start().await;
        join_handles.push(observer_join_handle);

        meta_client.activate(&frontend_address).await?;

        let frontend_metrics = Arc::new(GLOBAL_FRONTEND_METRICS.clone());
        let source_metrics = Arc::new(GLOBAL_SOURCE_METRICS.clone());
        let spill_metrics = Arc::new(GLOBAL_BATCH_SPILL_METRICS.clone());

        if config.server.metrics_level > MetricLevel::Disabled {
            MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone());
        }

        let health_srv = HealthServiceImpl::new();
        let frontend_srv = FrontendServiceImpl::new();
        let frontend_rpc_addr = opts.frontend_rpc_listener_addr.parse().unwrap();

        let telemetry_manager = TelemetryManager::new(
            Arc::new(meta_client.clone()),
            Arc::new(FrontendTelemetryCreator::new()),
        );

        // if the toml config file or env variable disables telemetry, do not watch system params
        // change because if any of configs disable telemetry, we should never start it
        if config.server.telemetry_enabled && telemetry_env_enabled() {
            let (join_handle, shutdown_sender) = telemetry_manager.start().await;
            join_handles.push(join_handle);
            shutdown_senders.push(shutdown_sender);
        } else {
            tracing::info!("Telemetry didn't start due to config");
        }

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealthServer::new(health_srv))
                .add_service(FrontendServiceServer::new(frontend_srv))
                .serve(frontend_rpc_addr)
                .await
                .unwrap();
        });
        info!(
            "Health Check RPC Listener is set up on {}",
            opts.frontend_rpc_listener_addr.clone()
        );

        let creating_streaming_job_tracker =
            Arc::new(StreamingJobTracker::new(frontend_meta_client.clone()));

        let compute_runtime = Arc::new(BackgroundShutdownRuntime::from(
            Builder::new_multi_thread()
                .worker_threads(config.batch.frontend_compute_runtime_worker_threads)
                .thread_name("rw-batch-local")
                .enable_all()
                .build()
                .unwrap(),
        ));

        let sessions = sessions_map.clone();
        // Idle transaction background monitor
        let join_handle = tokio::spawn(async move {
            let mut check_idle_txn_interval =
                tokio::time::interval(core::time::Duration::from_secs(10));
            check_idle_txn_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            check_idle_txn_interval.reset();
            loop {
                check_idle_txn_interval.tick().await;
                sessions.read().values().for_each(|session| {
                    let _ = session.check_idle_in_transaction_timeout();
                })
            }
        });
        join_handles.push(join_handle);

        // Clean up the spill directory.
        #[cfg(not(madsim))]
        if config.batch.enable_spill {
            SpillOp::clean_spill_directory()
                .await
                .map_err(|err| anyhow!(err))?;
        }

        let total_memory_bytes = opts.frontend_total_memory_bytes;
        let heap_profiler =
            HeapProfiler::new(total_memory_bytes, config.server.heap_profiling.clone());
        // Run a background heap profiler
        heap_profiler.start();

        let batch_memory_limit = total_memory_bytes as f64 * FRONTEND_BATCH_MEMORY_PROPORTION;
        let mem_context = MemoryContext::root(
            frontend_metrics.batch_total_mem.clone(),
            batch_memory_limit as u64,
        );

        info!(
            "Frontend  total_memory: {} batch_memory: {}",
            convert(total_memory_bytes as _),
            convert(batch_memory_limit as _),
        );

        Ok((
            Self {
                catalog_reader,
                catalog_writer,
                user_info_reader,
                user_info_writer,
                worker_node_manager,
                meta_client: frontend_meta_client,
                query_manager,
                hummock_snapshot_manager,
                system_params_manager,
                session_params,
                server_addr: frontend_address,
                client_pool: compute_client_pool,
                frontend_metrics,
                cursor_metrics,
                spill_metrics,
                sessions_map,
                batch_config: config.batch,
                meta_config: config.meta,
                streaming_config: config.streaming,
                source_metrics,
                creating_streaming_job_tracker,
                compute_runtime,
                mem_context,
            },
            join_handles,
            shutdown_senders,
        ))
    }

    /// Get a reference to the frontend env's catalog writer.
    ///
    /// This method is intentionally private, and a write guard is required for the caller to
    /// prove that the write operations are permitted in the current transaction.
    fn catalog_writer(&self, _guard: transaction::WriteGuard) -> &dyn CatalogWriter {
        &*self.catalog_writer
    }

    /// Get a reference to the frontend env's catalog reader.
    pub fn catalog_reader(&self) -> &CatalogReader {
        &self.catalog_reader
    }

    /// Get a reference to the frontend env's user info writer.
    ///
    /// This method is intentionally private, and a write guard is required for the caller to
    /// prove that the write operations are permitted in the current transaction.
    fn user_info_writer(&self, _guard: transaction::WriteGuard) -> &dyn UserInfoWriter {
        &*self.user_info_writer
    }

    /// Get a reference to the frontend env's user info reader.
    pub fn user_info_reader(&self) -> &UserInfoReader {
        &self.user_info_reader
    }

    pub fn worker_node_manager(&self) -> &WorkerNodeManager {
        &self.worker_node_manager
    }

    pub fn worker_node_manager_ref(&self) -> WorkerNodeManagerRef {
        self.worker_node_manager.clone()
    }

    pub fn meta_client(&self) -> &dyn FrontendMetaClient {
        &*self.meta_client
    }

    pub fn meta_client_ref(&self) -> Arc<dyn FrontendMetaClient> {
        self.meta_client.clone()
    }

    pub fn query_manager(&self) -> &QueryManager {
        &self.query_manager
    }

    pub fn hummock_snapshot_manager(&self) -> &HummockSnapshotManagerRef {
        &self.hummock_snapshot_manager
    }

    pub fn system_params_manager(&self) -> &LocalSystemParamsManagerRef {
        &self.system_params_manager
    }

    pub fn session_params_snapshot(&self) -> SessionConfig {
        self.session_params.read_recursive().clone()
    }

    pub fn server_address(&self) -> &HostAddr {
        &self.server_addr
    }

    pub fn client_pool(&self) -> ComputeClientPoolRef {
        self.client_pool.clone()
    }

    pub fn batch_config(&self) -> &BatchConfig {
        &self.batch_config
    }

    pub fn meta_config(&self) -> &MetaConfig {
        &self.meta_config
    }

    pub fn streaming_config(&self) -> &StreamingConfig {
        &self.streaming_config
    }

    pub fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.source_metrics.clone()
    }

    pub fn spill_metrics(&self) -> Arc<BatchSpillMetrics> {
        self.spill_metrics.clone()
    }

    pub fn creating_streaming_job_tracker(&self) -> &StreamingJobTrackerRef {
        &self.creating_streaming_job_tracker
    }

    pub fn sessions_map(&self) -> &SessionMapRef {
        &self.sessions_map
    }

    pub fn compute_runtime(&self) -> Arc<BackgroundShutdownRuntime> {
        self.compute_runtime.clone()
    }

    /// Cancel queries (i.e. batch queries) in session.
    /// If the session exists return true, otherwise, return false.
    pub fn cancel_queries_in_session(&self, session_id: SessionId) -> bool {
        let guard = self.sessions_map.read();
        if let Some(session) = guard.get(&session_id) {
            session.cancel_current_query();
            true
        } else {
            info!("Current session finished, ignoring cancel query request");
            false
        }
    }

    /// Cancel creating jobs (i.e. streaming queries) in session.
    /// If the session exists return true, otherwise, return false.
    pub fn cancel_creating_jobs_in_session(&self, session_id: SessionId) -> bool {
        let guard = self.sessions_map.read();
        if let Some(session) = guard.get(&session_id) {
            session.cancel_current_creating_job();
            true
        } else {
            info!("Current session finished, ignoring cancel creating request");
            false
        }
    }

    pub fn mem_context(&self) -> MemoryContext {
        self.mem_context.clone()
    }
}

pub struct AuthContext {
    pub database: String,
    pub user_name: String,
    pub user_id: UserId,
}

impl AuthContext {
    pub fn new(database: String, user_name: String, user_id: UserId) -> Self {
        Self {
            database,
            user_name,
            user_id,
        }
    }
}
pub struct SessionImpl {
    env: FrontendEnv,
    auth_context: Arc<AuthContext>,
    /// Used for user authentication.
    user_authenticator: UserAuthenticator,
    /// Stores the value of configurations.
    config_map: Arc<RwLock<SessionConfig>>,
    /// buffer the Notices to users,
    notices: RwLock<Vec<String>>,

    /// Identified by `process_id`, `secret_key`. Corresponds to `SessionManager`.
    id: (i32, i32),

    /// Client address
    peer_addr: AddressRef,

    /// Transaction state.
    /// TODO: get rid of the `Mutex` here as a workaround if the `Send` requirement of
    /// async functions, there should actually be no contention.
    txn: Arc<Mutex<transaction::State>>,

    /// Query cancel flag.
    /// This flag is set only when current query is executed in local mode, and used to cancel
    /// local query.
    current_query_cancel_flag: Mutex<Option<ShutdownSender>>,

    /// execution context represents the lifetime of a running SQL in the current session
    exec_context: Mutex<Option<Weak<ExecContext>>>,

    /// Last idle instant
    last_idle_instant: Arc<Mutex<Option<Instant>>>,

    cursor_manager: Arc<CursorManager>,

    /// temporary sources for the current session
    temporary_source_manager: Arc<Mutex<TemporarySourceManager>>,
}

/// If TEMPORARY or TEMP is specified, the source is created as a temporary source.
/// Temporary sources are automatically dropped at the end of a session
/// Temporary sources are expected to be selected by batch queries, not streaming queries.
/// Temporary sources currently are only used by cloud portal to preview the data during table and
/// source creation, so it is a internal feature and not exposed to users.
/// The current PR supports temporary source with minimum effort,
/// so we don't care about the database name and schema name, but only care about the source name.
/// Temporary sources can only be shown via `show sources` command but not other system tables.
#[derive(Default, Clone)]
pub struct TemporarySourceManager {
    sources: HashMap<String, SourceCatalog>,
}

impl TemporarySourceManager {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
        }
    }

    pub fn create_source(&mut self, name: String, source: SourceCatalog) {
        self.sources.insert(name, source);
    }

    pub fn drop_source(&mut self, name: &str) {
        self.sources.remove(name);
    }

    pub fn get_source(&self, name: &str) -> Option<&SourceCatalog> {
        self.sources.get(name)
    }

    pub fn keys(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }
}

#[derive(Error, Debug)]
pub enum CheckRelationError {
    #[error("{0}")]
    Resolve(#[from] ResolveQualifiedNameError),
    #[error("{0}")]
    Catalog(#[from] CatalogError),
}

impl From<CheckRelationError> for RwError {
    fn from(e: CheckRelationError) -> Self {
        match e {
            CheckRelationError::Resolve(e) => e.into(),
            CheckRelationError::Catalog(e) => e.into(),
        }
    }
}

impl SessionImpl {
    pub fn new(
        env: FrontendEnv,
        auth_context: Arc<AuthContext>,
        user_authenticator: UserAuthenticator,
        id: SessionId,
        peer_addr: AddressRef,
        session_config: SessionConfig,
    ) -> Self {
        let cursor_metrics = env.cursor_metrics.clone();
        Self {
            env,
            auth_context,
            user_authenticator,
            config_map: Arc::new(RwLock::new(session_config)),
            id,
            peer_addr,
            txn: Default::default(),
            current_query_cancel_flag: Mutex::new(None),
            notices: Default::default(),
            exec_context: Mutex::new(None),
            last_idle_instant: Default::default(),
            cursor_manager: Arc::new(CursorManager::new(cursor_metrics)),
            temporary_source_manager: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        let env = FrontendEnv::mock();
        Self {
            env: FrontendEnv::mock(),
            auth_context: Arc::new(AuthContext::new(
                DEFAULT_DATABASE_NAME.to_owned(),
                DEFAULT_SUPER_USER.to_owned(),
                DEFAULT_SUPER_USER_ID,
            )),
            user_authenticator: UserAuthenticator::None,
            config_map: Default::default(),
            // Mock session use non-sense id.
            id: (0, 0),
            txn: Default::default(),
            current_query_cancel_flag: Mutex::new(None),
            notices: Default::default(),
            exec_context: Mutex::new(None),
            peer_addr: Address::Tcp(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                8080,
            ))
            .into(),
            last_idle_instant: Default::default(),
            cursor_manager: Arc::new(CursorManager::new(env.cursor_metrics.clone())),
            temporary_source_manager: Default::default(),
        }
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    pub fn auth_context(&self) -> Arc<AuthContext> {
        self.auth_context.clone()
    }

    pub fn database(&self) -> &str {
        &self.auth_context.database
    }

    pub fn user_name(&self) -> &str {
        &self.auth_context.user_name
    }

    pub fn user_id(&self) -> UserId {
        self.auth_context.user_id
    }

    pub fn shared_config(&self) -> Arc<RwLock<SessionConfig>> {
        Arc::clone(&self.config_map)
    }

    pub fn config(&self) -> RwLockReadGuard<'_, SessionConfig> {
        self.config_map.read()
    }

    pub fn set_config(&self, key: &str, value: String) -> Result<String> {
        self.config_map
            .write()
            .set(key, value, &mut ())
            .map_err(Into::into)
    }

    pub fn set_config_report(
        &self,
        key: &str,
        value: Option<String>,
        mut reporter: impl ConfigReporter,
    ) -> Result<String> {
        if let Some(value) = value {
            self.config_map
                .write()
                .set(key, value, &mut reporter)
                .map_err(Into::into)
        } else {
            self.config_map
                .write()
                .reset(key, &mut reporter)
                .map_err(Into::into)
        }
    }

    pub fn session_id(&self) -> SessionId {
        self.id
    }

    pub fn running_sql(&self) -> Option<Arc<str>> {
        self.exec_context
            .lock()
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|context| context.running_sql.clone())
    }

    pub fn get_cursor_manager(&self) -> Arc<CursorManager> {
        self.cursor_manager.clone()
    }

    pub fn peer_addr(&self) -> &Address {
        &self.peer_addr
    }

    pub fn elapse_since_running_sql(&self) -> Option<u128> {
        self.exec_context
            .lock()
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|context| context.last_instant.elapsed().as_millis())
    }

    pub fn elapse_since_last_idle_instant(&self) -> Option<u128> {
        self.last_idle_instant
            .lock()
            .as_ref()
            .map(|x| x.elapsed().as_millis())
    }

    pub fn check_relation_name_duplicated(
        &self,
        name: ObjectName,
        stmt_type: StatementType,
        if_not_exists: bool,
    ) -> std::result::Result<Either<(), RwPgResponse>, CheckRelationError> {
        let db_name = self.database();
        let catalog_reader = self.env().catalog_reader().read_guard();
        let (schema_name, relation_name) = {
            let (schema_name, relation_name) =
                Binder::resolve_schema_qualified_name(db_name, name)?;
            let search_path = self.config().search_path();
            let user_name = &self.auth_context().user_name;
            let schema_name = match schema_name {
                Some(schema_name) => schema_name,
                None => catalog_reader
                    .first_valid_schema(db_name, &search_path, user_name)?
                    .name(),
            };
            (schema_name, relation_name)
        };
        match catalog_reader.check_relation_name_duplicated(db_name, &schema_name, &relation_name) {
            Err(CatalogError::Duplicated(_, name)) if if_not_exists => Ok(Either::Right(
                PgResponse::builder(stmt_type)
                    .notice(format!("relation \"{}\" already exists, skipping", name))
                    .into(),
            )),
            Err(e) => Err(e.into()),
            Ok(_) => Ok(Either::Left(())),
        }
    }

    pub fn check_secret_name_duplicated(&self, name: ObjectName) -> Result<()> {
        let db_name = self.database();
        let catalog_reader = self.env().catalog_reader().read_guard();
        let (schema_name, secret_name) = {
            let (schema_name, secret_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
            let search_path = self.config().search_path();
            let user_name = &self.auth_context().user_name;
            let schema_name = match schema_name {
                Some(schema_name) => schema_name,
                None => catalog_reader
                    .first_valid_schema(db_name, &search_path, user_name)?
                    .name(),
            };
            (schema_name, secret_name)
        };
        catalog_reader
            .check_secret_name_duplicated(db_name, &schema_name, &secret_name)
            .map_err(RwError::from)
    }

    pub fn check_connection_name_duplicated(&self, name: ObjectName) -> Result<()> {
        let db_name = self.database();
        let catalog_reader = self.env().catalog_reader().read_guard();
        let (schema_name, connection_name) = {
            let (schema_name, connection_name) =
                Binder::resolve_schema_qualified_name(db_name, name)?;
            let search_path = self.config().search_path();
            let user_name = &self.auth_context().user_name;
            let schema_name = match schema_name {
                Some(schema_name) => schema_name,
                None => catalog_reader
                    .first_valid_schema(db_name, &search_path, user_name)?
                    .name(),
            };
            (schema_name, connection_name)
        };
        catalog_reader
            .check_connection_name_duplicated(db_name, &schema_name, &connection_name)
            .map_err(RwError::from)
    }

    /// Also check if the user has the privilege to create in the schema.
    pub fn get_database_and_schema_id_for_create(
        &self,
        schema_name: Option<String>,
    ) -> Result<(DatabaseId, SchemaId)> {
        let db_name = self.database();

        let search_path = self.config().search_path();
        let user_name = &self.auth_context().user_name;

        let catalog_reader = self.env().catalog_reader().read_guard();
        let schema = match schema_name {
            Some(schema_name) => catalog_reader.get_schema_by_name(db_name, &schema_name)?,
            None => catalog_reader.first_valid_schema(db_name, &search_path, user_name)?,
        };

        check_schema_writable(&schema.name())?;
        if schema.name() != DEFAULT_SCHEMA_NAME {
            self.check_privileges(&[ObjectCheckItem::new(
                schema.owner(),
                AclMode::Create,
                Object::SchemaId(schema.id()),
            )])?;
        }

        let db_id = catalog_reader.get_database_by_name(db_name)?.id();
        Ok((db_id, schema.id()))
    }

    pub fn get_connection_by_name(
        &self,
        schema_name: Option<String>,
        connection_name: &str,
    ) -> Result<Arc<ConnectionCatalog>> {
        let db_name = self.database();
        let search_path = self.config().search_path();
        let user_name = &self.auth_context().user_name;

        let catalog_reader = self.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
        let (connection, _) =
            catalog_reader.get_connection_by_name(db_name, schema_path, connection_name)?;
        Ok(connection.clone())
    }

    pub fn get_subscription_by_schema_id_name(
        &self,
        schema_id: SchemaId,
        subscription_name: &str,
    ) -> Result<Arc<SubscriptionCatalog>> {
        let db_name = self.database();

        let catalog_reader = self.env().catalog_reader().read_guard();
        let db_id = catalog_reader.get_database_by_name(db_name)?.id();
        let schema = catalog_reader.get_schema_by_id(&db_id, &schema_id)?;
        let subscription = schema
            .get_subscription_by_name(subscription_name)
            .ok_or_else(|| {
                RwError::from(ErrorCode::ItemNotFound(format!(
                    "subscription {} not found",
                    subscription_name
                )))
            })?;
        Ok(subscription.clone())
    }

    pub fn get_subscription_by_name(
        &self,
        schema_name: Option<String>,
        subscription_name: &str,
    ) -> Result<Arc<SubscriptionCatalog>> {
        let db_name = self.database();
        let search_path = self.config().search_path();
        let user_name = &self.auth_context().user_name;

        let catalog_reader = self.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
        let (subscription, _) =
            catalog_reader.get_subscription_by_name(db_name, schema_path, subscription_name)?;
        Ok(subscription.clone())
    }

    pub fn get_table_by_id(&self, table_id: &TableId) -> Result<Arc<TableCatalog>> {
        let catalog_reader = self.env().catalog_reader().read_guard();
        Ok(catalog_reader.get_any_table_by_id(table_id)?.clone())
    }

    pub fn get_table_by_name(
        &self,
        table_name: &str,
        db_id: u32,
        schema_id: u32,
    ) -> Result<Arc<TableCatalog>> {
        let catalog_reader = self.env().catalog_reader().read_guard();
        let table = catalog_reader
            .get_schema_by_id(&DatabaseId::from(db_id), &SchemaId::from(schema_id))?
            .get_created_table_by_name(table_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("table \"{}\" does not exist", table_name),
                )
            })?;
        Ok(table.clone())
    }

    pub fn get_secret_by_name(
        &self,
        schema_name: Option<String>,
        secret_name: &str,
    ) -> Result<Arc<SecretCatalog>> {
        let db_name = self.database();
        let search_path = self.config().search_path();
        let user_name = &self.auth_context().user_name;

        let catalog_reader = self.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
        let (secret, _) = catalog_reader.get_secret_by_name(db_name, schema_path, secret_name)?;
        Ok(secret.clone())
    }

    pub fn list_change_log_epochs(
        &self,
        table_id: u32,
        min_epoch: u64,
        max_count: u32,
    ) -> Result<Vec<u64>> {
        Ok(self
            .env
            .hummock_snapshot_manager()
            .acquire()
            .list_change_log_epochs(table_id, min_epoch, max_count))
    }

    pub fn clear_cancel_query_flag(&self) {
        let mut flag = self.current_query_cancel_flag.lock();
        *flag = None;
    }

    pub fn reset_cancel_query_flag(&self) -> ShutdownToken {
        let mut flag = self.current_query_cancel_flag.lock();
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        *flag = Some(shutdown_tx);
        shutdown_rx
    }

    fn clear_notices(&self) {
        *self.notices.write() = vec![];
    }

    pub fn cancel_current_query(&self) {
        let mut flag_guard = self.current_query_cancel_flag.lock();
        if let Some(sender) = flag_guard.take() {
            info!("Trying to cancel query in local mode.");
            // Current running query is in local mode
            sender.cancel();
            info!("Cancel query request sent.");
        } else {
            info!("Trying to cancel query in distributed mode.");
            self.env.query_manager().cancel_queries_in_session(self.id)
        }
        self.clear_notices()
    }

    pub fn cancel_current_creating_job(&self) {
        self.env.creating_streaming_job_tracker.abort_jobs(self.id);
        self.clear_notices()
    }

    /// This function only used for test now.
    /// Maybe we can remove it in the future.
    pub async fn run_statement(
        self: Arc<Self>,
        sql: Arc<str>,
        formats: Vec<Format>,
    ) -> std::result::Result<PgResponse<PgResponseStream>, BoxedError> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(&sql)?;
        if stmts.is_empty() {
            return Ok(PgResponse::empty_result(
                pgwire::pg_response::StatementType::EMPTY,
            ));
        }
        if stmts.len() > 1 {
            return Ok(
                PgResponse::builder(pgwire::pg_response::StatementType::EMPTY)
                    .notice("cannot insert multiple commands into statement")
                    .into(),
            );
        }
        let stmt = stmts.swap_remove(0);
        let rsp = handle(self, stmt, sql.clone(), formats).await?;
        Ok(rsp)
    }

    pub fn notice_to_user(&self, str: impl Into<String>) {
        let notice = str.into();
        tracing::trace!(notice, "notice to user");
        self.notices.write().push(notice);
    }

    pub fn is_barrier_read(&self) -> bool {
        match self.config().visibility_mode() {
            VisibilityMode::Default => self.env.batch_config.enable_barrier_read,
            VisibilityMode::All => true,
            VisibilityMode::Checkpoint => false,
        }
    }

    pub fn statement_timeout(&self) -> Duration {
        if self.config().statement_timeout() == 0 {
            Duration::from_secs(self.env.batch_config.statement_timeout_in_sec as u64)
        } else {
            Duration::from_secs(self.config().statement_timeout() as u64)
        }
    }

    pub fn create_temporary_source(&self, source: SourceCatalog) {
        self.temporary_source_manager
            .lock()
            .create_source(source.name.clone(), source);
    }

    pub fn get_temporary_source(&self, name: &str) -> Option<SourceCatalog> {
        self.temporary_source_manager
            .lock()
            .get_source(name)
            .cloned()
    }

    pub fn drop_temporary_source(&self, name: &str) {
        self.temporary_source_manager.lock().drop_source(name);
    }

    pub fn temporary_source_manager(&self) -> TemporarySourceManager {
        self.temporary_source_manager.lock().clone()
    }

    pub async fn check_cluster_limits(&self) -> Result<()> {
        if self.config().bypass_cluster_limits() {
            return Ok(());
        }

        let gen_message = |violated_limit: &ActorCountPerParallelism,
                           exceed_hard_limit: bool|
         -> String {
            let (limit_type, action) = if exceed_hard_limit {
                ("critical", "Please scale the cluster before proceeding!")
            } else {
                ("recommended", "Scaling the cluster is recommended.")
            };
            format!(
                "\n- {}\n- {}\n- {}\n- {}\n- {}\n{}",
                format_args!("Actor count per parallelism exceeds the {} limit.", limit_type),
                format_args!("Depending on your workload, this may overload the cluster and cause performance/stability issues. {}", action),
                "Contact us via slack or https://risingwave.com/contact-us/ for further enquiry.",
                "You can bypass this check via SQL `SET bypass_cluster_limits TO true`.",
                "You can check actor count distribution via SQL `SELECT * FROM rw_worker_actor_count`.",
                violated_limit,
            )
        };

        let limits = self.env().meta_client().get_cluster_limits().await?;
        for limit in limits {
            match limit {
                cluster_limit::ClusterLimit::ActorCount(l) => {
                    if l.exceed_hard_limit() {
                        return Err(RwError::from(ErrorCode::ProtocolError(gen_message(
                            &l, true,
                        ))));
                    } else if l.exceed_soft_limit() {
                        self.notice_to_user(gen_message(&l, false));
                    }
                }
            }
        }
        Ok(())
    }
}

pub static SESSION_MANAGER: std::sync::OnceLock<Arc<SessionManagerImpl>> =
    std::sync::OnceLock::new();

pub struct SessionManagerImpl {
    env: FrontendEnv,
    _join_handles: Vec<JoinHandle<()>>,
    _shutdown_senders: Vec<Sender<()>>,
    number: AtomicI32,
}

impl SessionManager for SessionManagerImpl {
    type Session = SessionImpl;

    fn create_dummy_session(
        &self,
        database_id: u32,
        user_id: u32,
    ) -> std::result::Result<Arc<Self::Session>, BoxedError> {
        let dummy_addr = Address::Tcp(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            5691, // port of meta
        ));
        let user_reader = self.env.user_info_reader();
        let reader = user_reader.read_guard();
        if let Some(user_name) = reader.get_user_name_by_id(user_id) {
            self.connect_inner(database_id, user_name.as_str(), Arc::new(dummy_addr))
        } else {
            Err(Box::new(Error::new(
                ErrorKind::InvalidInput,
                format!("Role id {} does not exist", user_id),
            )))
        }
    }

    fn connect(
        &self,
        database: &str,
        user_name: &str,
        peer_addr: AddressRef,
    ) -> std::result::Result<Arc<Self::Session>, BoxedError> {
        let catalog_reader = self.env.catalog_reader();
        let reader = catalog_reader.read_guard();
        let database_id = reader
            .get_database_by_name(database)
            .map_err(|_| {
                Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    format!("database \"{}\" does not exist", database),
                ))
            })?
            .id();

        self.connect_inner(database_id, user_name, peer_addr)
    }

    /// Used when cancel request happened.
    fn cancel_queries_in_session(&self, session_id: SessionId) {
        self.env.cancel_queries_in_session(session_id);
    }

    fn cancel_creating_jobs_in_session(&self, session_id: SessionId) {
        self.env.cancel_creating_jobs_in_session(session_id);
    }

    fn end_session(&self, session: &Self::Session) {
        self.delete_session(&session.session_id());
    }

    async fn shutdown(&self) {
        // Clean up the session map.
        self.env.sessions_map().write().clear();
        // Unregister from the meta service.
        self.env.meta_client().try_unregister().await;
    }
}

impl SessionManagerImpl {
    pub async fn new(opts: FrontendOpts) -> Result<Self> {
        // TODO(shutdown): only save join handles that **need** to be shutdown
        let (env, join_handles, shutdown_senders) = FrontendEnv::init(opts).await?;
        Ok(Self {
            env,
            _join_handles: join_handles,
            _shutdown_senders: shutdown_senders,
            number: AtomicI32::new(0),
        })
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    fn insert_session(&self, session: Arc<SessionImpl>) {
        let active_sessions = {
            let mut write_guard = self.env.sessions_map.write();
            write_guard.insert(session.id(), session);
            write_guard.len()
        };
        self.env
            .frontend_metrics
            .active_sessions
            .set(active_sessions as i64);
    }

    fn delete_session(&self, session_id: &SessionId) {
        let active_sessions = {
            let mut write_guard = self.env.sessions_map.write();
            write_guard.remove(session_id);
            write_guard.len()
        };
        self.env
            .frontend_metrics
            .active_sessions
            .set(active_sessions as i64);
    }

    fn connect_inner(
        &self,
        database_id: u32,
        user_name: &str,
        peer_addr: AddressRef,
    ) -> std::result::Result<Arc<SessionImpl>, BoxedError> {
        let catalog_reader = self.env.catalog_reader();
        let reader = catalog_reader.read_guard();
        let database_name = reader
            .get_database_by_id(&database_id)
            .map_err(|_| {
                Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    format!("database \"{}\" does not exist", database_id),
                ))
            })?
            .name();

        let user_reader = self.env.user_info_reader();
        let reader = user_reader.read_guard();
        if let Some(user) = reader.get_user_by_name(user_name) {
            if !user.can_login {
                return Err(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    format!("User {} is not allowed to login", user_name),
                )));
            }
            let has_privilege =
                user.check_privilege(&Object::DatabaseId(database_id), AclMode::Connect);
            if !user.is_super && !has_privilege {
                return Err(Box::new(Error::new(
                    ErrorKind::PermissionDenied,
                    "User does not have CONNECT privilege.",
                )));
            }
            let user_authenticator = match &user.auth_info {
                None => UserAuthenticator::None,
                Some(auth_info) => {
                    if auth_info.encryption_type == EncryptionType::Plaintext as i32 {
                        UserAuthenticator::ClearText(auth_info.encrypted_value.clone())
                    } else if auth_info.encryption_type == EncryptionType::Md5 as i32 {
                        let mut salt = [0; 4];
                        let mut rng = rand::thread_rng();
                        rng.fill_bytes(&mut salt);
                        UserAuthenticator::Md5WithSalt {
                            encrypted_password: md5_hash_with_salt(
                                &auth_info.encrypted_value,
                                &salt,
                            ),
                            salt,
                        }
                    } else if auth_info.encryption_type == EncryptionType::Oauth as i32 {
                        UserAuthenticator::OAuth(auth_info.metadata.clone())
                    } else {
                        return Err(Box::new(Error::new(
                            ErrorKind::Unsupported,
                            format!("Unsupported auth type: {}", auth_info.encryption_type),
                        )));
                    }
                }
            };

            // Assign a session id and insert into sessions map (for cancel request).
            let secret_key = self.number.fetch_add(1, Ordering::Relaxed);
            // Use a trivial strategy: process_id and secret_key are equal.
            let id = (secret_key, secret_key);
            // Read session params snapshot from frontend env.
            let session_config = self.env.session_params_snapshot();

            let session_impl: Arc<SessionImpl> = SessionImpl::new(
                self.env.clone(),
                Arc::new(AuthContext::new(
                    database_name.to_owned(),
                    user_name.to_owned(),
                    user.id,
                )),
                user_authenticator,
                id,
                peer_addr,
                session_config,
            )
            .into();
            self.insert_session(session_impl.clone());

            Ok(session_impl)
        } else {
            Err(Box::new(Error::new(
                ErrorKind::InvalidInput,
                format!("Role {} does not exist", user_name),
            )))
        }
    }
}

impl Session for SessionImpl {
    type Portal = Portal;
    type PreparedStatement = PrepareStatement;
    type ValuesStream = PgResponseStream;

    /// A copy of `run_statement` but exclude the parser part so each run must be at most one
    /// statement. The str sql use the `to_string` of AST. Consider Reuse later.
    async fn run_one_query(
        self: Arc<Self>,
        stmt: Statement,
        format: Format,
    ) -> std::result::Result<PgResponse<PgResponseStream>, BoxedError> {
        let string = stmt.to_string();
        let sql_str = string.as_str();
        let sql: Arc<str> = Arc::from(sql_str);
        let rsp = handle(self, stmt, sql.clone(), vec![format]).await?;
        Ok(rsp)
    }

    fn user_authenticator(&self) -> &UserAuthenticator {
        &self.user_authenticator
    }

    fn id(&self) -> SessionId {
        self.id
    }

    async fn parse(
        self: Arc<Self>,
        statement: Option<Statement>,
        params_types: Vec<Option<DataType>>,
    ) -> std::result::Result<PrepareStatement, BoxedError> {
        Ok(if let Some(statement) = statement {
            handle_parse(self, statement, params_types).await?
        } else {
            PrepareStatement::Empty
        })
    }

    fn bind(
        self: Arc<Self>,
        prepare_statement: PrepareStatement,
        params: Vec<Option<Bytes>>,
        param_formats: Vec<Format>,
        result_formats: Vec<Format>,
    ) -> std::result::Result<Portal, BoxedError> {
        Ok(handle_bind(
            prepare_statement,
            params,
            param_formats,
            result_formats,
        )?)
    }

    async fn execute(
        self: Arc<Self>,
        portal: Portal,
    ) -> std::result::Result<PgResponse<PgResponseStream>, BoxedError> {
        let rsp = handle_execute(self, portal).await?;
        Ok(rsp)
    }

    fn describe_statement(
        self: Arc<Self>,
        prepare_statement: PrepareStatement,
    ) -> std::result::Result<(Vec<DataType>, Vec<PgFieldDescriptor>), BoxedError> {
        Ok(match prepare_statement {
            PrepareStatement::Empty => (vec![], vec![]),
            PrepareStatement::Prepared(prepare_statement) => (
                prepare_statement.bound_result.param_types,
                infer(
                    Some(prepare_statement.bound_result.bound),
                    prepare_statement.statement,
                )?,
            ),
            PrepareStatement::PureStatement(statement) => (vec![], infer(None, statement)?),
        })
    }

    fn describe_portal(
        self: Arc<Self>,
        portal: Portal,
    ) -> std::result::Result<Vec<PgFieldDescriptor>, BoxedError> {
        match portal {
            Portal::Empty => Ok(vec![]),
            Portal::Portal(portal) => {
                let mut columns = infer(Some(portal.bound_result.bound), portal.statement)?;
                let formats = FormatIterator::new(&portal.result_formats, columns.len())?;
                columns.iter_mut().zip_eq_fast(formats).for_each(|(c, f)| {
                    if f == Format::Binary {
                        c.set_to_binary()
                    }
                });
                Ok(columns)
            }
            Portal::PureStatement(statement) => Ok(infer(None, statement)?),
        }
    }

    fn set_config(&self, key: &str, value: String) -> std::result::Result<String, BoxedError> {
        Self::set_config(self, key, value).map_err(Into::into)
    }

    fn take_notices(self: Arc<Self>) -> Vec<String> {
        let inner = &mut (*self.notices.write());
        std::mem::take(inner)
    }

    fn transaction_status(&self) -> TransactionStatus {
        match &*self.txn.lock() {
            transaction::State::Initial | transaction::State::Implicit(_) => {
                TransactionStatus::Idle
            }
            transaction::State::Explicit(_) => TransactionStatus::InTransaction,
            // TODO: failed transaction
        }
    }

    /// Init and return an `ExecContextGuard` which could be used as a guard to represent the execution flow.
    fn init_exec_context(&self, sql: Arc<str>) -> ExecContextGuard {
        let exec_context = Arc::new(ExecContext {
            running_sql: sql,
            last_instant: Instant::now(),
            last_idle_instant: self.last_idle_instant.clone(),
        });
        *self.exec_context.lock() = Some(Arc::downgrade(&exec_context));
        // unset idle state, since there is a sql running
        *self.last_idle_instant.lock() = None;
        ExecContextGuard::new(exec_context)
    }

    /// Check whether idle transaction timeout.
    /// If yes, unpin snapshot and return an `IdleInTxnTimeout` error.
    fn check_idle_in_transaction_timeout(&self) -> PsqlResult<()> {
        // In transaction.
        if matches!(self.transaction_status(), TransactionStatus::InTransaction) {
            let idle_in_transaction_session_timeout =
                self.config().idle_in_transaction_session_timeout() as u128;
            // Idle transaction timeout has been enabled.
            if idle_in_transaction_session_timeout != 0 {
                // Hold the `exec_context` lock to ensure no new sql coming when unpin_snapshot.
                let guard = self.exec_context.lock();
                // No running sql i.e. idle
                if guard.as_ref().and_then(|weak| weak.upgrade()).is_none() {
                    // Idle timeout.
                    if let Some(elapse_since_last_idle_instant) =
                        self.elapse_since_last_idle_instant()
                    {
                        if elapse_since_last_idle_instant > idle_in_transaction_session_timeout {
                            return Err(PsqlError::IdleInTxnTimeout);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Returns row description of the statement
fn infer(bound: Option<BoundStatement>, stmt: Statement) -> Result<Vec<PgFieldDescriptor>> {
    match stmt {
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Delete { .. }
        | Statement::Update { .. }
        | Statement::FetchCursor { .. } => Ok(bound
            .unwrap()
            .output_fields()
            .iter()
            .map(to_pg_field)
            .collect()),
        Statement::ShowObjects {
            object: show_object,
            ..
        } => Ok(infer_show_object(&show_object)),
        Statement::ShowCreateObject { .. } => Ok(infer_show_create_object()),
        Statement::ShowTransactionIsolationLevel => {
            let name = "transaction_isolation";
            Ok(infer_show_variable(name))
        }
        Statement::ShowVariable { variable } => {
            let name = &variable[0].real_value().to_lowercase();
            Ok(infer_show_variable(name))
        }
        Statement::Describe { name: _ } => Ok(infer_describe()),
        Statement::Explain { .. } => Ok(vec![PgFieldDescriptor::new(
            "QUERY PLAN".to_owned(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        )]),
        _ => Ok(vec![]),
    }
}
