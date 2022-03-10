use std::cell::RefCell;
use std::fmt::Formatter;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{Session, SessionManager};
use risingwave_common::error::Result;
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use risingwave_sqlparser::parser::Parser;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::catalog::catalog_service::{
    CatalogCache, CatalogConnector, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::handler::handle;
use crate::observer::observer_manager::ObserverManager;
use crate::optimizer::plan_node::PlanNodeId;
use crate::scheduler::schedule::WorkerNodeManager;
use crate::test_utils::FrontendMockMetaClient;
use crate::FrontendOpts;

pub struct QueryContext {
    pub session_ctx: Arc<SessionContext>,
    pub next_id: i32,
}
/// The reference of `QueryContext`, our system assumes that frontend will not parallel for a query,
/// so we use `RefCell` here.
pub type QueryContextRef = Rc<RefCell<QueryContext>>;

impl QueryContext {
    pub fn new(session_ctx: Arc<SessionContext>) -> Self {
        Self {
            session_ctx,
            next_id: 0,
        }
    }

    pub fn get_id(&mut self) -> PlanNodeId {
        let ret = PlanNodeId(self.next_id);
        self.next_id += 1;
        ret
    }

    pub async fn mock() -> Self {
        Self {
            session_ctx: Arc::new(SessionContext::mock().await),
            next_id: 0,
        }
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueryContext {{ current id = {} }}", self.next_id)
    }
}

/// The global environment for the frontend server.
#[derive(Clone)]
pub struct FrontendEnv {
    meta_client: MetaClient,
    // Different session may access catalog manager at the same time.
    catalog_manager: CatalogConnector,
}

impl FrontendEnv {
    pub async fn init(opts: &FrontendOpts) -> Result<(Self, JoinHandle<()>)> {
        let meta_client = MetaClient::new(opts.meta_addr.clone().as_str()).await?;
        Self::with_meta_client(meta_client, opts).await
    }

    pub async fn mock() -> Self {
        let meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);
        let (_catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let catalog_cache = Arc::new(RwLock::new(
            CatalogCache::new(meta_client.clone()).await.unwrap(),
        ));
        let catalog_manager =
            CatalogConnector::new(meta_client.clone(), catalog_cache, catalog_updated_rx);
        Self {
            meta_client,
            catalog_manager,
        }
    }

    pub async fn with_meta_client(
        mut meta_client: MetaClient,
        opts: &FrontendOpts,
    ) -> Result<(Self, JoinHandle<()>)> {
        let host = opts.host.parse().unwrap();

        // Register in meta by calling `AddWorkerNode` RPC.
        meta_client.register(host, WorkerType::Frontend).await?;

        let (catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let catalog_cache = Arc::new(RwLock::new(CatalogCache::new(meta_client.clone()).await?));
        let catalog_manager = CatalogConnector::new(
            meta_client.clone(),
            catalog_cache.clone(),
            catalog_updated_rx,
        );

        let worker_node_manager = Arc::new(WorkerNodeManager::new(meta_client.clone()).await?);

        let observer_manager = ObserverManager::new(
            meta_client.clone(),
            host,
            worker_node_manager,
            catalog_cache,
            catalog_updated_tx,
        )
        .await;
        let observer_join_handle = observer_manager.start();

        meta_client.activate(host).await?;

        // Create default database when env init.
        let db_name = DEFAULT_DATABASE_NAME;
        let schema_name = DEFAULT_SCHEMA_NAME;
        if catalog_manager.get_database(db_name).is_none() {
            catalog_manager.create_database(db_name).await?;
        }
        if catalog_manager.get_schema(db_name, schema_name).is_none() {
            catalog_manager.create_schema(db_name, schema_name).await?;
        }

        Ok((
            Self {
                meta_client,
                catalog_manager,
            },
            observer_join_handle,
        ))
    }

    pub fn meta_client(&self) -> &MetaClient {
        &self.meta_client
    }

    pub fn catalog_mgr(&self) -> &CatalogConnector {
        &self.catalog_manager
    }
}

pub struct SessionImpl {
    pub ctx: Arc<SessionContext>,
}

pub struct SessionContext {
    env: FrontendEnv,
    database: String,
}

impl SessionContext {
    pub fn new(env: FrontendEnv, database: String) -> Self {
        Self { env, database }
    }
    pub async fn mock() -> Self {
        Self {
            env: FrontendEnv::mock().await,
            database: "dev".to_string(),
        }
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    pub fn database(&self) -> &str {
        &self.database
    }
}

impl SessionImpl {
    pub fn new(env: FrontendEnv, database: String) -> Self {
        // Self { env, database }
        let context = SessionContext { env, database };
        Self {
            ctx: Arc::new(context),
        }
    }
}

pub struct SessionManagerImpl {
    env: FrontendEnv,
    observer_join_handle: JoinHandle<()>,
}

impl SessionManager for SessionManagerImpl {
    fn connect(&self) -> Box<dyn Session> {
        Box::new(SessionImpl {
            ctx: Arc::new(SessionContext::new(self.env.clone(), "dev".to_string()))
            // env: self.env.clone(),
            // database: "dev".to_string(),
        })
    }
}

impl SessionManagerImpl {
    pub async fn new(opts: &FrontendOpts) -> Result<Self> {
        let (env, join_handle) = FrontendEnv::init(opts).await?;
        Ok(Self {
            env,
            observer_join_handle: join_handle,
        })
    }

    /// Used in unit test. Called before `LocalMeta::stop`.
    pub fn terminate(&self) {
        self.observer_join_handle.abort();
    }
}

#[async_trait::async_trait]
impl Session for SessionImpl {
    async fn run_statement(
        &self,
        sql: &str,
    ) -> std::result::Result<PgResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql)?;
        // With pgwire, there would be at most 1 statement in the vec.
        assert_eq!(stmts.len(), 1);
        let stmt = stmts.swap_remove(0);
        let rsp = handle(self, stmt).await?;
        Ok(rsp)
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_run_statement() {
        use std::ffi::OsString;

        use clap::StructOpt;
        use risingwave_meta::test_utils::LocalMeta;

        use super::*;

        let meta = LocalMeta::start(12008).await;
        let args: [OsString; 0] = []; // No argument.
        let mut opts = FrontendOpts::parse_from(args);
        opts.meta_addr = format!("http://{}", meta.meta_addr());
        let mgr = SessionManagerImpl::new(&opts).await.unwrap();
        // Check default database is created.
        assert!(mgr
            .env
            .catalog_manager
            .get_database(DEFAULT_DATABASE_NAME)
            .is_some());
        let session = mgr.connect();
        assert!(session.run_statement("select * from t").await.is_err());

        mgr.terminate();
        meta.stop().await;
    }
}
