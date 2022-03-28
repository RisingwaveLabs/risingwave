// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;
use std::io;
use std::result::Result;
use std::sync::Arc;

use log::{error, info};
use tokio::net::{TcpListener, TcpStream};

use crate::pg_protocol::PgProtocol;
use crate::pg_response::PgResponse;

/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager: Send + Sync {
<<<<<<< HEAD
    fn connect(&self, database: &str) -> Result<Arc<dyn Session>, Box<dyn Error + Send + Sync>>;
=======
    fn connect(&self) -> Arc<dyn Session>;
>>>>>>> aa996054 (refactor(session): run statement use Arc and remove SessionContext (#913))
}

/// A psql connection. Each connection binds with a database. Switching database will need to
/// recreate another connection.
#[async_trait::async_trait]
pub trait Session: Send + Sync {
    async fn run_statement(
        self: Arc<Self>,
        sql: &str,
    ) -> Result<PgResponse, Box<dyn Error + Send + Sync>>;
}

/// Binds a Tcp listener at [`addr`]. Spawn a coroutine to serve every new connection.
pub async fn pg_serve(addr: &str, session_mgr: Arc<dyn SessionManager>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await.unwrap();
    // accept connections and process them, spawning a new thread for each one
    info!("Starting server at {}", addr);
    loop {
        let session_mgr = session_mgr.clone();
        let conn_ret = listener.accept().await;
        match conn_ret {
            Ok((stream, peer_addr)) => {
                info!("New connection: {}", peer_addr);
                tokio::spawn(async move {
                    // connection succeeded
                    pg_serve_conn(stream, session_mgr).await;
                });
            }

            Err(e) => {
                error!("Connection failure: {}", e);
            }
        }
    }
}

async fn pg_serve_conn(socket: TcpStream, session_mgr: Arc<dyn SessionManager>) {
    let mut pg_proto = PgProtocol::new(socket, session_mgr);
    loop {
        let terminate = pg_proto.process().await;
        match terminate {
            Ok(is_ter) => {
                if is_ter {
                    println!("Connection closed by terminate cmd!");
                    break;
                }
            }
            Err(_) => {
                println!("Connection closed by error!");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use tokio_postgres::{NoTls, SimpleQueryMessage};

    use super::{Session, SessionManager};
    use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
    use crate::pg_response::{PgResponse, StatementType};
    use crate::pg_server::pg_serve;
    use crate::types::Row;

    struct TestSessionManager {}

    impl SessionManager for TestSessionManager {
<<<<<<< HEAD
        fn connect(
            &self,
            _database: &str,
        ) -> Result<Arc<dyn super::Session>, Box<dyn Error + Send + Sync>> {
            Ok(Arc::new(TestSession {}))
=======
        fn connect(&self) -> Arc<dyn super::Session> {
            Arc::new(TestSession {})
>>>>>>> aa996054 (refactor(session): run statement use Arc and remove SessionContext (#913))
        }
    }

    struct TestSession {}

    #[async_trait::async_trait]
    impl Session for TestSession {
        async fn run_statement(
            self: Arc<TestSession>,
            sql: &str,
        ) -> Result<PgResponse, Box<dyn Error + Send + Sync>> {
            // simulate an error
            if sql.starts_with("SELECTA") {
                return Err("parse error: invalid token: SELECTA".into());
            }
            Ok(
                // Returns a single-column single-row result, containing the sql string.
                PgResponse::new(
                    StatementType::SELECT,
                    1,
                    vec![Row::new(vec![Some(sql.to_string())])],
                    vec![PgFieldDescriptor::new("sql".to_string(), TypeOid::Varchar)],
                ),
            )
        }
    }

    #[tokio::test]
    /// Test the psql connection establish of PG server.
    async fn test_connection() {
        tokio::spawn(
            async move { pg_serve("127.0.0.1:45661", Arc::new(TestSessionManager {})).await },
        );
        // Connect to the database.
        let (client, connection) = tokio_postgres::connect("host=localhost port=45661", NoTls)
            .await
            .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Now we can execute a simple statement that just returns its AST.
        let query = "SELECT * from t;";
        let ret = client.simple_query(query).await.unwrap();
        assert_eq!(ret.len(), 2);
        for (idx, row) in ret.iter().enumerate() {
            if idx == 0 {
                if let SimpleQueryMessage::Row(row_inner) = row {
                    assert_eq!(row_inner.get(0), Some("SELECT * from t;"));
                } else {
                    panic!("The first message should be row values")
                }
            } else if idx == 1 {
                if let SimpleQueryMessage::CommandComplete(row_inner) = row {
                    assert_eq!(*row_inner, 1);
                } else {
                    panic!("The last message should be command complete")
                }
            }
        }

        let query2 = "SELECTA * from t;";
        let ret = client.simple_query(query2).await;
        assert!(ret.is_err());
        if let Err(e) = ret {
            // Internal error code.
            assert_eq!(e.code().unwrap().code(), "XX000");
        }
    }
}
