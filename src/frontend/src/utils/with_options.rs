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

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use risingwave_common::catalog::ConnectionId;
use risingwave_connector::source::kafka::private_link::{
    insert_privatelink_broker_rewrite_map, PRIVATELINK_ENDPOINT_KEY,
};
pub use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::WithPropertiesExt;
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::connection_params::PbConnectionType;
use risingwave_pb::secret::secret_ref::PbRefAsType;
use risingwave_pb::secret::PbSecretRef;
use risingwave_sqlparser::ast::{
    ConnectionRefValue, CreateConnectionStatement, CreateSinkStatement, CreateSourceStatement,
    CreateSubscriptionStatement, SecretRefAsType, SecretRefValue, SqlOption, Statement, Value,
};

use super::OverwriteOptions;
use crate::error::{ErrorCode, Result as RwResult, RwError};
use crate::session::SessionImpl;
use crate::Binder;

mod options {

    pub const RETENTION_SECONDS: &str = "retention_seconds";
}

const CONNECTION_REF_KEY: &str = "profile";

/// Options or properties extracted from the `WITH` clause of DDLs.
#[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WithOptions {
    inner: BTreeMap<String, String>,
    secret_ref: BTreeMap<String, SecretRefValue>,
    connection_ref: BTreeMap<String, ConnectionRefValue>,
}

impl std::ops::Deref for WithOptions {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for WithOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl WithOptions {
    /// Create a new [`WithOptions`] from a [`BTreeMap`].
    pub fn new_with_options(inner: BTreeMap<String, String>) -> Self {
        Self {
            inner,
            secret_ref: Default::default(),
            connection_ref: Default::default(),
        }
    }

    /// Create a new [`WithOptions`] from a option [`BTreeMap`] and secret ref.
    pub fn new(
        inner: BTreeMap<String, String>,
        secret_ref: BTreeMap<String, SecretRefValue>,
        connection_ref: BTreeMap<String, ConnectionRefValue>,
    ) -> Self {
        Self {
            inner,
            secret_ref,
            connection_ref,
        }
    }

    pub fn inner_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.inner
    }

    /// Take the value of the option map and secret refs.
    pub fn into_parts(
        self,
    ) -> (
        BTreeMap<String, String>,
        BTreeMap<String, SecretRefValue>,
        BTreeMap<String, ConnectionRefValue>,
    ) {
        (self.inner, self.secret_ref, self.connection_ref)
    }

    /// Convert to connector props, remove the key-value pairs used in the top-level.
    pub fn into_connector_props(self) -> WithOptions {
        let inner = self
            .inner
            .into_iter()
            .filter(|(key, _)| {
                key != OverwriteOptions::SOURCE_RATE_LIMIT_KEY && key != options::RETENTION_SECONDS
            })
            .collect();

        Self {
            inner,
            secret_ref: self.secret_ref,
            connection_ref: self.connection_ref,
        }
    }

    /// Parse the retention seconds from the options.
    pub fn retention_seconds(&self) -> Option<NonZeroU32> {
        self.inner
            .get(options::RETENTION_SECONDS)
            .and_then(|s| s.parse().ok())
    }

    /// Get a subset of the options from the given keys.
    pub fn subset(&self, keys: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        let inner = keys
            .into_iter()
            .filter_map(|k| {
                self.inner
                    .get_key_value(k.as_ref())
                    .map(|(k, v)| (k.clone(), v.clone()))
            })
            .collect();

        Self {
            inner,
            secret_ref: self.secret_ref.clone(),
            connection_ref: self.connection_ref.clone(),
        }
    }

    pub fn value_eq_ignore_case(&self, key: &str, val: &str) -> bool {
        if let Some(inner_val) = self.inner.get(key) {
            if inner_val.eq_ignore_ascii_case(val) {
                return true;
            }
        }
        false
    }

    pub fn secret_ref(&self) -> &BTreeMap<String, SecretRefValue> {
        &self.secret_ref
    }

    pub fn secret_ref_mut(&mut self) -> &mut BTreeMap<String, SecretRefValue> {
        &mut self.secret_ref
    }

    pub fn connection_ref(&self) -> &BTreeMap<String, ConnectionRefValue> {
        &self.connection_ref
    }

    pub fn connection_ref_mut(&mut self) -> &mut BTreeMap<String, ConnectionRefValue> {
        &mut self.connection_ref
    }

    pub fn oauth_options_to_map(sql_options: &[SqlOption]) -> RwResult<BTreeMap<String, String>> {
        let WithOptions {
            inner, secret_ref, ..
        } = WithOptions::try_from(sql_options)?;
        if secret_ref.is_empty() {
            Ok(inner)
        } else {
            Err(RwError::from(ErrorCode::InvalidParameterValue(
                "Secret reference is not allowed in OAuth options".to_string(),
            )))
        }
    }
}

pub(crate) fn resolve_connection_ref_and_secret_ref(
    with_options: WithOptions,
    session: &SessionImpl,
) -> RwResult<(WithOptionsSecResolved, PbConnectionType)> {
    let db_name: &str = session.database();
    let (mut options, secret_refs, connection_refs) = with_options.clone().into_parts();

    let mut connection_params = None;
    for connection_ref in connection_refs.values() {
        // at most one connection ref in the map
        connection_params = {
            // get connection params from catalog
            let (schema_name, connection_name) = Binder::resolve_schema_qualified_name(
                db_name,
                connection_ref.connection_name.clone(),
            )?;
            let connection_catalog =
                session.get_connection_by_name(schema_name, &connection_name)?;
            if let ConnectionInfo::ConnectionParams(params) = &connection_catalog.info {
                Some(params.clone())
            } else {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(
                    "Private Link Service has been deprecated. Please create a new connection instead."
                        .to_string(),
        )));
            }
        };
    }

    let mut inner_secret_refs = {
        let mut resolved_secret_refs = BTreeMap::new();
        for (key, secret_ref) in secret_refs {
            let (schema_name, secret_name) =
                Binder::resolve_schema_qualified_name(db_name, secret_ref.secret_name.clone())?;
            let secret_catalog = session.get_secret_by_name(schema_name, &secret_name)?;
            let ref_as = match secret_ref.ref_as {
                SecretRefAsType::Text => PbRefAsType::Text,
                SecretRefAsType::File => PbRefAsType::File,
            };
            let pb_secret_ref = PbSecretRef {
                secret_id: secret_catalog.id.secret_id(),
                ref_as: ref_as.into(),
            };
            resolved_secret_refs.insert(key.clone(), pb_secret_ref);
        }
        resolved_secret_refs
    };

    let mut connection_type = PbConnectionType::Unspecified;
    if let Some(connection_params) = connection_params {
        connection_type = connection_params.connection_type();
        for (k, v) in connection_params.properties {
            if options.insert(k.clone(), v).is_some() {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                    "Duplicated key: {}",
                    k
                ))));
            }
        }

        for (k, v) in connection_params.secret_refs {
            if inner_secret_refs.insert(k.clone(), v).is_some() {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                    "Duplicated key: {}",
                    k
                ))));
            }
        }
    }

    Ok((
        WithOptionsSecResolved::new(options, inner_secret_refs),
        connection_type,
    ))
}

/// Get the secret id from the name.
pub(crate) fn resolve_secret_ref_in_with_options(
    with_options: WithOptions,
    session: &SessionImpl,
) -> RwResult<WithOptionsSecResolved> {
    let (options, secret_refs, _) = with_options.into_parts();
    let mut resolved_secret_refs = BTreeMap::new();
    let db_name: &str = session.database();
    for (key, secret_ref) in secret_refs {
        let (schema_name, secret_name) =
            Binder::resolve_schema_qualified_name(db_name, secret_ref.secret_name.clone())?;
        let secret_catalog = session.get_secret_by_name(schema_name, &secret_name)?;
        let ref_as = match secret_ref.ref_as {
            SecretRefAsType::Text => PbRefAsType::Text,
            SecretRefAsType::File => PbRefAsType::File,
        };
        let pb_secret_ref = PbSecretRef {
            secret_id: secret_catalog.id.secret_id(),
            ref_as: ref_as.into(),
        };
        resolved_secret_refs.insert(key.clone(), pb_secret_ref);
    }
    Ok(WithOptionsSecResolved::new(options, resolved_secret_refs))
}

pub(crate) fn resolve_privatelink_in_with_option(
    with_options: &mut WithOptions,
) -> RwResult<Option<ConnectionId>> {
    let is_kafka = with_options.is_kafka_connector();
    let privatelink_endpoint = with_options.remove(PRIVATELINK_ENDPOINT_KEY);

    // if `privatelink.endpoint` is provided in WITH, use it to rewrite broker address directly
    if let Some(endpoint) = privatelink_endpoint {
        if !is_kafka {
            return Err(RwError::from(ErrorCode::ProtocolError(
                "Privatelink is only supported in kafka connector".to_string(),
            )));
        }
        insert_privatelink_broker_rewrite_map(with_options.inner_mut(), None, Some(endpoint))
            .map_err(RwError::from)?;
    }
    Ok(None)
}

impl TryFrom<&[SqlOption]> for WithOptions {
    type Error = RwError;

    fn try_from(options: &[SqlOption]) -> Result<Self, Self::Error> {
        let mut inner: BTreeMap<String, String> = BTreeMap::new();
        let mut secret_ref: BTreeMap<String, SecretRefValue> = BTreeMap::new();
        let mut connection_ref: BTreeMap<String, ConnectionRefValue> = BTreeMap::new();
        for option in options {
            let key = option.name.real_value();
            match &option.value {
                Value::SecretRef(r) => {
                    if secret_ref.insert(key.clone(), r.clone()).is_some()
                        || inner.contains_key(&key)
                    {
                        return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                            "Duplicated option: {}",
                            key
                        ))));
                    }
                    continue;
                }
                Value::ConnectionRef(r) => {
                    if key != CONNECTION_REF_KEY {
                        return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                            "expect 'profile' as the key for connection ref, but got: {}",
                            key
                        ))));
                    }
                    if connection_ref.insert(key.clone(), r.clone()).is_some()
                        || inner.contains_key(&key)
                    {
                        return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                            "Duplicated option: {}",
                            key
                        ))));
                    }
                    continue;
                }
                _ => {}
            }
            let value: String = match option.value.clone() {
                Value::CstyleEscapedString(s) => s.value,
                Value::SingleQuotedString(s) => s,
                Value::Number(n) => n,
                Value::Boolean(b) => b.to_string(),
                _ => {
                    return Err(RwError::from(ErrorCode::InvalidParameterValue(
                        "`with options` or `with properties` only support single quoted string value and C style escaped string"
                            .to_owned(),
                    )))
                }
            };
            if inner.insert(key.clone(), value).is_some() || secret_ref.contains_key(&key) {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                    "Duplicated option: {}",
                    key
                ))));
            }
        }

        Ok(Self {
            inner,
            secret_ref,
            connection_ref,
        })
    }
}

impl TryFrom<&Statement> for WithOptions {
    type Error = RwError;

    /// Extract options from the `WITH` clause from the given statement.
    fn try_from(statement: &Statement) -> Result<Self, Self::Error> {
        match statement {
            // Explain: forward to the inner statement.
            Statement::Explain { statement, .. } => Self::try_from(statement.as_ref()),

            // View
            Statement::CreateView { with_options, .. } => Self::try_from(with_options.as_slice()),

            // Sink
            Statement::CreateSink {
                stmt:
                    CreateSinkStatement {
                        with_properties, ..
                    },
            }
            | Statement::CreateConnection {
                stmt:
                    CreateConnectionStatement {
                        with_properties, ..
                    },
            } => Self::try_from(with_properties.0.as_slice()),
            Statement::CreateSource {
                stmt:
                    CreateSourceStatement {
                        with_properties, ..
                    },
                ..
            } => Self::try_from(with_properties.0.as_slice()),
            Statement::CreateSubscription {
                stmt:
                    CreateSubscriptionStatement {
                        with_properties, ..
                    },
                ..
            } => Self::try_from(with_properties.0.as_slice()),
            Statement::CreateTable { with_options, .. } => Self::try_from(with_options.as_slice()),

            _ => Ok(Default::default()),
        }
    }
}
