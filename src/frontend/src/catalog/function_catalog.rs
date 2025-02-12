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

use enum_as_inner::EnumAsInner;
use parse_display::Display;
use risingwave_common::catalog::FunctionId;
use risingwave_common::types::DataType;
use risingwave_pb::catalog::function::PbKind;
use risingwave_pb::catalog::PbFunction;
use risingwave_pb::expr::PbUserDefinedFunctionMetadata;

use crate::catalog::OwnedByUserCatalog;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FunctionCatalog {
    pub id: FunctionId,
    pub name: String,
    pub owner: u32,
    pub kind: FunctionKind,
    pub arg_names: Vec<String>,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub language: String,
    pub runtime: Option<String>,
    pub identifier: Option<String>,
    pub body: Option<String>,
    pub link: Option<String>,
    pub compressed_binary: Option<Vec<u8>>,
    pub always_retry_on_network_error: bool,
    pub is_async: Option<bool>,
    pub is_batched: Option<bool>,
}

#[derive(Clone, Display, PartialEq, Eq, Hash, Debug, EnumAsInner)]
#[display(style = "UPPERCASE")]
pub enum FunctionKind {
    Scalar,
    Table,
    Aggregate,
}

impl From<&PbKind> for FunctionKind {
    fn from(prost: &PbKind) -> Self {
        use risingwave_pb::catalog::function::*;
        match prost {
            Kind::Scalar(ScalarFunction {}) => Self::Scalar,
            Kind::Table(TableFunction {}) => Self::Table,
            Kind::Aggregate(AggregateFunction {}) => Self::Aggregate,
        }
    }
}

impl From<&PbFunction> for FunctionCatalog {
    fn from(prost: &PbFunction) -> Self {
        FunctionCatalog {
            id: prost.id.into(),
            name: prost.name.clone(),
            owner: prost.owner,
            kind: prost.kind.as_ref().unwrap().into(),
            arg_names: prost.arg_names.clone(),
            arg_types: prost.arg_types.iter().map(|arg| arg.into()).collect(),
            return_type: prost.return_type.as_ref().expect("no return type").into(),
            language: prost.language.clone(),
            runtime: prost.runtime.clone(),
            identifier: prost.identifier.clone(),
            body: prost.body.clone(),
            link: prost.link.clone(),
            compressed_binary: prost.compressed_binary.clone(),
            always_retry_on_network_error: prost.always_retry_on_network_error,
            is_async: prost.is_async,
            is_batched: prost.is_batched,
        }
    }
}

impl From<&FunctionCatalog> for PbUserDefinedFunctionMetadata {
    fn from(c: &FunctionCatalog) -> Self {
        PbUserDefinedFunctionMetadata {
            arg_names: c.arg_names.clone(),
            arg_types: c.arg_types.iter().map(|t| t.to_protobuf()).collect(),
            return_type: Some(c.return_type.to_protobuf()),
            language: c.language.clone(),
            runtime: c.runtime.clone(),
            link: c.link.clone(),
            identifier: c.identifier.clone(),
            body: c.body.clone(),
            compressed_binary: c.compressed_binary.clone(),
        }
    }
}

impl OwnedByUserCatalog for FunctionCatalog {
    fn owner(&self) -> u32 {
        self.owner
    }
}
