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

//! `Acl` defines all grantable privileges.

use std::fmt::Formatter;
use std::sync::LazyLock;

use enumflags2::{bitflags, make_bitflags, BitFlags};
use parse_display::Display;
use risingwave_pb::user::grant_privilege::PbAction;

#[bitflags]
#[repr(u64)]
#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum AclMode {
    #[display("a")]
    Insert = 1 << 0, // formerly known as "append".
    #[display("r")]
    Select = 1 << 1, // formerly known as "read".
    #[display("w")]
    Update = 1 << 2, // formerly known as "write".
    #[display("d")]
    Delete = 1 << 3,
    #[display("D")]
    Truncate = 1 << 4, // super-delete, as it were
    #[display("x")]
    References = 1 << 5,
    #[display("t")]
    Trigger = 1 << 6,
    #[display("X")]
    Execute = 1 << 7, // For functions
    #[display("U")]
    Usage = 1 << 8, // For various object types
    #[display("C")]
    Create = 1 << 9, // For namespaces and databases
    #[display("T")]
    CreateTemp = 1 << 10, // For databases
    #[display("c")]
    Connect = 1 << 11, // For databases
    #[display("s")]
    Set = 1 << 12, // For configuration parameters
    #[display("A")]
    AlterSystem = 1 << 13, // For configuration parameters
    #[display("m")]
    Maintain = 1 << 14, // For relations
}

impl From<PbAction> for AclMode {
    fn from(action: PbAction) -> Self {
        match action {
            PbAction::Unspecified => unreachable!(),
            PbAction::Select => AclMode::Select,
            PbAction::Insert => AclMode::Insert,
            PbAction::Update => AclMode::Update,
            PbAction::Delete => AclMode::Delete,
            PbAction::Create => AclMode::Create,
            PbAction::Connect => AclMode::Connect,
            PbAction::Usage => AclMode::Usage,
            PbAction::Execute => AclMode::Execute,
        }
    }
}

impl From<AclMode> for PbAction {
    fn from(val: AclMode) -> Self {
        match val {
            AclMode::Select => PbAction::Select,
            AclMode::Insert => PbAction::Insert,
            AclMode::Update => PbAction::Update,
            AclMode::Delete => PbAction::Delete,
            AclMode::Create => PbAction::Create,
            AclMode::Connect => PbAction::Connect,
            AclMode::Usage => PbAction::Usage,
            AclMode::Execute => PbAction::Execute,
            _ => unreachable!(),
        }
    }
}

/// `AclModeSet` defines a set of `AclMode`s.
#[derive(Clone, Debug)]
pub struct AclModeSet {
    pub modes: BitFlags<AclMode>,
}

pub static ALL_AVAILABLE_DATABASE_MODES: LazyLock<AclModeSet> =
    LazyLock::new(|| make_bitflags!(AclMode::{Create | Connect}).into());
pub static ALL_AVAILABLE_SCHEMA_MODES: LazyLock<AclModeSet> =
    LazyLock::new(|| make_bitflags!(AclMode::{Create | Usage}).into());
// Including TABLES and VIEWS
pub static ALL_AVAILABLE_TABLE_MODES: LazyLock<AclModeSet> =
    LazyLock::new(|| make_bitflags!(AclMode::{Select | Insert | Update | Delete}).into());
pub static ALL_AVAILABLE_SOURCE_MODES: LazyLock<AclModeSet> = LazyLock::new(AclModeSet::readonly);
pub static ALL_AVAILABLE_MVIEW_MODES: LazyLock<AclModeSet> = LazyLock::new(AclModeSet::readonly);
pub static ALL_AVAILABLE_SINK_MODES: LazyLock<AclModeSet> = LazyLock::new(AclModeSet::readonly);
pub static ALL_AVAILABLE_SUBSCRIPTION_MODES: LazyLock<AclModeSet> =
    LazyLock::new(AclModeSet::readonly);
pub static ALL_AVAILABLE_FUNCTION_MODES: LazyLock<AclModeSet> =
    LazyLock::new(|| BitFlags::from(AclMode::Execute).into());
pub static ALL_AVAILABLE_CONNECTION_MODES: LazyLock<AclModeSet> =
    LazyLock::new(|| BitFlags::from(AclMode::Usage).into());

impl AclModeSet {
    #[allow(dead_code)]
    pub fn empty() -> Self {
        Self {
            modes: BitFlags::empty(),
        }
    }

    pub fn readonly() -> Self {
        Self {
            modes: BitFlags::from(AclMode::Select),
        }
    }

    pub fn has_mode(&self, mode: AclMode) -> bool {
        self.modes.contains(mode)
    }

    pub fn iter(&self) -> impl Iterator<Item = AclMode> + '_ {
        self.modes.iter()
    }
}

impl From<BitFlags<AclMode>> for AclModeSet {
    fn from(modes: BitFlags<AclMode>) -> Self {
        Self { modes }
    }
}

impl std::fmt::Display for AclModeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.modes)
    }
}
