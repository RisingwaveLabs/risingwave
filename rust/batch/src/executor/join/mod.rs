use risingwave_pb::plan::JoinType as JoinTypeProst;

use crate::executor::join::JoinType::Inner;

mod chunked_data;
mod hash_join;
pub use hash_join::*;

mod hash_join_state;
pub mod nested_loop_join;
pub mod row_level_iter;
pub mod sort_merge_join;
#[derive(Copy, Clone, Debug)]
pub(super) enum JoinType {
    Inner,
    LeftOuter,
    /// Semi join when probe side should output when matched
    LeftSemi,
    /// Anti join when probe side should not output when matched
    LeftAnti,
    RightOuter,
    /// Semi join when build side should output when matched
    RightSemi,
    /// Anti join when build side should output when matched
    RightAnti,
    FullOuter,
}

impl JoinType {
    #[inline(always)]
    pub(super) fn need_join_remaining(self) -> bool {
        matches!(
            self,
            JoinType::RightOuter | JoinType::RightAnti | JoinType::FullOuter
        )
    }

    pub fn from_prost(prost: JoinTypeProst) -> Self {
        match prost {
            JoinTypeProst::Inner => JoinType::Inner,
            JoinTypeProst::LeftOuter => JoinType::LeftOuter,
            JoinTypeProst::LeftSemi => JoinType::LeftSemi,
            JoinTypeProst::LeftAnti => JoinType::LeftAnti,
            JoinTypeProst::RightOuter => JoinType::RightOuter,
            JoinTypeProst::RightSemi => JoinType::RightSemi,
            JoinTypeProst::RightAnti => JoinType::RightAnti,
            JoinTypeProst::FullOuter => JoinType::FullOuter,
        }
    }

    fn need_build_flag(self) -> bool {
        match self {
            JoinType::RightSemi => true,
            other => other.need_join_remaining(),
        }
    }

    fn need_probe_flag(self) -> bool {
        matches!(
            self,
            JoinType::FullOuter | JoinType::LeftOuter | JoinType::LeftAnti | JoinType::LeftSemi
        )
    }
}

impl Default for JoinType {
    fn default() -> Self {
        Inner
    }
}
