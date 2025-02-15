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

use expr_node::Type;
use risingwave_pb::expr::expr_node;

use super::{ExprImpl, ExprVisitor};
use crate::expr::FunctionCall;

#[derive(Default)]
pub(crate) struct ImpureAnalyzer {
    pub(crate) impure: bool,
}

impl ExprVisitor for ImpureAnalyzer {
    fn visit_user_defined_function(&mut self, _func_call: &super::UserDefinedFunction) {
        self.impure = true;
    }

    fn visit_now(&mut self, _: &super::Now) {
        self.impure = true;
    }

    fn visit_function_call(&mut self, func_call: &super::FunctionCall) {
        match func_call.func_type() {
            Type::Unspecified => unreachable!(),
            Type::Add
            | Type::Subtract
            | Type::Multiply
            | Type::Divide
            | Type::Modulus
            | Type::Equal
            | Type::NotEqual
            | Type::LessThan
            | Type::LessThanOrEqual
            | Type::GreaterThan
            | Type::GreaterThanOrEqual
            | Type::And
            | Type::Or
            | Type::Not
            | Type::In
            | Type::Some
            | Type::All
            | Type::BitwiseAnd
            | Type::BitwiseOr
            | Type::BitwiseXor
            | Type::BitwiseNot
            | Type::BitwiseShiftLeft
            | Type::BitwiseShiftRight
            | Type::Extract
            | Type::DatePart
            | Type::TumbleStart
            | Type::SecToTimestamptz
            | Type::AtTimeZone
            | Type::DateTrunc
            | Type::MakeDate
            | Type::MakeTime
            | Type::MakeTimestamp
            | Type::MakeTimestampNs
            | Type::CharToTimestamptz
            | Type::CharToDate
            | Type::CastWithTimeZone
            | Type::AddWithTimeZone
            | Type::SubtractWithTimeZone
            | Type::Cast
            | Type::Substr
            | Type::Length
            | Type::Like
            | Type::ILike
            | Type::SimilarToEscape
            | Type::Upper
            | Type::Lower
            | Type::Trim
            | Type::Replace
            | Type::Position
            | Type::Ltrim
            | Type::Rtrim
            | Type::Case
            | Type::ConstantLookup
            | Type::RoundDigit
            | Type::Round
            | Type::Ascii
            | Type::Translate
            | Type::Coalesce
            | Type::ConcatWs
            | Type::ConcatWsVariadic
            | Type::Abs
            | Type::SplitPart
            | Type::Ceil
            | Type::Floor
            | Type::Trunc
            | Type::ToChar
            | Type::Md5
            | Type::CharLength
            | Type::Repeat
            | Type::ConcatOp
            | Type::Concat
            | Type::ConcatVariadic
            | Type::BoolOut
            | Type::OctetLength
            | Type::BitLength
            | Type::Overlay
            | Type::RegexpMatch
            | Type::RegexpReplace
            | Type::RegexpCount
            | Type::RegexpSplitToArray
            | Type::RegexpEq
            | Type::Pow
            | Type::Exp
            | Type::Ln
            | Type::Log10
            | Type::Chr
            | Type::StartsWith
            | Type::Initcap
            | Type::Lpad
            | Type::Rpad
            | Type::Reverse
            | Type::Strpos
            | Type::ToAscii
            | Type::ToHex
            | Type::QuoteIdent
            | Type::Sin
            | Type::Cos
            | Type::Tan
            | Type::Cot
            | Type::Asin
            | Type::Acos
            | Type::Acosd
            | Type::Atan
            | Type::Atan2
            | Type::Sqrt
            | Type::Cbrt
            | Type::Sign
            | Type::Scale
            | Type::MinScale
            | Type::TrimScale
            | Type::Left
            | Type::Right
            | Type::Degrees
            | Type::Radians
            | Type::IsTrue
            | Type::IsNotTrue
            | Type::IsFalse
            | Type::IsNotFalse
            | Type::IsNull
            | Type::IsNotNull
            | Type::IsDistinctFrom
            | Type::IsNotDistinctFrom
            | Type::Neg
            | Type::Field
            | Type::Array
            | Type::ArrayAccess
            | Type::ArrayRangeAccess
            | Type::Row
            | Type::ArrayToString
            | Type::ArrayCat
            | Type::ArrayMax
            | Type::ArraySum
            | Type::ArraySort
            | Type::ArrayAppend
            | Type::ArrayPrepend
            | Type::FormatType
            | Type::ArrayDistinct
            | Type::ArrayMin
            | Type::ArrayDims
            | Type::ArrayLength
            | Type::Cardinality
            | Type::TrimArray
            | Type::ArrayRemove
            | Type::ArrayReplace
            | Type::ArrayPosition
            | Type::ArrayContains
            | Type::ArrayContained
            | Type::HexToInt256
            | Type::JsonbConcat
            | Type::JsonbAccess
            | Type::JsonbAccessStr
            | Type::JsonbExtractPath
            | Type::JsonbExtractPathVariadic
            | Type::JsonbExtractPathText
            | Type::JsonbExtractPathTextVariadic
            | Type::JsonbTypeof
            | Type::JsonbArrayLength
            | Type::JsonbObject
            | Type::JsonbPretty
            | Type::JsonbDeletePath
            | Type::JsonbContains
            | Type::JsonbContained
            | Type::JsonbExists
            | Type::JsonbExistsAny
            | Type::JsonbExistsAll
            | Type::JsonbStripNulls
            | Type::JsonbBuildArray
            | Type::JsonbBuildArrayVariadic
            | Type::JsonbBuildObject
            | Type::JsonbPopulateRecord
            | Type::JsonbToRecord
            | Type::JsonbBuildObjectVariadic
            | Type::JsonbPathExists
            | Type::JsonbPathMatch
            | Type::JsonbPathQueryArray
            | Type::JsonbPathQueryFirst
            | Type::JsonbSet
            | Type::JsonbPopulateMap
            | Type::IsJson
            | Type::ToJsonb
            | Type::Sind
            | Type::Cosd
            | Type::Cotd
            | Type::Asind
            | Type::Sinh
            | Type::Cosh
            | Type::Coth
            | Type::Tanh
            | Type::Atanh
            | Type::Asinh
            | Type::Acosh
            | Type::Decode
            | Type::Encode
            | Type::Sha1
            | Type::Sha224
            | Type::Sha256
            | Type::Sha384
            | Type::Sha512
            | Type::Hmac
            | Type::SecureCompare
            | Type::Decrypt
            | Type::Encrypt
            | Type::Tand
            | Type::ArrayPositions
            | Type::StringToArray
            | Type::Format
            | Type::FormatVariadic
            | Type::PgwireSend
            | Type::PgwireRecv
            | Type::ArrayTransform
            | Type::Greatest
            | Type::Least
            | Type::ConvertFrom
            | Type::ConvertTo
            | Type::IcebergTransform
            | Type::InetNtoa
            | Type::InetAton
            | Type::QuoteLiteral
            | Type::QuoteNullable
            | Type::MapFromEntries
            | Type::MapAccess
            | Type::MapKeys
            | Type::MapValues
            | Type::MapEntries
            | Type::MapFromKeyValues
            | Type::MapCat
            | Type::MapContains
            | Type::MapDelete
            | Type::MapInsert
            | Type::MapLength
            | Type::VnodeUser
            |Type::RwEpochToTs =>
            // expression output is deterministic(same result for the same input)
            {
                func_call
                    .inputs()
                    .iter()
                    .for_each(|expr| self.visit_expr(expr));
            }
            // expression output is not deterministic
            Type::Vnode // obtain vnode count from the context
            | Type::TestPaidTier
            | Type::Proctime
            | Type::PgSleep
            | Type::PgSleepFor
            | Type::PgSleepUntil
            | Type::CastRegclass
            | Type::PgGetIndexdef
            | Type::ColDescription
            | Type::PgGetViewdef
            | Type::PgGetUserbyid
            | Type::PgIndexesSize
            | Type::PgRelationSize
            | Type::PgGetSerialSequence
            | Type::PgIndexColumnHasProperty
            | Type::HasTablePrivilege
            | Type::HasAnyColumnPrivilege
            | Type::HasSchemaPrivilege
            | Type::MakeTimestamptz
            | Type::PgIsInRecovery
            | Type::RwRecoveryStatus
            | Type::PgTableIsVisible => self.impure = true,
        }
    }
}

pub fn is_pure(expr: &ExprImpl) -> bool {
    !is_impure(expr)
}

pub fn is_impure(expr: &ExprImpl) -> bool {
    let mut a = ImpureAnalyzer::default();
    a.visit_expr(expr);
    a.impure
}

pub fn is_impure_func_call(func_call: &FunctionCall) -> bool {
    let mut a = ImpureAnalyzer::default();
    a.visit_function_call(func_call);
    a.impure
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use crate::expr::{is_impure, is_pure, ExprImpl, FunctionCall, InputRef};

    fn expect_pure(expr: &ExprImpl) {
        assert!(is_pure(expr));
        assert!(!is_impure(expr));
    }

    fn expect_impure(expr: &ExprImpl) {
        assert!(!is_pure(expr));
        assert!(is_impure(expr));
    }

    #[test]
    fn test_pure_funcs() {
        let e: ExprImpl = FunctionCall::new(
            Type::Add,
            vec![
                InputRef::new(0, DataType::Int16).into(),
                InputRef::new(0, DataType::Int16).into(),
            ],
        )
        .unwrap()
        .into();
        expect_pure(&e);

        let e: ExprImpl = FunctionCall::new(
            Type::GreaterThan,
            vec![
                InputRef::new(0, DataType::Timestamptz).into(),
                FunctionCall::new(Type::Proctime, vec![]).unwrap().into(),
            ],
        )
        .unwrap()
        .into();
        expect_impure(&e);
    }
}
