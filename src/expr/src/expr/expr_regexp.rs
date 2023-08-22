// Copyright 2023 RisingWave Labs
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

use std::str::FromStr;
use std::sync::Arc;

use itertools::Itertools;
use regex::{Regex, RegexBuilder};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayRef, DataChunk, ListArrayBuilder, ListRef, ListValue, Utf8Array,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{build_from_prost as expr_build_from_prost, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct RegexpContext(pub Regex);

impl RegexpContext {
    pub fn new(pattern: &str, flags: &str) -> Result<Self> {
        let options = RegexpOptions::from_str(flags)?;
        Ok(Self(
            RegexBuilder::new(pattern)
                .case_insensitive(options.case_insensitive)
                .build()?,
        ))
    }

    pub fn from_pattern(pattern: Datum) -> Result<Self> {
        let pattern = match &pattern {
            None => NULL_PATTERN,
            Some(ScalarImpl::Utf8(s)) => s.as_ref(),
            _ => bail!("invalid pattern: {pattern:?}"),
        };
        Self::new(pattern, "")
    }

    pub fn from_pattern_flags(pattern: Datum, flags: Datum) -> Result<Self> {
        let pattern = match (&pattern, &flags) {
            (None, _) | (_, None) => NULL_PATTERN,
            (Some(ScalarImpl::Utf8(s)), _) => s.as_ref(),
            _ => bail!("invalid pattern: {pattern:?}"),
        };
        let flags = match &flags {
            None => "",
            Some(ScalarImpl::Utf8(s)) => s.as_ref(),
            _ => bail!("invalid flags: {flags:?}"),
        };
        Self::new(pattern, flags)
    }
}

/// <https://www.postgresql.org/docs/current/functions-matching.html#POSIX-EMBEDDED-OPTIONS-TABLE>
struct RegexpOptions {
    /// `c` and `i`
    case_insensitive: bool,
}

#[expect(clippy::derivable_impls)]
impl Default for RegexpOptions {
    fn default() -> Self {
        Self {
            case_insensitive: false,
        }
    }
}

impl FromStr for RegexpOptions {
    type Err = ExprError;

    fn from_str(s: &str) -> Result<Self> {
        let mut opts = Self::default();
        for c in s.chars() {
            match c {
                // Case sensitive matching here
                'c' => opts.case_insensitive = false,
                // Case insensitive matching here
                'i' => opts.case_insensitive = true,
                // Not yet support
                'g' => {}
                _ => {
                    bail!("invalid regular expression option: \"{c}\"");
                }
            }
        }
        Ok(opts)
    }
}

#[derive(Debug)]
pub struct RegexpMatchExpression {
    pub child: Box<dyn Expression>,
    pub ctx: RegexpContext,
}

/// The pattern that matches nothing.
pub const NULL_PATTERN: &str = "a^";

impl<'a> TryFrom<&'a ExprNode> for RegexpMatchExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_function_type().unwrap() == Type::RegexpMatch);
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        let mut children = func_call_node.children.iter();
        let Some(text_node) = children.next() else {
            bail!("Expected argument text");
        };
        let text_expr = expr_build_from_prost(text_node)?;
        let Some(pattern_node) = children.next() else {
            bail!("Expected argument pattern");
        };
        let mut pattern = match &pattern_node.get_rex_node()? {
            RexNode::Constant(pattern_value) => {
                let pattern_datum = deserialize_datum(
                    pattern_value.get_body().as_slice(),
                    &DataType::from(pattern_node.get_return_type().unwrap()),
                )
                .map_err(|e| ExprError::Internal(e.into()))?;

                match pattern_datum {
                    Some(ScalarImpl::Utf8(pattern)) => pattern.to_string(),
                    // NULL pattern
                    None => NULL_PATTERN.to_string(),
                    _ => bail!("Expected pattern to be a String"),
                }
            }
            _ => {
                return Err(ExprError::UnsupportedFunction(
                    "non-constant pattern in regexp_match".to_string(),
                ))
            }
        };

        let flags = if let Some(flags_node) = children.next() {
            match &flags_node.get_rex_node()? {
                RexNode::Constant(flags_value) => {
                    let flags_datum = deserialize_datum(
                        flags_value.get_body().as_slice(),
                        &DataType::from(flags_node.get_return_type().unwrap()),
                    )
                    .map_err(|e| ExprError::Internal(e.into()))?;

                    match flags_datum {
                        Some(ScalarImpl::Utf8(flags)) => flags.to_string(),
                        // NULL flag
                        None => {
                            pattern = NULL_PATTERN.to_string();
                            "".to_string()
                        }
                        _ => bail!("Expected flags to be a String"),
                    }
                }
                _ => {
                    return Err(ExprError::UnsupportedFunction(
                        "non-constant flags in regexp_match".to_string(),
                    ))
                }
            }
        } else {
            "".to_string()
        };

        let ctx = RegexpContext::new(&pattern, &flags)?;
        Ok(Self {
            child: text_expr,
            ctx,
        })
    }
}

impl RegexpMatchExpression {
    /// Match one row and return the result.
    // TODO: The optimization can be allocated.
    fn match_one(&self, text: Option<&str>) -> Option<ListValue> {
        // If there are multiple captures, then the first one is the whole match, and should be
        // ignored in PostgreSQL's behavior.
        let skip_flag = self.ctx.0.captures_len() > 1;

        if let Some(text) = text {
            if let Some(capture) = self.ctx.0.captures(text) {
                let list = capture
                    .iter()
                    .skip(if skip_flag { 1 } else { 0 })
                    .map(|mat| mat.map(|m| m.as_str().into()))
                    .collect_vec();
                let list = ListValue::new(list);
                Some(list)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl Expression for RegexpMatchExpression {
    fn return_type(&self) -> DataType {
        DataType::List(Box::new(DataType::Varchar))
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let text_arr = self.child.eval_checked(input).await?;
        let text_arr: &Utf8Array = text_arr.as_ref().into();
        let mut output = ListArrayBuilder::with_type(
            input.capacity(),
            DataType::List(Box::new(DataType::Varchar)),
        );

        for (text, vis) in text_arr.iter().zip_eq_fast(input.vis().iter()) {
            if !vis {
                output.append_null();
            } else if let Some(list) = self.match_one(text) {
                let list_ref = ListRef::ValueRef { val: &list };
                output.append(Some(list_ref));
            } else {
                output.append_null();
            }
        }

        Ok(Arc::new(output.finish().into()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let text = self.child.eval_row(input).await?;
        Ok(if let Some(ScalarImpl::Utf8(text)) = text {
            self.match_one(Some(&text)).map(Into::into)
        } else {
            None
        })
    }
}

#[derive(Debug)]
pub struct RegexpReplaceExpression {
    /// The source to be matched and replaced
    pub source: Box<dyn Expression>,
    /// The regex context, used to match the given pattern
    pub ctx: RegexpContext,
    /// The replacement string
    pub replacement: String,
    /// The actual return type by evaluating this expression
    pub return_type: DataType,
    /// The start position to replace the source
    /// The starting index should be `0`
    pub start: i32,
    /// The N, used to specified the N-th position to be replaced
    /// Note that this field is only available if `start` > 0
    pub n: i32,
    /// Indicates if the `-g` flag is specified
    pub global_flag: bool,
}

/// This trait provides the transformation from `ExprNode` to `RegexpReplaceExpression`
impl<'a> TryFrom<&'a ExprNode> for RegexpReplaceExpression {
    type Error = ExprError;

    /// Try to convert the given `ExprNode` to the replace expression
    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        // The function type must be of Type::RegexpReplace
        ensure!(prost.get_function_type().unwrap() == Type::RegexpReplace);

        // Get the return type first
        let return_type = DataType::from(prost.get_return_type().unwrap());

        // Get the top node, which must be the function call node in this case
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

        // The children node, must contain `source`, `pattern`, `replacement`
        // `start, N`, `flags` are optional
        let mut children = func_call_node.children.iter();

        // Get the source expression, will be used as the `child` in replace expr
        let Some(source_node) = children.next() else {
            bail!("Expected argument text");
        };
        let source = expr_build_from_prost(source_node)?;

        // Get the regex pattern of this call
        let Some(pattern_node) = children.next() else {
            bail!("Expected argument pattern");
        };
        // Store the pattern as the string, to pass in the regex context
        let pattern = match &pattern_node.get_rex_node()? {
            RexNode::Constant(pattern_value) => {
                let pattern_datum = deserialize_datum(
                    pattern_value.get_body().as_slice(),
                    &DataType::from(pattern_node.get_return_type().unwrap()),
                )
                .map_err(|e| ExprError::Internal(e.into()))?;

                match pattern_datum {
                    Some(ScalarImpl::Utf8(pattern)) => pattern.to_string(),
                    // NULL pattern
                    None => NULL_PATTERN.to_string(),
                    _ => bail!("Expected pattern to be a String"),
                }
            }
            _ => {
                return Err(ExprError::UnsupportedFunction(
                    "non-constant pattern in regexp_replace".to_string(),
                ))
            }
        };

        // Get the replacement string of this call
        let Some(replacement_node) = children.next() else {
            bail!("Expected argument replacement");
        };
        // Same as the pattern above, store as the string
        let replacement = match &replacement_node.get_rex_node()? {
            RexNode::Constant(replacement_value) => {
                let replacement_datum = deserialize_datum(
                    replacement_value.get_body().as_slice(),
                    &DataType::from(replacement_node.get_return_type().unwrap()),
                )
                .map_err(|e| ExprError::Internal(e.into()))?;

                match replacement_datum {
                    Some(ScalarImpl::Utf8(replacement)) => replacement.to_string(),
                    // NULL replacement
                    // FIXME: Do we need the NULL match arm here?
                    _ => bail!("Expected replacement to be a String"),
                }
            }
            _ => {
                return Err(ExprError::UnsupportedFunction(
                    "non-constant in regexp_replace".to_string(),
                ))
            }
        };

        // TODO: [, start [, N ]] [, flags ] nodes support
        let mut flags = String::from("");
        let mut start = -1;
        let mut n = -1;
        let mut n_flag = false;
        let mut f_flag = false;

        // Try to get the next possible node, see if any of the options are specified
        if let Some(placeholder_node) = children.next() {
            // Get the placeholder text first
            let _placeholder = match &placeholder_node.get_rex_node()? {
                RexNode::Constant(placeholder_value) => {
                    let placeholder_datum = deserialize_datum(
                        placeholder_value.get_body().as_slice(),
                        &DataType::from(placeholder_node.get_return_type().unwrap()),
                    )
                    .map_err(|e| ExprError::Internal(e.into()))?;

                    match placeholder_datum {
                        Some(ScalarImpl::Int32(v)) => {
                            start = v;
                            "".to_string()
                        }
                        Some(ScalarImpl::Utf8(v)) => {
                            // If the `start` is not specified
                            // Then this must be the `flags`
                            f_flag = true;
                            flags = v.to_string();
                            "".to_string()
                        }
                        // NULL replacement
                        // FIXME: Do we need the NULL match arm here?
                        _ => bail!("Expected extra option to be a String/Int32"),
                    }
                }
                _ => {
                    return Err(ExprError::UnsupportedFunction(
                        "non-constant in regexp_replace".to_string(),
                    ))
                }
            };

            // Get the next node
            if !f_flag {
                if let Some(placeholder_node) = children.next() {
                    // Get the text as above
                    let placeholder = match &placeholder_node.get_rex_node()? {
                        RexNode::Constant(placeholder_value) => {
                            let placeholder_datum = deserialize_datum(
                                placeholder_value.get_body().as_slice(),
                                &DataType::from(placeholder_node.get_return_type().unwrap()),
                            )
                            .map_err(|e| ExprError::Internal(e.into()))?;

                            match placeholder_datum {
                                Some(ScalarImpl::Int32(v)) => {
                                    n_flag = true;
                                    n = v;
                                    "".to_string()
                                }
                                Some(ScalarImpl::Utf8(v)) => v.to_string(),
                                // NULL replacement
                                // FIXME: Do we need the NULL match arm here?
                                _ => bail!("Expected extra option to be a String/Int32"),
                            }
                        }
                        _ => {
                            return Err(ExprError::UnsupportedFunction(
                                "non-constant in regexp_replace".to_string(),
                            ))
                        }
                    };

                    if n_flag {
                        // Check if any flag is specified
                        if let Some(flag_node) = children.next() {
                            // Get the flag
                            flags = match &flag_node.get_rex_node()? {
                                RexNode::Constant(flag_value) => {
                                    let flag_datum = deserialize_datum(
                                        flag_value.get_body().as_slice(),
                                        &DataType::from(flag_node.get_return_type().unwrap()),
                                    )
                                    .map_err(|e| ExprError::Internal(e.into()))?;

                                    match flag_datum {
                                        Some(ScalarImpl::Utf8(v)) => v.to_string(),
                                        // NULL replacement
                                        // FIXME: Do we need the NULL match arm here?
                                        _ => bail!("Expected flag to be a String"),
                                    }
                                }
                                _ => {
                                    return Err(ExprError::UnsupportedFunction(
                                        "non-constant in regexp_replace".to_string(),
                                    ))
                                }
                            };
                        }
                    } else {
                        flags = placeholder;
                    }
                }
            }
        }

        // TODO: Any other error handling?
        if let Some(_other) = children.next() {
            // There should not any other option after the `flags`
            bail!("invalid parameters specified in regexp_replace");
        }

        // Construct the final `RegexpReplaceExpression`
        let ctx = RegexpContext::new(&pattern, &flags)?;

        let mut global_flag = false;
        if !flags.is_empty() && flags.starts_with('g') {
            // Set the `global_flag` to true
            global_flag = true;
        }

        Ok(Self {
            source,
            ctx,
            replacement,
            return_type,
            start,
            n,
            global_flag,
        })
    }
}

impl RegexpReplaceExpression {
    /// Match and replace one row, return the replaced string
    fn match_row(&self, text: Option<&str>) -> Option<String> {
        if let Some(text) = text {
            // The start position to begin the search
            let start = if self.start != -1 { self.start - 1 } else { 0 };

            if self.global_flag {
                // `-g` enabled, we need to replace all the occurrence of the matched pattern
                if self.ctx.0.captures_len() <= 1 {
                    println!("Path One");
                    // There is no capture groups in the regex
                    return Some(
                        self.ctx
                            .0
                            .replace_all(&text[start as usize..], self.replacement.clone())
                            .into(),
                    );
                } else {
                    println!("Path Two");

                    // Get the replaced string
                    let regex = Regex::new(r"\\([1-9])").unwrap();
                    let replaced = regex.replace_all(&self.replacement, "$${$1}").to_string();
                    println!("replaced: {}", replaced);

                    // The position to start searching for replacement
                    let mut search_start = start as usize;

                    // Construct the return string
                    let mut ret = text[..search_start].to_string();

                    while let Some(capture) = self.ctx.0.captures(&text[search_start..]) {
                        let match_start = capture.get(0).unwrap().start();
                        let match_end = capture.get(0).unwrap().end();

                        if match_start == match_end {
                            // If this is an empty match
                            search_start += 1;
                            continue;
                        }

                        // Append the portion of the text from `search_start` to `match_start`
                        ret.push_str(&text[search_start..search_start + match_start].to_string());
                        println!("current ret: {}", ret);

                        // Start to replacing
                        let mut expanded = String::new();
                        capture.expand(&replaced, &mut expanded);

                        // Update the return string
                        ret.push_str(&expanded);

                        // Update the `search_start`
                        search_start += match_end;
                    }

                    Some(ret)
                }
            } else {
                // Only replace the first matched pattern
                // Construct the return string
                let mut ret = if start > 1 {
                    text[..start as usize].to_string()
                } else {
                    "".to_string()
                };

                if self.ctx.0.captures_len() <= 1 {
                    // There is no capture groups in the regex
                    println!("Path Three");
                    ret.push_str(&self.ctx.0.replacen(
                        &text[start as usize..],
                        1,
                        self.replacement.clone(),
                    ));
                } else {
                    // There are capture groups in the regex
                    println!("Path Four");
                    if let Some(capture) = self.ctx.0.captures(&text[start as usize..]) {
                        // Make the replacement to satisfy `expand`
                        let start = capture.get(0).unwrap().start();
                        let end = capture.get(0).unwrap().end();
                        let regex = Regex::new(r"\\([1-9])").unwrap();
                        let replaced = regex.replace_all(&self.replacement, "$${$1}").to_string();
                        let mut expanded = String::new();
                        capture.expand(&replaced, &mut expanded);
                        ret = format!("{}{}{}", &text[..start], expanded, &text[end..]);
                    } else {
                        // No match
                        ret = text.into();
                    }
                }
                
                Some(ret)
            }
        } else {
            // The input string is None
            println!("Path Four");
            None
        }
    }
}

#[async_trait::async_trait]
impl Expression for RegexpReplaceExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, _input: &DataChunk) -> Result<ArrayRef> {
        // let text_arr = self.source.eval_checked(input).await?;
        // let text_arr = text_arr.as_utf8();
        unimplemented!()
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        println!("Current Expr: {:?}", self);
        // Get the source text to match and replace
        let source = self.source.eval_row(input).await?;
        let source = match source {
            Some(ScalarImpl::Utf8(s)) => s,
            // The input source is invalid, directly return None
            _ => return Ok(None),
        };

        Ok(self
            .match_row(Some(&source))
            .map(|replaced| replaced.into()))
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn dummy_test() {
        println!("dummy test");
    }
}
