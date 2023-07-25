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

//! Generate code for the functions.

use itertools::Itertools;
use proc_macro2::Span;
use quote::{format_ident, quote};

use super::*;

impl FunctionAttr {
    /// Expands the wildcard in function arguments or return type.
    pub fn expand(&self) -> Vec<Self> {
        let args = self.args.iter().map(|ty| types::expand_type_wildcard(ty));
        let ret = types::expand_type_wildcard(&self.ret);
        // multi_cartesian_product should emit an empty set if the input is empty.
        let args_cartesian_product =
            args.multi_cartesian_product()
                .chain(match self.args.is_empty() {
                    true => vec![vec![]],
                    false => vec![],
                });
        let mut attrs = Vec::new();
        for (args, mut ret) in args_cartesian_product.cartesian_product(ret) {
            if ret == "auto" {
                ret = types::min_compatible_type(&args);
            }
            let attr = FunctionAttr {
                args: args.iter().map(|s| s.to_string()).collect(),
                ret: ret.to_string(),
                ..self.clone()
            };
            attrs.push(attr);
        }
        attrs
    }

    /// Generate a descriptor of the function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_descriptor(&self, build_fn: bool) -> Result<TokenStream2> {
        if self.is_table_function {
            return self.generate_table_function_descriptor(build_fn);
        }
        let name = self.name.clone();
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { crate::sig::func::FuncSign };
        let build_fn = if build_fn {
            let name = format_ident!("{}", self.user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_fn()?
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { crate::sig::func::_register(#descriptor_type {
                    func: risingwave_pb::expr::expr_node::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                }) };
            }
        })
    }

    fn generate_build_fn(&self) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let fn_name = format_ident!("{}", self.user_fn.name);
        let struct_name = format_ident!("{}", self.ident_name());
        let arg_ids = (0..num_args)
            .filter(|i| match &self.prebuild {
                Some(s) => !s.contains(&format!("${i}")),
                None => true,
            })
            .collect_vec();
        let const_ids = (0..num_args).filter(|i| match &self.prebuild {
            Some(s) => s.contains(&format!("${i}")),
            None => false,
        });
        let inputs: Vec<_> = arg_ids.iter().map(|i| format_ident!("i{i}")).collect();
        let all_child: Vec<_> = (0..num_args).map(|i| format_ident!("child{i}")).collect();
        let const_child: Vec<_> = const_ids.map(|i| format_ident!("child{i}")).collect();
        let child: Vec<_> = arg_ids.iter().map(|i| format_ident!("child{i}")).collect();
        let array_refs: Vec<_> = arg_ids.iter().map(|i| format_ident!("array{i}")).collect();
        let arrays: Vec<_> = arg_ids.iter().map(|i| format_ident!("a{i}")).collect();
        let datums: Vec<_> = arg_ids.iter().map(|i| format_ident!("v{i}")).collect();
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", types::array_type(t)));
        let arg_types = self
            .args
            .iter()
            .map(|t| types::ref_type(t).parse::<TokenStream2>().unwrap());
        let annotation: TokenStream2 = match self.user_fn.core_return_type.as_str() {
            // add type annotation for functions that return generic types
            "T" | "T1" | "T2" | "T3" => format!(": Option<{}>", types::owned_type(&self.ret))
                .parse()
                .unwrap(),
            _ => quote! {},
        };
        let ret_array_type = format_ident!("{}", types::array_type(&self.ret));
        let builder_type = format_ident!("{}Builder", types::array_type(&self.ret));
        let const_arg_type = match &self.prebuild {
            Some(s) => s.split("::").next().unwrap().parse().unwrap(),
            None => quote! { () },
        };
        let const_arg_value = match &self.prebuild {
            Some(s) => s
                .replace('$', "child")
                .parse()
                .expect("invalid prebuild syntax"),
            None => quote! { () },
        };
        let generic = if self.ret == "boolean" && self.user_fn.generic == 3 {
            // XXX: for generic compare functions, we need to specify the compatible type
            let compatible_type = types::ref_type(types::min_compatible_type(&self.args))
                .parse::<TokenStream2>()
                .unwrap();
            quote! { ::<_, _, #compatible_type> }
        } else {
            quote! {}
        };
        let const_arg = match &self.prebuild {
            Some(_) => quote! { &self.const_arg, },
            None => quote! {},
        };
        let context = match self.user_fn.context {
            true => quote! { &self.context, },
            false => quote! {},
        };
        let writer = match self.user_fn.writer {
            true => quote! { &mut writer, },
            false => quote! {},
        };
        // inputs: [ Option<impl ScalarRef> ]
        let mut output = quote! { #fn_name #generic(#(#inputs,)* #const_arg #context #writer) };
        output = match self.user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { Some(#output) },
            ReturnTypeKind::Option => output,
            ReturnTypeKind::Result => quote! { Some(#output?) },
            ReturnTypeKind::ResultOption => quote! { #output? },
        };
        if !self.user_fn.arg_option {
            output = quote! {
                match (#(#inputs,)*) {
                    (#(Some(#inputs),)*) => #output,
                    _ => None,
                }
            };
        };
        // output: Option<impl ScalarRef or Scalar>
        let append_output = match self.user_fn.writer {
            true => quote! {{
                let mut writer = builder.writer().begin();
                if #output.is_some() {
                    writer.finish();
                } else {
                    drop(writer);
                    builder.append_null();
                }
            }},
            false if self.user_fn.core_return_type == "impl AsRef < [u8] >" => quote! {
                builder.append(#output.as_ref().map(|s| s.as_ref()));
            },
            false => quote! {
                let output #annotation = #output;
                builder.append(output.as_ref().map(|s| s.as_scalar_ref()));
            },
        };
        let row_output = match self.user_fn.writer {
            true => quote! {{
                let mut writer = String::new();
                #output.map(|_| writer.into())
            }},
            false if self.user_fn.core_return_type == "impl AsRef < [u8] >" => quote! {
                #output.map(|s| s.as_ref().into())
            },
            false => quote! {{
                let output #annotation = #output;
                output.map(|s| s.into())
            }},
        };
        let eval = if let Some(batch_fn) = &self.batch_fn {
            // user defined batch function
            let fn_name = format_ident!("{}", batch_fn);
            quote! {
                let c = #fn_name(#(#arrays),*);
                Ok(Arc::new(c.into()))
            }
        } else if (types::is_primitive(&self.ret) || self.ret == "boolean")
            && self.user_fn.is_pure()
        {
            // SIMD optimization for primitive types
            match self.args.len() {
                0 => quote! {
                    let c = #ret_array_type::from_iter_bitmap(
                        std::iter::repeat_with(|| #fn_name()).take(input.capacity())
                        Bitmap::ones(input.capacity()),
                    );
                    Ok(Arc::new(c.into()))
                },
                1 => quote! {
                    let c = #ret_array_type::from_iter_bitmap(
                        a0.raw_iter().map(|a| #fn_name(a)),
                        a0.null_bitmap().clone()
                    );
                    Ok(Arc::new(c.into()))
                },
                2 => quote! {
                    // allow using `zip` for performance
                    #[allow(clippy::disallowed_methods)]
                    let c = #ret_array_type::from_iter_bitmap(
                        a0.raw_iter()
                            .zip(a1.raw_iter())
                            .map(|(a, b)| #fn_name #generic(a, b)),
                        a0.null_bitmap() & a1.null_bitmap(),
                    );
                    Ok(Arc::new(c.into()))
                },
                n => todo!("SIMD optimization for {n} arguments"),
            }
        } else {
            // no optimization
            quote! {
                let mut builder = #builder_type::with_type(input.capacity(), self.context.return_type.clone());

                match input.vis() {
                    Vis::Bitmap(vis) => {
                        for ((#(#inputs,)*), visible) in multizip((#(#arrays.iter(),)*)).zip_eq_fast(vis.iter()) {
                            if !visible {
                                builder.append_null();
                                continue;
                            }
                            #append_output
                        }
                    }
                    Vis::Compact(_) => {
                        for (#(#inputs,)*) in multizip((#(#arrays.iter(),)*)) {
                            #append_output
                        }
                    }
                }
                Ok(Arc::new(builder.finish().into()))
            }
        };

        Ok(quote! {
            |return_type, children| {
                use std::sync::Arc;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::row::OwnedRow;
                use risingwave_common::util::iter_util::ZipEqFast;
                use itertools::multizip;

                use crate::expr::{Context, BoxedExpression};
                use crate::Result;

                crate::ensure!(children.len() == #num_args);
                let context = Context {
                    return_type,
                    arg_types: children.iter().map(|c| c.return_type()).collect(),
                };
                let mut iter = children.into_iter();
                #(let #all_child = iter.next().unwrap();)*
                // evaluate const arguments
                #(let #const_child = #const_child.eval_const()?;)*

                #[derive(Debug)]
                struct #struct_name {
                    context: Context,
                    #(#child: BoxedExpression,)*
                    const_arg: #const_arg_type,
                }
                #[async_trait::async_trait]
                impl crate::expr::Expression for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.context.return_type.clone()
                    }
                    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
                        // evaluate children and downcast arrays
                        #(
                            let #array_refs = self.#child.eval_checked(input).await?;
                            let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*
                        #eval
                    }
                    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
                        #(
                            let #datums = self.#child.eval_row(input).await?;
                            let #inputs: Option<#arg_types> = #datums.as_ref().map(|s| s.as_scalar_ref_impl().try_into().unwrap());
                        )*
                        Ok(#row_output)
                    }
                }

                Ok(Box::new(#struct_name {
                    context,
                    #(#child,)*
                    const_arg: #const_arg_value,
                }))
            }
        })
    }

    /// Generate a descriptor of the aggregate function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_agg_descriptor(&self, build_fn: bool) -> Result<TokenStream2> {
        let name = self.name.clone();

        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { crate::sig::agg::AggFuncSig };
        let build_fn = if build_fn {
            let name = format_ident!("{}", self.user_fn.name);
            quote! { #name }
        } else {
            self.generate_agg_build_fn()?
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { crate::sig::agg::_register(#descriptor_type {
                    func: crate::agg::AggKind::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                }) };
            }
        })
    }

    /// Generate build function for aggregate function.
    fn generate_agg_build_fn(&self) -> Result<TokenStream2> {
        let ret_variant: TokenStream2 = types::variant(&self.ret).parse().unwrap();
        let ret_owned: TokenStream2 = types::owned_type(&self.ret).parse().unwrap();
        let state_type: TokenStream2 = match &self.state {
            Some(state) => state.parse().unwrap(),
            None => types::owned_type(&self.ret).parse().unwrap(),
        };
        let args = (0..self.args.len()).map(|i| format_ident!("v{i}"));
        let args = quote! { #(#args),* };
        let let_arrays = self.args.iter().enumerate().map(|(i, arg)| {
            let array = format_ident!("a{i}");
            let variant: TokenStream2 = types::variant(arg).parse().unwrap();
            quote! {
                let ArrayImpl::#variant(#array) = &**input.column_at(#i) else {
                    bail!("input type mismatch. expect: {}", stringify!(#variant));
                };
            }
        });
        let let_values = (0..self.args.len()).map(|i| {
            let v = format_ident!("v{i}");
            let a = format_ident!("a{i}");
            quote! { let #v = #a.value_at(row_id); }
        });
        let let_state = match &self.state {
            Some(_) => quote! { self.state.take() },
            None => quote! { self.state.as_ref().map(|x| x.as_scalar_ref()) },
        };
        let assign_state = match &self.state {
            Some(_) => quote! { state },
            None => quote! { state.map(|x| x.to_owned_scalar()) },
        };
        let init_state = match &self.init_state {
            Some(s) => s.parse().unwrap(),
            _ => quote! { None },
        };
        let fn_name = format_ident!("{}", self.user_fn.name);
        let mut next_state = quote! { #fn_name(state, #args) };
        next_state = match self.user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { Some(#next_state) },
            ReturnTypeKind::Option => next_state,
            ReturnTypeKind::Result => quote! { Some(#next_state?) },
            ReturnTypeKind::ResultOption => quote! { #next_state? },
        };
        if !self.user_fn.arg_option {
            if self.args.len() > 1 {
                todo!("multiple arguments are not supported for non-option function");
            }
            let first_state = match &self.init_state {
                Some(_) => quote! { unreachable!() },
                _ => quote! { Some(v0.into()) },
            };
            next_state = quote! {
                match (state, v0) {
                    (Some(state), Some(v0)) => #next_state,
                    (None, Some(v0)) => #first_state,
                    (state, None) => state,
                }
            };
        }

        Ok(quote! {
            |agg| {
                use std::collections::HashSet;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bail;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::estimate_size::EstimateSize;

                use crate::Result;

                #[derive(Clone, EstimateSize)]
                struct Agg {
                    return_type: DataType,
                    state: Option<#state_type>,
                }

                #[async_trait::async_trait]
                impl crate::agg::Aggregator for Agg {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }
                    async fn update_multi(
                        &mut self,
                        input: &DataChunk,
                        start_row_id: usize,
                        end_row_id: usize,
                    ) -> Result<()> {
                        #(#let_arrays)*
                        let mut state = #let_state;
                        for row_id in start_row_id..end_row_id {
                            if !input.vis().is_set(row_id) {
                                continue;
                            }
                            #(#let_values)*
                            state = #next_state;
                        }
                        self.state = #assign_state;
                        Ok(())
                    }
                    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                        let ArrayBuilderImpl::#ret_variant(builder) = builder else {
                            bail!("output type mismatch. expect: {}", stringify!(#ret_variant));
                        };
                        #[allow(clippy::mem_replace_option_with_none)]
                        match std::mem::replace(&mut self.state, #init_state) {
                            Some(state) => builder.append(Some(<#ret_owned>::from(state).as_scalar_ref())),
                            None => builder.append_null(),
                        }
                        Ok(())
                    }
                    fn estimated_size(&self) -> usize {
                        EstimateSize::estimated_size(self)
                    }
                }

                Ok(Box::new(Agg {
                    return_type: agg.return_type,
                    state: #init_state,
                }))
            }
        })
    }

    /// Generate a descriptor of the table function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    fn generate_table_function_descriptor(&self, build_fn: bool) -> Result<TokenStream2> {
        let name = self.name.clone();
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { crate::sig::table_function::FuncSign };
        let build_fn = if build_fn {
            let name = format_ident!("{}", self.user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_table_function()?
        };
        let type_infer_fn = if let Some(func) = &self.type_infer {
            func.parse().unwrap()
        } else {
            if matches!(self.ret.as_str(), "any" | "list" | "struct") {
                return Err(Error::new(
                    Span::call_site(),
                    format!("type inference function is required for {}", self.ret),
                ));
            }
            let ty = data_type(&self.ret);
            quote! { |_| Ok(#ty) }
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { crate::sig::table_function::_register(#descriptor_type {
                    func: risingwave_pb::expr::table_function::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                    type_infer: #type_infer_fn,
                }) };
            }
        })
    }

    fn generate_build_table_function(&self) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let return_types = output_types(&self.ret);
        let fn_name = format_ident!("{}", self.user_fn.name);
        let struct_name = format_ident!("{}", self.ident_name());
        let arg_ids = (0..num_args)
            .filter(|i| match &self.prebuild {
                Some(s) => !s.contains(&format!("${i}")),
                None => true,
            })
            .collect_vec();
        let const_ids = (0..num_args).filter(|i| match &self.prebuild {
            Some(s) => s.contains(&format!("${i}")),
            None => false,
        });
        let inputs: Vec<_> = arg_ids.iter().map(|i| format_ident!("i{i}")).collect();
        let all_child: Vec<_> = (0..num_args).map(|i| format_ident!("child{i}")).collect();
        let const_child: Vec<_> = const_ids.map(|i| format_ident!("child{i}")).collect();
        let child: Vec<_> = arg_ids.iter().map(|i| format_ident!("child{i}")).collect();
        let array_refs: Vec<_> = arg_ids.iter().map(|i| format_ident!("array{i}")).collect();
        let arrays: Vec<_> = arg_ids.iter().map(|i| format_ident!("a{i}")).collect();
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", types::array_type(t)));
        let outputs = (0..return_types.len())
            .map(|i| format_ident!("o{i}"))
            .collect_vec();
        let builders = (0..return_types.len())
            .map(|i| format_ident!("builder{i}"))
            .collect_vec();
        let builder_types = return_types
            .iter()
            .map(|ty| format_ident!("{}Builder", types::array_type(ty)))
            .collect_vec();
        let return_types = if return_types.len() == 1 {
            vec![quote! { self.return_type.clone() }]
        } else {
            (0..return_types.len())
                .map(|i| quote! { self.return_type.as_struct().unwrap().types().nth(#i).unwrap().clone() })
                .collect()
        };
        let build_value_array = if return_types.len() == 1 {
            quote! { let [value_array] = value_arrays; }
        } else {
            quote! {
                let bitmap = value_arrays[0].null_bitmap().clone();
                let value_array = StructArray::new(
                    self.return_type.as_struct().unwrap().clone(),
                    value_arrays.to_vec(),
                    bitmap,
                ).into_ref();
            }
        };
        let const_arg = match &self.prebuild {
            Some(_) => quote! { &self.const_arg },
            None => quote! {},
        };
        let const_arg_type = match &self.prebuild {
            Some(s) => s.split("::").next().unwrap().parse().unwrap(),
            None => quote! { () },
        };
        let const_arg_value = match &self.prebuild {
            Some(s) => s
                .replace('$', "child")
                .parse()
                .expect("invalid prebuild syntax"),
            None => quote! { () },
        };
        let iter = match self.user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { iter },
            ReturnTypeKind::Option => quote! { iter.flatten() },
            ReturnTypeKind::Result => quote! { iter? },
            ReturnTypeKind::ResultOption => quote! { value?.flatten() },
        };
        let iterator_item_type = self.user_fn.iterator_item_kind.clone().ok_or_else(|| {
            Error::new(
                self.user_fn.return_type_span,
                "expect `impl Iterator` in return type",
            )
        })?;
        let output = match iterator_item_type {
            ReturnTypeKind::T => quote! { Some(output) },
            ReturnTypeKind::Option => quote! { output },
            ReturnTypeKind::Result => quote! { Some(output?) },
            ReturnTypeKind::ResultOption => quote! { output? },
        };

        Ok(quote! {
            |return_type, chunk_size, children| {
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::util::iter_util::ZipEqFast;
                use itertools::multizip;

                crate::ensure!(children.len() == #num_args);
                let mut iter = children.into_iter();
                #(let #all_child = iter.next().unwrap();)*
                #(let #const_child = #const_child.eval_const()?;)*

                #[derive(Debug)]
                struct #struct_name {
                    return_type: DataType,
                    chunk_size: usize,
                    #(#child: BoxedExpression,)*
                    const_arg: #const_arg_type,
                }
                #[async_trait::async_trait]
                impl crate::table_function::TableFunction for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }
                    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
                        self.eval_inner(input)
                    }
                }
                impl #struct_name {
                    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
                    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
                        #(
                        let #array_refs = self.#child.eval_checked(input).await?;
                        let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*

                        let mut index_builder = I32ArrayBuilder::new(self.chunk_size);
                        #(let mut #builders = #builder_types::with_type(self.chunk_size, #return_types);)*

                        for (i, (row, visible)) in multizip((#(#arrays.iter(),)*)).zip_eq_fast(input.vis().iter()).enumerate() {
                            if let (#(Some(#inputs),)*) = row && visible {
                                let iter = #fn_name(#(#inputs,)* #const_arg);
                                for output in #iter {
                                    index_builder.append(Some(i as i32));
                                    match #output {
                                        Some((#(#outputs),*)) => { #(#builders.append(Some(#outputs.as_scalar_ref()));)* }
                                        None => { #(#builders.append_null();)* }
                                    }

                                    if index_builder.len() == self.chunk_size {
                                        let index_array = std::mem::replace(&mut index_builder, I32ArrayBuilder::new(self.chunk_size)).finish().into_ref();
                                        let value_arrays = [#(std::mem::replace(&mut #builders, #builder_types::with_type(self.chunk_size, #return_types)).finish().into_ref()),*];
                                        #build_value_array
                                        yield DataChunk::new(vec![index_array, value_array], self.chunk_size);
                                    }
                                }
                            }
                        }

                        if index_builder.len() > 0 {
                            let len = index_builder.len();
                            let index_array = index_builder.finish().into_ref();
                            let value_arrays = [#(#builders.finish().into_ref()),*];
                            #build_value_array
                            yield DataChunk::new(vec![index_array, value_array], len);
                        }
                    }
                }

                Ok(Box::new(#struct_name {
                    return_type,
                    chunk_size,
                    #(#child,)*
                    const_arg: #const_arg_value,
                }))
            }
        })
    }
}

fn data_type_name(ty: &str) -> TokenStream2 {
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { DataTypeName::#variant }
}

fn data_type(ty: &str) -> TokenStream2 {
    if let Some(ty) = ty.strip_suffix("[]") {
        let inner_type = data_type(ty);
        return quote! { DataType::List(Box::new(#inner_type)) };
    }
    if ty.starts_with("struct<") {
        return quote! { DataType::Struct(#ty.parse().expect("invalid struct type")) };
    }
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { DataType::#variant }
}

/// Extract multiple output types.
///
/// ```ignore
/// output_types("int32") -> ["int32"]
/// output_types("struct<key varchar, value jsonb>") -> ["varchar", "jsonb"]
/// ```
fn output_types(ty: &str) -> Vec<&str> {
    if let Some(s) = ty.strip_prefix("struct<") && let Some(args) = s.strip_suffix('>') {
        args.split(',').map(|s| s.split_whitespace().nth(1).unwrap()).collect()
    } else {
        vec![ty]
    }
}
