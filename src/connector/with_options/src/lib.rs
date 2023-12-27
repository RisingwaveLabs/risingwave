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

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Annotates that the struct represents the WITH properties for a connector.
#[proc_macro_derive(WithOptions, attributes(with_option))]
pub fn derive_helper_attr(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let fields = match input.data {
        syn::Data::Struct(ref data) => match data.fields {
            syn::Fields::Named(ref fields) => &fields.named,
            _ => return quote! { compile_error!("WithOptions can only be derived for structs with named fields"); }.into(),
        },
        _ => return quote! { compile_error!("WithOptions can only be derived for structs"); }.into(),
    };

    let mut assert_impls = vec![];

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();

        assert_impls.push(quote!(
            crate::with_options::WithOptions::assert_receiver_is_with_options(&self.#field_name);
        ))
    }

    let struct_name = input.ident;
    // This macro is only be expected to used in risingwave_connector. This trait is also defined there.
    if input.generics.params.is_empty() {
        quote! {
            impl crate::with_options::WithOptions for #struct_name {
                fn assert_receiver_is_with_options(&self) {
                    #(#assert_impls)*
                }
            }
        }
        .into()
    } else {
        // Note: CDC properties have generics.
        let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
        quote! {
            impl #impl_generics crate::with_options::WithOptions for #struct_name #ty_generics #where_clause {
                fn assert_receiver_is_with_options(&self) {
                    #(#assert_impls)*
                }
            }
        }.into()
    }
}
