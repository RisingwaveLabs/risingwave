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

// std try_collect is slower than itertools
// #![feature(iterator_try_collect)]

// allow using `zip`.
// `zip_eq` is a source of poor performance.
#![allow(clippy::disallowed_methods)]

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::array::*;
use risingwave_common::types::{
    DataType, Decimal, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, OrderedF32,
    OrderedF64,
};
use risingwave_expr::expr::expr_binary_nonnull::*;
use risingwave_expr::expr::expr_binary_nullable::*;
use risingwave_expr::expr::expr_unary::new_unary_expr;
use risingwave_expr::expr::*;
use risingwave_expr::vector_op::agg::create_agg_state_unary;
use risingwave_expr::ExprError;
use risingwave_pb::expr::expr_node::Type;

criterion_group!(benches, bench_expr, bench_raw);
criterion_main!(benches);

const CHUNK_SIZE: usize = 1024;

fn bench_expr(c: &mut Criterion) {
    let input = DataChunk::new(
        vec![
            BoolArray::from_iter((1..=CHUNK_SIZE).map(|i| i % 2 == 0)).into(),
            I16Array::from_iter((1..=CHUNK_SIZE).map(|i| (i as i16 % 128).max(1))).into(),
            I32Array::from_iter(1..=CHUNK_SIZE as i32).into(),
            I64Array::from_iter(1..=CHUNK_SIZE as i64).into(),
            F32Array::from_iter((1..=CHUNK_SIZE).map(|i| OrderedF32::from(i as f32))).into(),
            F64Array::from_iter((1..=CHUNK_SIZE).map(|i| OrderedF64::from(i as f64))).into(),
            DecimalArray::from_iter((1..=CHUNK_SIZE).map(|i| Decimal::from(i))).into(),
            NaiveDateArray::from_iter((1..=CHUNK_SIZE).map(|_| NaiveDateWrapper::default())).into(),
            NaiveTimeArray::from_iter((1..=CHUNK_SIZE).map(|_| NaiveTimeWrapper::default())).into(),
            NaiveDateTimeArray::from_iter(
                (1..=CHUNK_SIZE).map(|_| NaiveDateTimeWrapper::default()),
            )
            .into(),
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(Some)).into(),
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(Some))
                .into_bytes_array()
                .into(),
        ],
        CHUNK_SIZE,
    );
    let inputrefs = [
        ("bool", InputRefExpression::new(DataType::Boolean, 0)),
        ("i16", InputRefExpression::new(DataType::Int16, 1)),
        ("i32", InputRefExpression::new(DataType::Int32, 2)),
        ("i64", InputRefExpression::new(DataType::Int64, 3)),
        ("f32", InputRefExpression::new(DataType::Float32, 4)),
        ("f64", InputRefExpression::new(DataType::Float64, 5)),
        ("decimal", InputRefExpression::new(DataType::Decimal, 6)),
        ("date", InputRefExpression::new(DataType::Date, 7)),
        ("time", InputRefExpression::new(DataType::Time, 8)),
        ("timestamp", InputRefExpression::new(DataType::Timestamp, 9)),
        ("string", InputRefExpression::new(DataType::Varchar, 10)),
        ("bytea", InputRefExpression::new(DataType::Bytea, 11)),
    ];

    c.bench_function("expr/inputref", |bencher| {
        let inputref = inputrefs[0].1.clone().boxed();
        bencher.iter(|| inputref.eval(&input).unwrap())
    });
    c.bench_function("expr/constant", |bencher| {
        let constant = LiteralExpression::new(DataType::Int32, Some(1_i32.into()));
        bencher.iter(|| constant.eval(&input).unwrap())
    });

    let ops = [
        ("add", Type::Add),
        ("sub", Type::Subtract),
        ("mul", Type::Multiply),
        ("div", Type::Divide),
        ("mod", Type::Modulus),
    ];
    for (op, op_type) in &ops {
        // only for numberic types
        for (ty, expr) in &inputrefs[1..=6] {
            c.bench_function(&format!("expr/{op}/{ty}"), |bencher| {
                let l = expr.clone().boxed();
                let r = expr.clone().boxed();
                let add = new_binary_expr(*op_type, expr.return_type(), l, r).unwrap();
                bencher.iter(|| add.eval(&input).unwrap())
            });
        }
    }

    let ops = [("eq", Type::Equal), ("gt", Type::GreaterThan)];
    for (op, op_type) in &ops {
        for (ty, expr) in &inputrefs {
            c.bench_function(&format!("expr/{op}/{ty}"), |bencher| {
                let l = expr.clone().boxed();
                let r = expr.clone().boxed();
                let add = new_binary_expr(*op_type, expr.return_type(), l, r).unwrap();
                bencher.iter(|| add.eval(&input).unwrap())
            });
        }
    }

    c.bench_function("expr/and/bool", |bencher| {
        let l = inputrefs[0].1.clone().boxed();
        let r = inputrefs[0].1.clone().boxed();
        let add = new_nullable_binary_expr(Type::And, DataType::Boolean, l, r).unwrap();
        bencher.iter(|| add.eval(&input).unwrap())
    });

    c.bench_function("expr/not/bool", |bencher| {
        let l = inputrefs[0].1.clone().boxed();
        let add = new_unary_expr(Type::Not, DataType::Boolean, l).unwrap();
        bencher.iter(|| add.eval(&input).unwrap())
    });

    c.bench_function("expr/sum/i32", |bencher| {
        let mut sum =
            create_agg_state_unary(DataType::Int32, 0, AggKind::Sum, DataType::Int64, false)
                .unwrap();
        bencher.iter(|| sum.update_multi(&input, 0, 1024).unwrap())
    });

    // ~360ns
    // This should be the optimization goal for our add expression.
    c.bench_function("expr/add/i32/TBD", |bencher| {
        bencher.iter(|| {
            let a = input.column_at(0).array_ref().as_int32();
            let b = input.column_at(1).array_ref().as_int32();
            assert_eq!(a.len(), b.len());
            let mut c = (a.raw_iter())
                .zip(b.raw_iter())
                .map(|(a, b)| a + b)
                .collect::<I32Array>();
            let mut overflow = false;
            for ((a, b), c) in a.raw_iter().zip(b.raw_iter()).zip(c.raw_iter()) {
                overflow |= (c ^ a) & (c ^ b) < 0;
            }
            if overflow {
                return Err(ExprError::NumericOutOfRange);
            }
            c.set_bitmap(a.null_bitmap() & b.null_bitmap());
            Ok(c)
        })
    });
}

/// Evaluate on raw Rust array.
///
/// This could be used as a baseline to compare and tune our expressions.
fn bench_raw(c: &mut Criterion) {
    // ~55ns
    c.bench_function("raw/sum/i32", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| a.iter().sum::<i32>())
    });
    // ~90ns
    c.bench_function("raw/add/i32", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
    // ~600ns
    c.bench_function("raw/add/i32/zip_eq", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| {
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
    // ~950ns
    c.bench_function("raw/add/Option<i32>/zip_eq", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        bencher.iter(|| {
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
    });
    enum Error {
        Overflow,
        Cast,
    }
    // ~2100ns
    c.bench_function("raw/add/Option<i32>/zip_eq,checked", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        bencher.iter(|| {
            use itertools::Itertools;
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => a.checked_add(*b).ok_or(Error::Overflow).map(Some),
                    _ => Ok(None),
                })
                .try_collect::<_, Vec<_>, Error>()
        })
    });
    // ~2400ns
    c.bench_function("raw/add/Option<i32>/zip_eq,checked,cast", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        #[allow(clippy::useless_conversion)]
        fn checked_add(a: i32, b: i32) -> Result<i32, Error> {
            let a: i32 = a.try_into().map_err(|_| Error::Cast)?;
            let b: i32 = b.try_into().map_err(|_| Error::Cast)?;
            a.checked_add(b).ok_or(Error::Overflow)
        }
        bencher.iter(|| {
            use itertools::Itertools;
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => checked_add(*a, *b).map(Some),
                    _ => Ok(None),
                })
                .try_collect::<_, Vec<_>, Error>()
        })
    });
    // ~3100ns
    c.bench_function(
        "raw/add/Option<i32>/zip_eq,checked,cast,collect_array",
        |bencher| {
            let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
            let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
            #[allow(clippy::useless_conversion)]
            fn checked_add(a: i32, b: i32) -> Result<i32, Error> {
                let a: i32 = a.try_into().map_err(|_| Error::Cast)?;
                let b: i32 = b.try_into().map_err(|_| Error::Cast)?;
                a.checked_add(b).ok_or(Error::Overflow)
            }
            bencher.iter(|| {
                use itertools::Itertools;
                itertools::Itertools::zip_eq(a.iter(), b.iter())
                    .map(|(a, b)| match (a, b) {
                        (Some(a), Some(b)) => checked_add(*a, *b).map(Some),
                        _ => Ok(None),
                    })
                    .try_collect::<_, I32Array, Error>()
            })
        },
    );
}
