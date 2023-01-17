// Copyright 2023 Singularity Data
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

#![feature(error_generic_member_access)]
#![feature(provide_any)]

mod iterator;

use std::backtrace::Backtrace;
use std::marker::PhantomData;
use std::ops::Deref;
use std::panic::catch_unwind;

use iterator::{Iterator, KeyedRow};
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong};
use jni::JNIEnv;
use risingwave_storage::error::StorageError;
use thiserror::Error;

#[derive(Error, Debug)]
enum BindingError {
    #[error("JniError {error}")]
    JniError {
        #[from]
        error: jni::errors::Error,
        backtrace: Backtrace,
    },

    #[error("StorageError {error}")]
    StorageError {
        #[from]
        error: StorageError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, BindingError>;

#[repr(transparent)]
#[derive(Default)]
pub struct ByteArray<'a>(JObject<'a>);

impl<'a> From<jbyteArray> for ByteArray<'a> {
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from(inner: jbyteArray) -> Self {
        unsafe { Self(JObject::from_raw(inner)) }
    }
}

#[repr(transparent)]
pub struct Pointer<'a, T> {
    pointer: jlong,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T> Default for Pointer<'a, T> {
    fn default() -> Self {
        Self {
            pointer: 0,
            _phantom: Default::default(),
        }
    }
}

impl<T> From<T> for Pointer<'static, T> {
    fn from(value: T) -> Self {
        Pointer {
            pointer: Box::into_raw(Box::new(value)) as jlong,
            _phantom: PhantomData::default(),
        }
    }
}

impl<T> Pointer<'static, T> {
    fn null() -> Self {
        Pointer {
            pointer: 0,
            _phantom: PhantomData::default(),
        }
    }
}

impl<'a, T> Pointer<'a, T> {
    fn as_ref(&self) -> &'a T {
        debug_assert!(self.pointer != 0);
        unsafe { &*(self.pointer as *const T) }
    }

    fn as_mut(&mut self) -> &'a mut T {
        debug_assert!(self.pointer != 0);
        unsafe { &mut *(self.pointer as *mut T) }
    }

    fn drop(self) {
        debug_assert!(self.pointer != 0);
        unsafe { drop(Box::from_raw(self.pointer as *mut T)) }
    }
}

/// In most Jni interfaces, the first parameter is `JNIEnv`, and the second parameter is `JClass`.
/// This struct simply encapsulates the two common parameters into a single struct for simplicity.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EnvParam<'a> {
    env: JNIEnv<'a>,
    class: JClass<'a>,
}

impl<'a> Deref for EnvParam<'a> {
    type Target = JNIEnv<'a>;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

impl<'a> EnvParam<'a> {
    pub fn get_class(&self) -> JClass<'a> {
        self.class
    }
}

fn execute_and_catch<F, Ret>(env: EnvParam<'_>, inner: F) -> Ret
where
    F: FnOnce() -> Result<Ret>,
    Ret: Default,
{
    match catch_unwind(std::panic::AssertUnwindSafe(inner)) {
        Ok(Ok(ret)) => ret,
        Ok(Err(e)) => {
            match e {
                BindingError::JniError {
                    error: jni::errors::Error::JavaException,
                    backtrace,
                } => {
                    tracing::error!("get JavaException thrown from: {:?}", backtrace);
                    // the exception is already thrown. No need to throw again
                }
                _ => {
                    env.throw(format!("get error while processing: {:?}", e))
                        .expect("should be able to throw");
                }
            }
            Ret::default()
        }
        Err(e) => {
            env.throw(format!("panic while processing: {:?}", e))
                .expect("should be able to throw");
            Ret::default()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNew(
    env: EnvParam<'_>,
) -> Pointer<'static, Iterator> {
    execute_and_catch(env, move || Ok(Iterator::new()?.into()))
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNext<'a>(
    env: EnvParam<'a>,
    mut pointer: Pointer<'a, Iterator>,
) -> Pointer<'static, KeyedRow> {
    execute_and_catch(env, move || match pointer.as_mut().next()? {
        None => Ok(Pointer::null()),
        Some(row) => Ok(row.into()),
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorClose(
    _env: EnvParam<'_>,
    pointer: Pointer<'_, Iterator>,
) {
    pointer.drop();
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetKey<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, KeyedRow>,
) -> ByteArray<'a> {
    execute_and_catch(env, move || {
        Ok(ByteArray::from(
            env.byte_array_from_slice(pointer.as_ref().key())?,
        ))
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowIsNull<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, KeyedRow>,
    idx: jint,
) -> jboolean {
    execute_and_catch(
        env,
        move || Ok(pointer.as_ref().is_null(idx as usize) as u8),
    )
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetInt64Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, KeyedRow>,
    idx: jint,
) -> jlong {
    execute_and_catch(env, move || Ok(pointer.as_ref().get_int64(idx as usize)))
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetStringValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, KeyedRow>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move || {
        Ok(env.new_string(pointer.as_ref().get_utf8(idx as usize))?)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowClose<'a>(
    _env: EnvParam<'a>,
    pointer: Pointer<'a, KeyedRow>,
) {
    pointer.drop()
}
