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

use core::option::Option::Some;
use std::ffi::c_void;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;

use jni::objects::JValueOwned;
use jni::strings::JNIString;
use jni::{InitArgsBuilder, JNIVersion, JavaVM, NativeMethod};
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;

/// Use 10% of compute total memory by default. Compute node uses 0.7 * system memory by default.
const DEFAULT_MEMORY_PROPORTION: f64 = 0.07;

pub static JVM: JavaVmWrapper = JavaVmWrapper::new();

pub struct JavaVmWrapper(OnceLock<Result<JavaVM, RwError>>);

impl JavaVmWrapper {
    const fn new() -> Self {
        Self(OnceLock::new())
    }

    pub fn get(&self) -> Option<&Result<JavaVM, RwError>> {
        self.0.get()
    }

    pub fn get_or_init(&self) -> Result<&JavaVM, &RwError> {
        self.0.get_or_init(Self::inner_new).as_ref()
    }

    fn inner_new() -> Result<JavaVM, RwError> {
        let libs_path = if let Ok(libs_path) = std::env::var("CONNECTOR_LIBS_PATH") {
            libs_path
        } else {
            return Err(ErrorCode::InternalError(
                "environment variable CONNECTOR_LIBS_PATH is not specified".to_string(),
            )
            .into());
        };

        let dir = Path::new(&libs_path);

        if !dir.is_dir() {
            return Err(ErrorCode::InternalError(format!(
                "CONNECTOR_LIBS_PATH \"{}\" is not a directory",
                libs_path
            ))
            .into());
        }

        let mut class_vec = vec![];

        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.file_name().is_some() {
                    let path = std::fs::canonicalize(entry_path)?;
                    class_vec.push(path.to_str().unwrap().to_string());
                }
            }
        } else {
            return Err(ErrorCode::InternalError(format!(
                "failed to read CONNECTOR_LIBS_PATH \"{}\"",
                libs_path
            ))
            .into());
        }

        let jvm_heap_size = if let Ok(heap_size) = std::env::var("JVM_HEAP_SIZE") {
            heap_size
        } else {
            format!(
                "{}",
                (system_memory_available_bytes() as f64 * DEFAULT_MEMORY_PROPORTION) as usize
            )
        };

        // Build the VM properties
        let args_builder = InitArgsBuilder::new()
            // Pass the JNI API version (default is 8)
            .version(JNIVersion::V8)
            .option("-Dis_embedded_connector=true")
            .option(format!("-Djava.class.path={}", class_vec.join(":")))
            .option("-Xms16m")
            .option(format!("-Xmx{}", jvm_heap_size));

        tracing::info!("JVM args: {:?}", args_builder);
        let jvm_args = args_builder.build().unwrap();

        // Create a new VM
        let jvm = match JavaVM::new(jvm_args) {
            Err(err) => {
                tracing::error!("fail to new JVM {:?}", err);
                return Err(ErrorCode::InternalError("fail to new JVM".to_string()).into());
            }
            Ok(jvm) => jvm,
        };

        tracing::info!("initialize JVM successfully");

        register_native_method_for_jvm(&jvm).unwrap();

        Ok(jvm)
    }
}

pub fn register_native_method_for_jvm(jvm: &JavaVM) -> Result<(), jni::errors::Error> {
    let mut env = jvm
        .attach_current_thread()
        .inspect_err(|e| tracing::error!("jvm attach thread error: {:?}", e))
        .unwrap();

    let binding_class = env
        .find_class("com/risingwave/java/binding/Binding")
        .inspect_err(|e| tracing::error!("jvm find class error: {:?}", e))
        .unwrap();
    use crate::*;
    macro_rules! gen_native_method_array {
        () => {{
            $crate::for_all_native_methods! {gen_native_method_array}
        }};
        ({$({ $func_name:ident, {$($ret:tt)+}, {$($args:tt)*} }),*}) => {
            [
                $(
                    {
                        let fn_ptr = paste::paste! {[<Java_com_risingwave_java_binding_Binding_ $func_name> ]} as *mut c_void;
                        let sig = $crate::gen_jni_sig! { $($ret)+ ($($args)*)};
                        NativeMethod {
                            name: JNIString::from(stringify! {$func_name}),
                            sig: JNIString::from(sig),
                            fn_ptr,
                        }
                    },
                )*
            ]
        }
    }
    env.register_native_methods(binding_class, &gen_native_method_array!())
        .inspect_err(|e| tracing::error!("jvm register native methods error: {:?}", e))?;

    tracing::info!("register native methods for jvm successfully");
    Ok(())
}

/// Load JVM memory statistics from the runtime. If JVM is not initialized or fail to initialize, return zero.
pub fn load_jvm_memory_stats() -> (usize, usize) {
    if let Some(jvm) = JVM.get() {
        match jvm {
            Ok(jvm) => {
                let mut env = jvm.attach_current_thread().unwrap();
                let runtime_instance = env
                    .call_static_method(
                        "java/lang/Runtime",
                        "getRuntime",
                        "()Ljava/lang/Runtime;",
                        &[],
                    )
                    .unwrap();

                let runtime_instance = match runtime_instance {
                    JValueOwned::Object(o) => o,
                    _ => unreachable!(),
                };

                let total_memory = env
                    .call_method(runtime_instance.as_ref(), "totalMemory", "()J", &[])
                    .unwrap()
                    .j()
                    .unwrap();

                let free_memory = env
                    .call_method(runtime_instance, "freeMemory", "()J", &[])
                    .unwrap()
                    .j()
                    .unwrap();

                (total_memory as usize, (total_memory - free_memory) as usize)
            }
            Err(_) => (0, 0),
        }
    } else {
        (0, 0)
    }
}
