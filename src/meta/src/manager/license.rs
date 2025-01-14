// Copyright 2024 RisingWave Labs
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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

use anyhow::Context;
use notify::Watcher;
use risingwave_common::system_param::LICENSE_KEY_KEY;
use thiserror_ext::AsReport;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::MetaSrvEnv;
use crate::MetaResult;

/// For test purposes, we count the number of times the license key file is reloaded.
static RELOAD_TIMES: AtomicUsize = AtomicUsize::new(0);

impl MetaSrvEnv {
    /// Spawn background tasks to watch the license key file and update the system parameter,
    /// if configured.
    pub fn may_start_watch_license_key_file(&self) -> MetaResult<Option<JoinHandle<()>>> {
        let Some(path) = self.opts.license_key_path.as_ref() else {
            return Ok(None);
        };

        let (changed_tx, mut changed_rx) = watch::channel(());
        // Send an initial event to trigger the initial load.
        changed_tx.send(()).unwrap();

        let mut watcher =
            notify::recommended_watcher(move |event: Result<notify::Event, notify::Error>| {
                match event {
                    Ok(event) => {
                        if event.kind.is_access() {
                            // Ignore access events as they do not indicate changes and will be
                            // triggered on our own read operations.
                        } else {
                            let _ = changed_tx.send(());
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            "error occurred while watching license key file"
                        );
                    }
                }
            })
            .context("failed to create license key file watcher")?;

        // This will spawn a new thread to watch the file, so no need to be concerned about blocking.
        watcher
            .watch(path, notify::RecursiveMode::NonRecursive)
            .context("failed to watch license key file")?;

        let updater = {
            let mgr = self.system_params_manager_impl_ref();
            let path = path.to_path_buf();

            async move {
                // Let the watcher live until the end of the updater to prevent dropping (then stopping).
                let _watcher = watcher;

                // Read the file content and set the system parameter every time the file changes.
                // Note that `changed()` will immediately resolves on the very first call, so we
                // will do the initialization then.
                while changed_rx.changed().await.is_ok() {
                    tracing::info!(path = %path.display(), "license key file changed, reloading...");
                    RELOAD_TIMES.fetch_add(1, Relaxed);

                    let content = match tokio::fs::read_to_string(&path).await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                path = %path.display(),
                                error = %e.as_report(),
                                "failed to read license key file"
                            );
                            continue;
                        }
                    };

                    // Trim the content and use it as the new license key value.
                    //
                    // It's always a `Some`, meaning that an empty license key file here is equivalent to
                    // `ALTER SYSTEM SET license_key TO ''`, instead of `... TO DEFAULT`. Please note
                    // the slight difference in behavior of debug build, where the default value of the
                    // `license_key` system parameter is a test key but not an empty string.
                    let value = Some(content.trim().to_owned());

                    let result = mgr.set_param(LICENSE_KEY_KEY, value).await;

                    if let Err(e) = result {
                        tracing::error!(
                            error = %e.as_report(),
                            "failed to set license key from file"
                        );
                    }
                }
            }
        };

        let handle = tokio::spawn(updater);
        Ok(Some(handle))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_license::{License, LicenseManager, Tier};

    use super::*;
    use crate::manager::MetaOpts;

    // License {
    //     sub: "rw-test",
    //     iss: Test,
    //     tier: Free,              <- difference from the default license in debug build
    //     cpu_core_limit: None,
    //     exp: 9999999999,
    // }
    const INITIAL_KEY: &str =
        "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
         eyJzdWIiOiJydy10ZXN0IiwidGllciI6ImZyZWUiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwiZXhwIjo5OTk5OTk5OTk5fQ.\
         ALC3Kc9LI6u0S-jeMB1YTxg1k8Azxwvc750ihuSZgjA_e1OJC9moxMvpLrHdLZDzCXHjBYi0XJ_1lowmuO_0iPEuPqN5AFpDV1ywmzJvGmMCMtw3A2wuN7hhem9OsWbwe6lzdwrefZLipyo4GZtIkg5ZdwGuHzm33zsM-X5gl_Ns4P6axHKiorNSR6nTAyA6B32YVET_FAM2YJQrXqpwA61wn1XLfarZqpdIQyJ5cgyiC33BFBlUL3lcRXLMLeYe6TjYGeV4K63qARCjM9yeOlsRbbW5ViWeGtR2Yf18pN8ysPXdbaXm_P_IVhl3jCTDJt9ctPh6pUCbkt36FZqO9A";

    #[cfg(not(madsim))] // `notify` will spawn system threads, which is not allowed in madsim
    #[tokio::test]
    #[cfg_attr(not(debug_assertions), ignore)] // skip in release build
    async fn test_watch_license_key_file() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();

        let key_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(key_file.path(), INITIAL_KEY).unwrap();

        let srv = MetaSrvEnv::for_test_opts(MetaOpts {
            license_key_path: Some(key_file.path().to_path_buf()),
            ..MetaOpts::test(false)
        })
        .await;
        let _updater_handle = srv.may_start_watch_license_key_file().unwrap().unwrap();

        // Since we've filled the key file with the initial key, the license should be loaded.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(RELOAD_TIMES.load(Relaxed), 1);
        let license = LicenseManager::get().license().unwrap();
        assert_eq!(license.sub, "rw-test");
        assert_eq!(license.tier, Tier::Free);

        // Update the key file with an empty content, which should reset the license to the default.
        std::fs::write(key_file.path(), "").unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(RELOAD_TIMES.load(Relaxed), 2);
        let license = LicenseManager::get().license().unwrap();
        assert_eq!(license, License::default());

        // Show that our "access" on the key file does not trigger a reload recursively.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(RELOAD_TIMES.load(Relaxed), 2);
    }
}
