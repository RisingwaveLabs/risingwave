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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

pub type SyncPoint = String;
pub type Signal = String;

#[derive(Clone)]
pub enum Action {
    WaitForSignal(WaitForSignal),
    EmitSignal(Signal),
}

#[derive(Clone)]
pub struct WaitForSignal {
    pub signal: Signal,
    pub clear_signal: bool,
    pub timeout: Duration,
}

lazy_static::lazy_static! {
    static ref SYNC_FACILITY: SyncFacility = {
        SyncFacility::new()
    };
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Wait for signal {0} timeout")]
    WaitForSignalTimeout(String),
}

#[derive(Clone)]
struct SyncPointInfo {
    actions: Vec<Action>,
    execute_times: u64,
}

struct SyncFacility {
    signals: parking_lot::Mutex<HashMap<Signal, Arc<tokio::sync::Notify>>>,
    sync_points: parking_lot::Mutex<HashMap<SyncPoint, SyncPointInfo>>,
}

impl SyncFacility {
    fn new() -> Self {
        Self {
            signals: Default::default(),
            sync_points: Default::default(),
        }
    }

    async fn wait_for_signal(&self, wait_for_signal: WaitForSignal) -> Result<(), Error> {
        let entry = self
            .signals
            .lock()
            .entry(wait_for_signal.signal.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone();
        match tokio::time::timeout(wait_for_signal.timeout, entry.notified()).await {
            Ok(_) => {
                if !wait_for_signal.clear_signal {
                    entry.notify_one();
                }
            }
            Err(_) => {
                return Err(Error::WaitForSignalTimeout(wait_for_signal.signal));
            }
        }
        Ok(())
    }

    fn emit_signal(&self, signal: Signal) {
        let entry = self
            .signals
            .lock()
            .entry(signal)
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone();
        entry.notify_one();
    }

    fn set_actions(&self, sync_point: &str, actions: Vec<Action>, execute_times: u64) {
        let mut guard = self.sync_points.lock();
        let sync_points = guard.deref_mut();
        sync_points.insert(
            sync_point.to_owned(),
            SyncPointInfo {
                actions,
                execute_times,
            },
        );
    }

    fn reset_actions(&self, sync_point: &str) {
        self.sync_points.lock().remove(sync_point);
    }

    async fn on_sync_point(&self, sync_point: &str) -> Result<(), Error> {
        let actions = {
            let mut guard = self.sync_points.lock();
            match guard.entry(sync_point.to_owned()) {
                Entry::Occupied(mut o) => {
                    if o.get().execute_times <= 1 {
                        // Deactivate the sync point
                        guard.remove(sync_point).unwrap().actions
                    } else {
                        o.get_mut().execute_times -= 1;
                        o.get().actions.clone()
                    }
                }
                Entry::Vacant(_) => {
                    return Ok(());
                }
            }
        };
        for action in actions {
            match action {
                Action::WaitForSignal(w) => {
                    self.wait_for_signal(w.to_owned()).await?;
                }
                Action::EmitSignal(s) => {
                    self.emit_signal(s.to_owned());
                }
            }
        }
        Ok(())
    }
}

/// The activation is reset after executed `execute_times`.
pub fn activate_sync_point(sync_point: &str, actions: Vec<Action>, execute_times: u64) {
    SYNC_FACILITY.set_actions(sync_point, actions, execute_times);
}

pub fn deactivate_sync_point(sync_point: &str) {
    SYNC_FACILITY.reset_actions(sync_point);
}

pub async fn on_sync_point(sync_point: &str) -> Result<(), Error> {
    if !cfg!(sync_test) {
        return Ok(());
    }
    SYNC_FACILITY.on_sync_point(sync_point).await
}
