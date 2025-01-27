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

use std::fmt::Debug;

use async_trait::async_trait;
use await_tree::InstrumentAwait;
use educe::Educe;
use risingwave_common::util::addr::is_local_address;

use super::error::ExchangeChannelClosed;
use super::permit::Sender;
use crate::error::StreamResult;
use crate::executor::DispatcherMessageBatch as Message;
use crate::task::{ActorId, SharedContext};

/// `Output` provides an interface for `Dispatcher` to send data into downstream actors.
#[async_trait]
pub trait Output: Debug + Send + Sync + 'static {
    async fn send(&mut self, message: Message) -> StreamResult<()>;

    /// The downstream actor id.
    fn actor_id(&self) -> ActorId;

    fn boxed(self) -> BoxedOutput
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub type BoxedOutput = Box<dyn Output>;

/// `LocalOutput` sends data to a local channel.
#[derive(Educe)]
#[educe(Debug)]
pub struct LocalOutput {
    actor_id: ActorId,

    #[educe(Debug(ignore))]
    span: await_tree::Span,

    #[educe(Debug(ignore))]
    ch: Sender,
}

impl LocalOutput {
    pub fn new(actor_id: ActorId, ch: Sender) -> Self {
        Self {
            actor_id,
            span: format!("LocalOutput (actor {:?})", actor_id).into(),
            ch,
        }
    }
}

#[async_trait]
impl Output for LocalOutput {
    async fn send(&mut self, message: Message) -> StreamResult<()> {
        self.ch
            .send(message)
            .verbose_instrument_await(self.span.clone())
            .await
            .map_err(|_| ExchangeChannelClosed::output(self.actor_id).into())
    }

    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// `RemoteOutput` compacts the data and send to a local buffer channel, which will be further sent
/// to the remote actor by [`ExchangeService`].
///
/// [`ExchangeService`]: risingwave_pb::task_service::exchange_service_server::ExchangeService
// FIXME: can we just use the same `Output` with local and compacts it in gRPC server?
#[derive(Educe)]
#[educe(Debug)]
pub struct RemoteOutput {
    actor_id: ActorId,

    #[educe(Debug(ignore))]
    span: await_tree::Span,

    #[educe(Debug(ignore))]
    ch: Sender,
}

impl RemoteOutput {
    pub fn new(actor_id: ActorId, ch: Sender) -> Self {
        Self {
            actor_id,
            span: format!("RemoteOutput (actor {:?})", actor_id).into(),
            ch,
        }
    }
}

#[async_trait]
impl Output for RemoteOutput {
    async fn send(&mut self, message: Message) -> StreamResult<()> {
        let message = match message {
            Message::Chunk(chk) => Message::Chunk(chk.compact()),
            _ => message,
        };

        self.ch
            .send(message)
            .verbose_instrument_await(self.span.clone())
            .await
            .map_err(|_| ExchangeChannelClosed::output(self.actor_id).into())
    }

    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// Create a [`LocalOutput`] or [`RemoteOutput`] instance for the current actor id and the
/// downstream actor id. Used by dispatchers.
pub fn new_output(
    context: &SharedContext,
    actor_id: ActorId,
    down_id: ActorId,
) -> StreamResult<BoxedOutput> {
    let tx = context.take_sender(&(actor_id, down_id))?;

    let is_local_address = match context.get_actor_info(&down_id) {
        Ok(info) => is_local_address(&context.addr, &info.get_host()?.into()),
        // If we can't get the actor info locally, it must be a remote actor.
        // This may happen when we create a mv-on-mv on different workers from the upstream. #4153
        Err(_) => false,
    };

    let output = if is_local_address {
        LocalOutput::new(down_id, tx).boxed()
    } else {
        RemoteOutput::new(down_id, tx).boxed()
    };

    Ok(output)
}
