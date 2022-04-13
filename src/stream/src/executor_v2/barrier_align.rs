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

use futures::StreamExt;
use tokio::select;

use crate::executor_v2::error::StreamExecutorResult;
use crate::executor_v2::{Barrier, BoxedMessageStream, Executor, Message, StreamChunk};

#[derive(Debug, PartialEq)]
enum BarrierWaitState {
    Left,
    Right,
    Either,
}

#[derive(Debug)]
pub enum AlignedMessage {
    Left(StreamExecutorResult<StreamChunk>),
    Right(StreamExecutorResult<StreamChunk>),
    Barrier(Barrier),
}

impl<'a> TryFrom<&'a AlignedMessage> for &'a Barrier {
    type Error = ();

    fn try_from(m: &'a AlignedMessage) -> std::result::Result<Self, Self::Error> {
        match m {
            AlignedMessage::Barrier(b) => Ok(b),
            _ => Err(()),
        }
    }
}

pub struct BarrierAligner {
    /// The input from the left executor
    input_l: BoxedMessageStream,
    /// The input from the right executor
    input_r: BoxedMessageStream,
    /// The barrier state
    state: BarrierWaitState,
}

impl BarrierAligner {
    pub fn new(input_l: Box<dyn Executor>, input_r: Box<dyn Executor>) -> Self {
        // Wrap the input executors into streams to ensure cancellation-safety
        Self {
            input_l: Box::pin(input_l.execute()),
            input_r: Box::pin(input_r.execute()),
            state: BarrierWaitState::Either,
        }
    }

    pub async fn next(&mut self) -> AlignedMessage {
        loop {
            select! {
                message = self.input_l.next(), if self.state != BarrierWaitState::Right => {
                match message.unwrap() {
                    Ok(message) => match message {
                        Message::Chunk(chunk) => break AlignedMessage::Left(Ok(chunk)),
                            Message::Barrier(barrier) => {
                                match self.state {
                                    BarrierWaitState::Left => {
                                        self.state = BarrierWaitState::Either;
                                        break AlignedMessage::Barrier(barrier);
                                    }
                                    BarrierWaitState::Either => {
                                        self.state = BarrierWaitState::Right;
                                    }
                                    _ => unreachable!("Should not reach this barrier state: {:?}", self.state),
                                };
                            },
                        },
                        Err(e) => break AlignedMessage::Left(Err(e)),
                    }
                },
                message = self.input_r.next(), if self.state != BarrierWaitState::Left => {
                    match message.unwrap() {
                        Ok(message) => match message {
                            Message::Chunk(chunk) => break AlignedMessage::Right(Ok(chunk)),
                            Message::Barrier(barrier) => match self.state {
                                BarrierWaitState::Right => {
                                    self.state = BarrierWaitState::Either;
                                    break AlignedMessage::Barrier(barrier);
                                }
                                BarrierWaitState::Either => {
                                    self.state = BarrierWaitState::Left;
                                }
                                _ => unreachable!("Should not reach this barrier state: {:?}", self.state),
                            },
                        },
                        Err(e) => break AlignedMessage::Right(Err(e)),
                    }
                }
            }
        }
    }
}
