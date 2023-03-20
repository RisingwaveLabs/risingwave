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

package com.risingwave.sourcenode.core;

import com.risingwave.connector.api.source.SourceHandler;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import com.risingwave.sourcenode.common.DbzConnectorConfig;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** * handler for starting a debezium source connectors */
public class DbzSourceHandler implements SourceHandler {
    static final Logger LOG = LoggerFactory.getLogger(DbzSourceHandler.class);

    private final DbzConnectorConfig config;

    public DbzSourceHandler(DbzConnectorConfig config) {
        this.config = config;
    }

    @Override
    public void startSource(ServerCallStreamObserver<GetEventStreamResponse> responseObserver) {
        var runner = DbzCdcEngineRunner.newCdcEngineRunner(config, responseObserver);
        if (runner == null) {
            responseObserver.onCompleted();
            return;
        }

        try {
            // Start the engine
            runner.start();
            LOG.info("Start consuming events of table {}", config.getSourceId());
            while (runner.isRunning()) {
                try {
                    // Thread will block on the channel to get output from engine
                    var resp =
                            runner.getEngine().getOutputChannel().poll(500, TimeUnit.MILLISECONDS);

                    if (resp != null) {
                        // check whether the send queue has room for new messages
                        while (!responseObserver.isReady() && !Context.current().isCancelled()) {
                            // wait a bit to avoid OOM
                            Thread.sleep(500);
                        }

                        LOG.debug(
                                "Engine#{}: emit one chunk {} events to network ",
                                config.getSourceId(),
                                resp.getEventsCount());
                        responseObserver.onNext(resp);
                    }

                    if (Context.current().isCancelled()) {
                        LOG.info(
                                "Engine#{}: Connection broken detected, stop the engine",
                                config.getSourceId());
                        runner.stop();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Poll engine output channel fail. ", e);
                }
            }
        } catch (Throwable t) {
            LOG.error("Cdc engine failed.", t);
            try {
                runner.stop();
            } catch (Exception e) {
                LOG.warn("Failed to stop Engine#{}", config.getSourceId(), e);
            }
        }
        LOG.info("End consuming events of table {}", config.getSourceId());
    }
}
