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

package com.risingwave.connector.source.core;

import static com.risingwave.proto.ConnectorServiceProto.SourceType.POSTGRES;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.DebeziumOffset;
import com.risingwave.connector.source.common.CdcConnectorException;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.connector.source.common.DbzSourceUtils;
import com.risingwave.java.binding.Binding;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** handler for starting a debezium source connectors for jni */
public class JniDbzSourceHandler implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(JniDbzSourceHandler.class);

    private final DbzConnectorConfig config;

    DbzCdcEngineRunner runner;
    long channelPtr;

    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "JniDbzSourceHandler-" + config.getSourceId());
                        }
                    });

    public JniDbzSourceHandler(DbzConnectorConfig config, long channelPtr) {
        this.config = config;
        this.channelPtr = channelPtr;
        this.runner = DbzCdcEngineRunner.create(config, channelPtr);

        if (runner == null) {
            throw new CdcConnectorException("Failed to create engine runner");
        }
    }

    public static JniDbzSourceHandler runJniDbzSourceThread(
            byte[] getEventStreamRequestBytes, long channelPtr) throws Exception {
        var request =
                ConnectorServiceProto.GetEventStreamRequest.parseFrom(getEventStreamRequestBytes);

        // For jni.rs
        java.lang.Thread.currentThread()
                .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());
        // userProps extracted from request, underlying implementation is UnmodifiableMap
        Map<String, String> mutableUserProps = new HashMap<>(request.getPropertiesMap());
        mutableUserProps.put("source.id", Long.toString(request.getSourceId()));
        boolean isCdcSourceJob = request.getIsSourceJob();

        if (request.getSourceType() == POSTGRES) {
            DbzSourceUtils.createPostgresPublicationIfNeeded(
                    request.getPropertiesMap(), request.getSourceId());
        }

        var config =
                new DbzConnectorConfig(
                        SourceTypeE.valueOf(request.getSourceType()),
                        request.getSourceId(),
                        request.getStartOffset(),
                        mutableUserProps,
                        request.getSnapshotDone(),
                        isCdcSourceJob);
        JniDbzSourceHandler handler = new JniDbzSourceHandler(config, channelPtr);
        handler.start();
        return handler;
    }

    public void start() {
        this.executor.submit(this);
    }

    public void commitOffset(DebeziumOffset offset) throws InterruptedException {
        var changeEventConsumer = runner.getChangeEventConsumer();
        if (changeEventConsumer != null) {
            changeEventConsumer.commitOffset(offset);
        } else {
            LOG.warn("Engine#{}: changeEventConsumer is null", config.getSourceId());
        }
    }

    private boolean sendHandshakeMessage(
            DbzCdcEngineRunner runner, long channelPtr, boolean startOk) throws Exception {
        // send a handshake message to notify the Source executor
        // if the handshake is not ok, the split reader will return error to source actor
        var controlInfo =
                GetEventStreamResponse.ControlInfo.newBuilder().setHandshakeOk(startOk).build();

        var handshakeMsg =
                GetEventStreamResponse.newBuilder()
                        .setSourceId(config.getSourceId())
                        .setControl(controlInfo)
                        .build();
        var success = Binding.sendCdcSourceMsgToChannel(channelPtr, handshakeMsg.toByteArray());
        if (!success) {
            LOG.info(
                    "Engine#{}: JNI sender broken detected, stop the engine", config.getSourceId());
            runner.stop();
        }
        return success;
    }

    @Override
    public void run() {
        try {
            // Start the engine
            var startOk = runner.start();
            if (!sendHandshakeMessage(runner, channelPtr, startOk)) {
                LOG.error(
                        "Failed to send handshake message to channel. sourceId={}",
                        config.getSourceId());
                return;
            }

            LOG.info("Start consuming events of table {}", config.getSourceId());
            while (runner.isRunning()) {
                // check whether the send queue has room for new messages
                // Thread will block on the channel to get output from engine
                var resp = runner.getEngine().getOutputChannel().poll(500, TimeUnit.MILLISECONDS);
                boolean success;
                if (resp != null) {
                    ConnectorNodeMetrics.incSourceRowsReceived(
                            config.getSourceType().toString(),
                            String.valueOf(config.getSourceId()),
                            resp.getEventsCount());
                    LOG.debug(
                            "Engine#{}: emit one chunk {} events to network ",
                            config.getSourceId(),
                            resp.getEventsCount());
                    success = Binding.sendCdcSourceMsgToChannel(channelPtr, resp.toByteArray());
                } else {
                    // If resp is null means just check whether channel is closed.
                    success = Binding.sendCdcSourceMsgToChannel(channelPtr, null);
                }
                if (!success) {
                    LOG.info(
                            "Engine#{}: JNI sender broken detected, stop the engine",
                            config.getSourceId());
                    runner.stop();
                    return;
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
    }
}
