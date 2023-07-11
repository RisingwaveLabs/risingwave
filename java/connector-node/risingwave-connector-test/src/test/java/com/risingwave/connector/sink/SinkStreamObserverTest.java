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

package com.risingwave.connector.sink;

import com.risingwave.connector.SinkStreamObserver;
import com.risingwave.connector.TestUtils;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.Op;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SinkStreamObserverTest {

    public ConnectorServiceProto.SinkParam fileSinkParam =
            ConnectorServiceProto.SinkParam.newBuilder()
                    .setTableSchema(TestUtils.getMockTableProto())
                    .putAllProperties(
                            Map.of("output.path", "/tmp/rw-connector", "connector", "file"))
                    .build();

    @Test
    public void testOnNext_StartTaskValidation() {

        StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> testResponseObserver =
                createNoisyFailResponseObserver();
        SinkStreamObserver sinkStreamObserver = getMockSinkStreamObserver(testResponseObserver);
        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(1)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();

        // test validation of start sink
        boolean exceptionThrown = false;
        try {
            sinkStreamObserver.onNext(firstSync);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("sink is not initialized"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: \"Sink is not initialized. Invoke `CreateSink` first.\"");
        }
    }

    private static StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse>
            createNoisyFailResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(ConnectorServiceProto.SinkWriterStreamResponse sinkResponse) {
                // response ok
            }

            @Override
            public void onError(Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {}
        };
    }

    private static SinkStreamObserver getMockSinkStreamObserver(
            StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> testResponseObserver) {
        return new SinkStreamObserver(testResponseObserver);
    }

    @Test
    public void testOnNext_syncValidation() {
        SinkStreamObserver sinkStreamObserver =
                getMockSinkStreamObserver(createNoisyFailResponseObserver());
        ConnectorServiceProto.SinkWriterStreamRequest startSink =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkWriterStreamRequest.StartSink.newBuilder()
                                        .setSinkParam(fileSinkParam)
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest duplicateSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();

        // test validation of sync
        boolean exceptionThrown = false;
        try {
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstSync);
            sinkStreamObserver.onNext(duplicateSync);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("epoch"));
        }
        if (!exceptionThrown) {
            Assert.fail("Expected exception not thrown: `No epoch assigned. Invoke `StartEpoch`.`");
        }
    }

    @Test
    public void testOnNext_startEpochValidation() {

        SinkStreamObserver sinkStreamObserver;
        ConnectorServiceProto.SinkWriterStreamRequest startSink =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkWriterStreamRequest.StartSink.newBuilder()
                                        .setSinkParam(fileSinkParam)
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest startEpoch =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBeginEpoch(
                                ConnectorServiceProto.SinkWriterStreamRequest.BeginEpoch
                                        .newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest duplicateStartEpoch =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBeginEpoch(
                                ConnectorServiceProto.SinkWriterStreamRequest.BeginEpoch
                                        .newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();

        // test validation of start epoch
        boolean exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstSync);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("epoch is not started"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `Epoch is not started. Invoke `StartEpoch`.`");
        }

        exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(startEpoch);
            sinkStreamObserver.onNext(duplicateStartEpoch);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(
                    e.getMessage().toLowerCase().contains("new epoch id should be larger"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `invalid epoch: new epoch ID should be larger than current epoch`");
        }
    }

    @Test
    public void testOnNext_writeValidation() {
        SinkStreamObserver sinkStreamObserver;

        ConnectorServiceProto.SinkWriterStreamRequest startSink =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkWriterStreamRequest.StartSink.newBuilder()
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .setSinkParam(fileSinkParam))
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest firstStartEpoch =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBeginEpoch(
                                ConnectorServiceProto.SinkWriterStreamRequest.BeginEpoch
                                        .newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest firstWrite =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setWriteBatch(
                                ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch
                                        .newBuilder()
                                        .setEpoch(0)
                                        .setBatchId(1)
                                        .setJsonPayload(
                                                ConnectorServiceProto.SinkWriterStreamRequest
                                                        .WriteBatch.JsonPayload.newBuilder()
                                                        .addRowOps(
                                                                ConnectorServiceProto
                                                                        .SinkWriterStreamRequest
                                                                        .WriteBatch.JsonPayload
                                                                        .RowOp.newBuilder()
                                                                        .setOpType(Op.INSERT)
                                                                        .setLine(
                                                                                "{\"id\": 1, \"name\": \"test\"}")
                                                                        .build()))
                                        .build())
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest secondStartEpoch =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBeginEpoch(
                                ConnectorServiceProto.SinkWriterStreamRequest.BeginEpoch
                                        .newBuilder()
                                        .setEpoch(1)
                                        .build())
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest secondWrite =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setWriteBatch(
                                ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch
                                        .newBuilder()
                                        .setEpoch(0)
                                        .setBatchId(2)
                                        .setJsonPayload(
                                                ConnectorServiceProto.SinkWriterStreamRequest
                                                        .WriteBatch.JsonPayload.newBuilder()
                                                        .addRowOps(
                                                                ConnectorServiceProto
                                                                        .SinkWriterStreamRequest
                                                                        .WriteBatch.JsonPayload
                                                                        .RowOp.newBuilder()
                                                                        .setOpType(Op.INSERT)
                                                                        .setLine(
                                                                                "{\"id\": 2, \"name\": \"test\"}")
                                                                        .build()))
                                        .build())
                        .build();

        boolean exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstStartEpoch);
            sinkStreamObserver.onNext(firstWrite);
            sinkStreamObserver.onNext(firstWrite);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("batch id"));
        }
        if (!exceptionThrown) {
            Assert.fail("Expected exception not thrown: `invalid batch id`");
        }

        exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstStartEpoch);
            sinkStreamObserver.onNext(firstWrite);
            sinkStreamObserver.onNext(firstSync);
            sinkStreamObserver.onNext(secondStartEpoch);
            sinkStreamObserver.onNext(secondWrite); // with mismatched epoch
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("invalid epoch"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `invalid epoch: expected write to epoch 1, got 0`");
        }
    }
}
