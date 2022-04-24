package com.risingwave.scheduler.streaming;

import com.google.inject.Inject;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.metanode.CreateMaterializedViewRequest;
import com.risingwave.proto.metanode.CreateMaterializedViewResponse;
import com.risingwave.proto.metanode.DropMaterializedViewRequest;
import com.risingwave.proto.metanode.DropMaterializedViewResponse;
import com.risingwave.proto.metanode.FlushRequest;
import com.risingwave.proto.metanode.FlushResponse;
import com.risingwave.proto.plan_common.TableRefId;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.MetaClient;

/** The implementation of a stream manager synchronized with meta service. */
public class RemoteStreamManager implements StreamManager {
  private final MetaClient metaClient;

  @Inject
  public RemoteStreamManager(MetaClient client) {
    this.metaClient = client;
  }

  @Override
  public void createMaterializedView(StreamNode streamNode, TableRefId tableRefId) {
    CreateMaterializedViewRequest.Builder builder = CreateMaterializedViewRequest.newBuilder();
    builder.setStreamNode(streamNode);
    builder.setTableRefId(tableRefId);
    CreateMaterializedViewResponse response = metaClient.createMaterializedView(builder.build());
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create materialized view failed");
    }
  }

  public void dropMaterializedView(TableRefId tableRefId) {
    DropMaterializedViewRequest.Builder builder = DropMaterializedViewRequest.newBuilder();
    DropMaterializedViewResponse response =
        metaClient.dropMaterializedView(builder.setTableRefId(tableRefId).build());
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Drop materialized view failed");
    }
  }

  public void flush() {
    FlushRequest.Builder builder = FlushRequest.newBuilder();
    FlushResponse response = metaClient.flush(builder.build());
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Drop materialized view failed");
    }
  }
}
