package com.risingwave.execution.handler.util;

import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.MaterializedViewCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.rel.streaming.RwStreamMaterialize;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.plan.*;
import com.risingwave.proto.plan_common.*;
import com.risingwave.rpc.Messages;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.SerializationException;

/**
 * This class provides unified proto serializer for create table requests and create materialized
 * view requests.
 */
public class TableNodeSerializer {
  /**
   * @param catalog The table (or materialized view) catalog to be created on compute node.
   * @param root The root node of the streaming plan of the materialized view (if exists).
   * @return The `PlanFragment` proto of the table catalog.
   */
  public static PlanFragment createProtoFromCatalog(
      TableCatalog catalog, boolean isTableV2, RwStreamMaterialize root) {
    TableCatalog.TableId tableId = catalog.getId();
    CreateTableNode.Builder builder = CreateTableNode.newBuilder();

    // Set table ref id.
    builder.setTableRefId(Messages.getTableRefId(tableId));

    // Add sort key for materialized views.
    // TODO: clean the code and make them compact.
    if (catalog instanceof MaterializedViewCatalog) {
      var info = MaterializedViewInfo.newBuilder();

      // Add columns.
      for (Pair<String, ColumnDesc> pair : root.getColumns()) {
        com.risingwave.proto.plan_common.ColumnDesc.Builder columnDescBuilder =
            com.risingwave.proto.plan_common.ColumnDesc.newBuilder();
        columnDescBuilder
            .setName(pair.getKey())
            .setColumnType(pair.getValue().getDataType().getProtobufType());
        builder.addColumnDescs(columnDescBuilder);
      }

      MaterializedViewCatalog materializedViewCatalog = (MaterializedViewCatalog) catalog;
      // Set primary key columns.
      info.addAllPkIndices(materializedViewCatalog.getPrimaryKeyIndices());

      // Set column orders.
      // Sort key serialization starts. The column that is in primary key but not sort key should be
      // ordered by `Ascending`
      List<ColumnOrder> columnOrders = new ArrayList<ColumnOrder>();
      Set<Integer> columnAdded = new HashSet<Integer>();
      if (materializedViewCatalog.getCollation() != null) {
        List<RelFieldCollation> rfc = materializedViewCatalog.getCollation().getFieldCollations();
        for (RelFieldCollation relFieldCollation : rfc) {
          RexInputRef inputRef =
              root.getCluster()
                  .getRexBuilder()
                  .makeInputRef(root.getInput(), relFieldCollation.getFieldIndex());
          DataType returnType = ((RisingWaveDataType) inputRef.getType()).getProtobufType();
          columnAdded.add(inputRef.getIndex());
          InputRefExpr inputRefExpr =
              InputRefExpr.newBuilder().setColumnIdx(inputRef.getIndex()).build();
          RelFieldCollation.Direction dir = relFieldCollation.getDirection();
          OrderType orderType;
          if (dir == RelFieldCollation.Direction.ASCENDING) {
            orderType = OrderType.ASCENDING;
          } else if (dir == RelFieldCollation.Direction.DESCENDING) {
            orderType = OrderType.DESCENDING;
          } else {
            throw new SerializationException(String.format("%s direction not supported", dir));
          }
          ColumnOrder columnOrder =
              ColumnOrder.newBuilder()
                  .setOrderType(orderType)
                  .setInputRef(inputRefExpr)
                  .setReturnType(returnType)
                  .build();
          columnOrders.add(columnOrder);
        }
      }
      for (var primaryKeyIndex : materializedViewCatalog.getPrimaryKeyIndices()) {
        if (!columnAdded.contains(primaryKeyIndex)) {
          RexInputRef inputRef =
              root.getCluster().getRexBuilder().makeInputRef(root.getInput(), primaryKeyIndex);
          DataType returnType = ((RisingWaveDataType) inputRef.getType()).getProtobufType();
          columnAdded.add(inputRef.getIndex());
          InputRefExpr inputRefExpr =
              InputRefExpr.newBuilder().setColumnIdx(inputRef.getIndex()).build();
          OrderType orderType = OrderType.ASCENDING;
          ColumnOrder columnOrder =
              ColumnOrder.newBuilder()
                  .setOrderType(orderType)
                  .setInputRef(inputRefExpr)
                  .setReturnType(returnType)
                  .build();
          columnOrders.add(columnOrder);
          columnAdded.add(primaryKeyIndex);
        }
      }
      info.addAllColumnOrders(columnOrders);
      // Add associated TableId to MV.
      var associatedTableId = root.getAssociatedTableId();
      if (associatedTableId != null) {
        info.setAssociatedTableRefId(Messages.getTableRefId(associatedTableId));
      }

      builder.setMaterializedView(info);
    } else {
      // If not materialized view then build regular table node.
      var info = TableSourceInfo.newBuilder();
      // Add columns.
      List<ColumnCatalog> allColumns = catalog.getAllColumns(true);
      if (isTableV2) {
        allColumns = catalog.getAllColumnsV2();
      }

      for (var column : allColumns) {
        com.risingwave.proto.plan_common.ColumnDesc.Builder columnDescBuilder =
            com.risingwave.proto.plan_common.ColumnDesc.newBuilder();
        columnDescBuilder
            .setName(column.getName())
            .setColumnType(column.getDesc().getDataType().getProtobufType())
            .setColumnId(column.getId().getValue());
        builder.addColumnDescs(columnDescBuilder);
      }
      builder.setTableSource(info);
    }

    // Add exchange node on top.
    ExchangeInfo exchangeInfo =
        ExchangeInfo.newBuilder().setMode(ExchangeInfo.DistributionMode.SINGLE).build();

    PlanNode rootNode = PlanNode.newBuilder().setCreateTable(builder.build()).build();

    return PlanFragment.newBuilder().setRoot(rootNode).setExchangeInfo(exchangeInfo).build();
  }
}
