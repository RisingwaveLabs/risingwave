package com.risingwave.planner.rel.physical;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.RwScan;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.proto.plan.Field;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SeqScanNode;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.rpc.Messages;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

/** Executor to scan from a columnar table */
public class RwBatchTableScan extends RwScan implements RisingWaveBatchPhyRel {
  protected RwBatchTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table, tableId, columnIds);
    checkConvention();
  }

  public static RwBatchTableScan create(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    TableCatalog tableCatalog = table.unwrapOrThrow(TableCatalog.class);

    RelTraitSet newTraitSet = traitSet.plus(RisingWaveBatchPhyRel.BATCH_PHYSICAL);

    return new RwBatchTableScan(
        cluster, newTraitSet, Collections.emptyList(), table, tableCatalog.getId(), columnIds);
  }

  public RwBatchTableScan copy(RelTraitSet traitSet) {
    return new RwBatchTableScan(
        getCluster(), traitSet, getHints(), getTable(), getTableId(), getColumnIds());
  }

  @Override
  public RelNode convertToDistributed() {
    return copy(getTraitSet().plus(BATCH_DISTRIBUTED).plus(RwDistributions.RANDOM_DISTRIBUTED));
  }

  @Override
  public PlanNode serialize() {
    var table = getTable().unwrapOrThrow(TableCatalog.class);

    TableRefId tableRefId = Messages.getTableRefId(tableId);
    SeqScanNode.Builder seqScanNodeBuilder = SeqScanNode.newBuilder().setTableRefId(tableRefId);
    columnIds.forEach(
        c -> {
          seqScanNodeBuilder.addColumnIds(c.getValue());
          var dataType = table.getColumnChecked(c).getDesc().getDataType().getProtobufType();
          seqScanNodeBuilder.addFields(
              Field.newBuilder()
                  .setDataType(dataType)
                  .setName(table.getColumnChecked(c).getName())
                  .build());
          ;
        });
    return PlanNode.newBuilder()
        .setSeqScan(seqScanNodeBuilder.build())
        .setIdentity(BatchPlan.getCurrentNodeIdentity(this))
        .build();
  }
}
