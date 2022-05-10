package com.risingwave.planner.rel.streaming;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.proto.plan_common.Field;
import com.risingwave.proto.streaming.plan.*;
import com.risingwave.rpc.Messages;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.util.ImmutableIntList;

/** Chain Node */
public class RwStreamChain extends Union implements RisingWaveStreamingRel {

  private final TableCatalog.TableId tableId;
  private final ImmutableList<ColumnCatalog.ColumnId> columnIds;
  private final ImmutableIntList primaryKeyIndices;
  private final ImmutableList<Field> upstreamFields;

  /**
   * ChainNode is used to scan materialized view snapshot and its further stream chunks.
   *
   * @param tableId table id of the origin materialized view table.
   * @param primaryKeyIndices derived pk indices of chain output.
   * @param columnIds column ids of the origin materialized view.
   */
  public RwStreamChain(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableCatalog.TableId tableId,
      ImmutableIntList primaryKeyIndices,
      ImmutableList<ColumnCatalog.ColumnId> columnIds,
      ImmutableList<Field> upstreamFields,
      List<RelNode> inputs) {
    super(cluster, traitSet, inputs, true);
    this.tableId = tableId;
    this.primaryKeyIndices = primaryKeyIndices;
    this.columnIds = columnIds;
    this.upstreamFields = upstreamFields;
  }

  public TableCatalog.TableId getTableId() {
    return tableId;
  }

  public ImmutableIntList getPrimaryKeyIndices() {
    return primaryKeyIndices;
  }

  public ImmutableList<ColumnCatalog.ColumnId> getColumnIds() {
    return columnIds;
  }

  public ImmutableList<Field> getUpstreamFields() {
    return upstreamFields;
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new RwStreamChain(
        getCluster(), traitSet, tableId, primaryKeyIndices, columnIds, upstreamFields, inputs);
  }

  /** Explain */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("tableId", tableId)
        .item("primaryKeyIndices", primaryKeyIndices)
        .item("columnIds", columnIds);
  }

  @Override
  public StreamNode serialize() {
    ChainNode.Builder builder = ChainNode.newBuilder();
    builder.setTableRefId(Messages.getTableRefId(tableId));
    columnIds.forEach(c -> builder.addColumnIds(c.getValue()));
    builder.addAllUpstreamFields(upstreamFields);
    ChainNode chainNode = builder.build();
    return StreamNode.newBuilder()
        .setChain(chainNode)
        .addInput(
            // Just a placeholder for operator id gen.
            StreamNode.newBuilder().setMerge(MergeNode.newBuilder().build()).build())
        .setIdentity(StreamingPlan.getCurrentNodeIdentity(this))
        .addAllPkIndices(primaryKeyIndices)
        .build();
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
