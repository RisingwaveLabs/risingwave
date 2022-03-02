package com.risingwave.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.proto.plan.RowFormatType;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

/**
 * Create Materialize View Info When we create a materialized view, there could be sort/limit/topN
 * at the end. We would achieve the sort part(if it is a sort or topN) at the materialized view, or
 * more specifically, at the storage of the materialized view.
 *
 * <p>The limit part(if it is a limit or topN) would be achieved when OLAP queries this materialized
 * view.
 *
 * <p>Therefore, we need to additionally keep collation(sort key/order), offset and limit in the
 * catalog so that we can modify the execution plan of an olap query on materialized view properly.
 *
 * <p>See BatchScanConverterRule for more information.
 */
public class CreateMaterializedViewInfo extends CreateTableInfo {
  private final RelCollation collation;

  private final TableCatalog.TableId associated;

  private CreateMaterializedViewInfo(
      String tableName,
      ImmutableList<Pair<String, ColumnDesc>> columns,
      ImmutableIntList primaryKeyIndices,
      ImmutableMap<String, String> properties,
      boolean appendOnly,
      RowFormatType rowFormat,
      String rowSchemaLocation,
      ImmutableList<TableCatalog.TableId> dependentTables,
      @Nullable RelCollation collation,
      TableCatalog.TableId associated) {
    super(
        tableName,
        columns,
        primaryKeyIndices,
        properties,
        appendOnly,
        false,
        rowFormat,
        rowSchemaLocation,
        dependentTables);
    this.collation = collation;
    this.associated = associated;
  }

  public RelCollation getCollation() {
    return collation;
  }

  public boolean isAssociated() {
    return associated != null;
  }

  public TableCatalog.TableId getAssociatedTableId() {
    return associated;
  }

  @Override
  public boolean isMv() {
    return true;
  }

  public static CreateMaterializedViewInfo.Builder builder(String tableName) {
    return new CreateMaterializedViewInfo.Builder(tableName);
  }

  /** Builder */
  public static class Builder extends CreateTableInfo.Builder {
    private RelCollation collation = null;

    private TableCatalog.TableId associated = null;

    private Builder(String tableName) {
      super(tableName);
    }

    public void setCollation(RelCollation collation) {
      this.collation = collation;
    }

    public void setAssociated(TableCatalog.TableId associated) {
      this.associated = associated;
    }

    public CreateMaterializedViewInfo build() {
      return new CreateMaterializedViewInfo(
          tableName,
          ImmutableList.copyOf(columns),
          ImmutableIntList.copyOf(primaryKeyIndices),
          ImmutableMap.copyOf(properties),
          appendOnly,
          rowFormat,
          rowSchemaLocation,
          ImmutableList.copyOf(dependentTables),
          collation,
          associated);
    }
  }
}
