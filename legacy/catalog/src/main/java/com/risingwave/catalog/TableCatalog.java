package com.risingwave.catalog;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.common.entity.EntityBase;
import com.risingwave.common.entity.NonRootLikeBase;
import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.common.exception.RisingWaveException;
import com.risingwave.proto.plan_common.RowFormatType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;

/** Table catalog definition. */
public class TableCatalog extends EntityBase<TableCatalog.TableId, TableCatalog.TableName>
    implements Table {
  private static final int ROWID_COLUMN_ID = 0;
  private static final String ROWID_COLUMN = "_row_id";
  private static final String TABLE_SOURCE_PREFIX = "_rw_source_";
  private final AtomicInteger nextColumnId = new AtomicInteger(ROWID_COLUMN_ID);
  private final List<ColumnCatalog> columns;
  private ColumnCatalog rowIdColumn;
  private final ConcurrentMap<ColumnCatalog.ColumnId, ColumnCatalog> columnById;
  private final ConcurrentMap<ColumnCatalog.ColumnName, ColumnCatalog> columnByName;
  private final boolean source;
  private final ImmutableIntList primaryKeyIndices;
  private final DataDistributionType distributionType;
  private final ImmutableMap<String, String> properties;
  private final RowFormatType rowFormat;
  // TODO: Need to be used as streaming job optimizes on append-only input specially.
  private final boolean appendOnly = false;
  private Long version;
  private final String rowSchemaLocation;

  TableCatalog(
      TableId id,
      TableName name,
      Collection<ColumnCatalog> columns,
      boolean source,
      ImmutableIntList primaryKeyIndices,
      DataDistributionType distributionType,
      ImmutableMap<String, String> properties,
      RowFormatType rowFormat,
      String rowSchemaLocation) {
    super(id, name);
    // We remark that we should only insert implicit row id for OLAP table, not MV, not Stream.
    // If an MV happen to have some implicit row id as its pk, it will be added in an explicit
    // manner.
    if (!isMaterializedView()) {
      this.nextColumnId.getAndIncrement();
    }
    this.columns = new ArrayList<>(columns);
    this.columnById = EntityBase.groupBy(columns, ColumnCatalog::getId);
    this.columnByName = EntityBase.groupBy(columns, ColumnCatalog::getEntityName);
    this.source = source;
    this.primaryKeyIndices = primaryKeyIndices;
    this.distributionType = distributionType;
    this.properties = properties;
    this.rowFormat = rowFormat;
    this.rowSchemaLocation = rowSchemaLocation;
    if (!isMaterializedView()) {
      // Put row-id column in map but do not put it in list of columns.
      this.rowIdColumn = buildRowIdColumn();
      this.columnById.put(rowIdColumn.getId(), rowIdColumn);
      this.columnByName.put(rowIdColumn.getEntityName(), rowIdColumn);
    }
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public Long getVersion() {
    return this.version;
  }

  public boolean isMaterializedView() {
    return false;
  }

  public boolean isAssociatedMaterializedView() {
    return false;
  }

  public ImmutableIntList getPrimaryKeyIndices() {
    return primaryKeyIndices;
  }

  public DataDistributionType getDistributionType() {
    return distributionType;
  }

  public ImmutableList<ColumnCatalog.ColumnId> getAllColumnIds() {
    return columns.stream().map(EntityBase::getId).collect(ImmutableList.toImmutableList());
  }

  public ImmutableList<ColumnCatalog> getAllColumns() {
    return getAllColumns(false);
  }

  public ImmutableList<ColumnCatalog> getAllColumns(boolean includeHidden) {
    if (!includeHidden) {
      return ImmutableList.copyOf(columns);
    } else {
      return ImmutableList.<ColumnCatalog>builder().add(getRowIdColumn()).addAll(columns).build();
    }
  }

  public ImmutableList<ColumnCatalog> getAllColumnsV2() {
    // put row id column to the last to match the behavior of mview
    return ImmutableList.<ColumnCatalog>builder().addAll(columns).add(getRowIdColumn()).build();
  }

  public Optional<ColumnCatalog> getColumn(ColumnCatalog.ColumnId columnId) {
    checkNotNull(columnId, "column id can't be null!");
    return Optional.ofNullable(columnById.get(columnId));
  }

  public Optional<ColumnCatalog> getColumn(String column) {
    checkNotNull(column, "column can't be null!");
    return Optional.ofNullable(
        columnByName.get(new ColumnCatalog.ColumnName(column, getEntityName())));
  }

  public ColumnCatalog getColumnChecked(ColumnCatalog.ColumnId columnId) {
    // TODO: Use PgErrorCode
    return getColumn(columnId).orElseThrow(() -> new RuntimeException("Column id not found!"));
  }

  public ColumnCatalog getColumnChecked(String column) {
    return getColumn(column)
        .orElseThrow(
            () ->
                new PgException(
                    PgErrorCode.UNDEFINED_COLUMN,
                    "Column not found: %s",
                    new ColumnCatalog.ColumnName(column, getEntityName())));
  }

  public Stream<String> mapColumnNames(Collection<ColumnCatalog.ColumnId> columnIds) {
    requireNonNull(columnIds, "columnIds");
    return columnIds.stream()
        .map(this::getColumnChecked)
        .map(ColumnCatalog::getEntityName)
        .map(ColumnCatalog.ColumnName::getValue);
  }

  public String joinColumnNames(Collection<ColumnCatalog.ColumnId> columnIds, String delimiter) {
    return mapColumnNames(columnIds).collect(Collectors.joining(delimiter));
  }

  void addColumn(String name, ColumnDesc columnDesc) {
    ColumnCatalog.ColumnName columnName = new ColumnCatalog.ColumnName(name, getEntityName());
    if (columnByName.containsKey(columnName)) {
      throw RisingWaveException.from(MetaServiceError.COLUMN_ALREADY_EXISTS, name, getEntityName());
    }

    ColumnCatalog.ColumnId columnId =
        new ColumnCatalog.ColumnId(nextColumnId.getAndIncrement(), getId());

    ColumnCatalog column = new ColumnCatalog(columnId, columnName, columnDesc);
    registerColumn(column);
  }

  private void registerColumn(ColumnCatalog column) {
    columnByName.put(column.getEntityName(), column);
    columnById.put(column.getId(), column);
    if (isAssociatedMaterializedView() && column.getName().equals(ROWID_COLUMN)) {
      // ignore row id column for associated mview, but store it
      rowIdColumn = column;
    } else {
      columns.add(column);
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> columnDataTypes =
        columns.stream()
            .map(ColumnCatalog::getDesc)
            .map(ColumnDesc::getDataType)
            .collect(Collectors.toList());

    List<String> fieldNames =
        columns.stream()
            .map(ColumnCatalog::getEntityName)
            .map(ColumnCatalog.ColumnName::getValue)
            .collect(Collectors.toList());

    return typeFactory.createStructType(StructKind.FULLY_QUALIFIED, columnDataTypes, fieldNames);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.of(1000, null);
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      String column,
      SqlCall call,
      @Nullable SqlNode parent,
      @Nullable CalciteConnectionConfig config) {
    return false;
  }

  public ImmutableMap<String, String> getProperties() {
    return properties;
  }

  public RowFormatType getRowFormat() {
    return rowFormat;
  }

  public String getRowSchemaLocation() {
    return rowSchemaLocation;
  }

  public boolean isSource() {
    return source;
  }

  private ColumnCatalog buildRowIdColumn() {
    final var typeFactory = RisingWaveTypeFactory.INSTANCE;
    return new ColumnCatalog(
        new ColumnCatalog.ColumnId(ROWID_COLUMN_ID, getId()),
        new ColumnCatalog.ColumnName(ROWID_COLUMN, getEntityName()),
        new ColumnDesc((RisingWaveDataType) typeFactory.createSqlType(SqlTypeName.BIGINT)));
  }

  public ColumnCatalog getRowIdColumn() {
    requireNonNull(rowIdColumn, "rowIdColumn is not initialized");
    return rowIdColumn;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", getId())
        .add("entityName", getEntityName())
        .add("columns", columns)
        .add("primaryKeyIndices", primaryKeyIndices)
        .add("distributionType", distributionType)
        .add("rowIdColumn", rowIdColumn)
        .toString();
  }

  public static String getTableSourceName(String name) {
    return String.format("%s%s", TABLE_SOURCE_PREFIX, name);
  }

  public static SqlIdentifier getTableSourceName(SqlIdentifier identifier) {
    var tableName = getTableSourceName(identifier.getSimple());
    return new SqlIdentifier(tableName, identifier.getCollation(), identifier.getParserPosition());
  }

  /** Table id definition. */
  public static class TableId extends NonRootLikeBase<Integer, SchemaCatalog.SchemaId> {

    public TableId(Integer value, SchemaCatalog.SchemaId parent) {
      super(value, parent);
    }
  }

  /** Table name definition. */
  public static class TableName extends NonRootLikeBase<String, SchemaCatalog.SchemaName> {
    public TableName(String value, SchemaCatalog.SchemaName parent) {
      super(value, parent);
    }

    public static TableName of(String db, String schema, String table) {
      return new TableName(table, SchemaCatalog.SchemaName.of(db, schema));
    }
  }
}
