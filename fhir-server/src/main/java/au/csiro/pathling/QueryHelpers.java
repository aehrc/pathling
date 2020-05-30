/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhirpath.FhirPath;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Common functionality for executing queries using Spark.
 *
 * @author John Grimes
 */
public abstract class QueryHelpers {

  /**
   * @param dataset A {@link Dataset} representing a raw resource, with at least 2 columns
   * @return A Dataset with two columns: an ID column and a value column containing all of the
   * columns from the original Dataset
   */
  @Nonnull
  public static Dataset<Row> resourceToIdAndValue(@Nonnull final Dataset<Row> dataset) {
    check(dataset.columns().length > 1);
    final String firstColumn = dataset.columns()[0];
    final String[] remainingColumns = Arrays
        .copyOfRange(dataset.columns(), 1, dataset.columns().length);
    final Column idColumn = dataset.col("id");
    final Column valueColumn = functions.struct(firstColumn, remainingColumns).as("value");

    return dataset.select(idColumn, valueColumn);
  }

  /**
   * @param left A {@link FhirPath} expression
   * @param right Another FhirPath expression
   * @param joinType A {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnId(@Nonnull final FhirPath left, @Nonnull final FhirPath right,
      @Nonnull final JoinType joinType) {
    final DatasetWithColumn hashedLeft = hashIdColumn(left);
    final DatasetWithColumn hashedRight = hashIdColumn(right);

    return joinOnId(hashedLeft, hashedRight, joinType);
  }

  /**
   * @param left A {@link Dataset}
   * @param leftId The ID {@link Column} in the left Dataset
   * @param right A {@link FhirPath} expression
   * @param joinType A {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnId(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftId, @Nonnull final FhirPath right,
      @Nonnull final JoinType joinType) {
    final DatasetWithColumn hashedLeft = hashIdColumn(left, leftId);
    final DatasetWithColumn hashedRight = hashIdColumn(right);

    return joinOnId(hashedLeft, hashedRight, joinType);
  }

  /**
   * @param left A {@link Dataset}
   * @param leftId The ID {@link Column} in the left Dataset
   * @param right Another Dataset
   * @param rightId The ID column in the right Dataset
   * @param joinType A {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnId(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftId, @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightId,
      @Nonnull final JoinType joinType) {
    final DatasetWithColumn hashedLeft = hashIdColumn(left, leftId);
    final DatasetWithColumn hashedRight = hashIdColumn(right, rightId);

    return joinOnId(hashedLeft, hashedRight, joinType);
  }

  @Nonnull
  public static List<Column> firstNColumns(@Nonnull final Dataset<Row> dataset,
      final int numberOfColumns) {
    return Arrays.asList(dataset.columns()).subList(0, numberOfColumns)
        .stream()
        .map(dataset::col)
        .collect(Collectors.toList());
  }

  @Nonnull
  public static DatasetWithColumns joinOnColumns(@Nonnull final Dataset<Row> left,
      @Nonnull final List<Column> leftColumns,
      final Dataset<Row> right, @Nonnull final List<Column> rightColumns,
      @Nonnull final JoinType joinType) {
    check(leftColumns.size() == rightColumns.size());

    final DatasetWithColumns leftDatasetWithColumns = hashColumns(left, leftColumns);
    final DatasetWithColumns rightDatasetWithColumns = hashColumns(right, rightColumns);

    @Nullable Column joinCondition = null;
    for (int i = 0; i < leftColumns.size(); i++) {
      final Column leftColumn = leftDatasetWithColumns.getColumns().get(i);
      final Column rightColumn = rightDatasetWithColumns.getColumns().get(i);
      // We need to do an explicit null check here, otherwise the join will nullify the result of 
      // the aggregation when the grouping value is null.
      final Column columnsEqual = leftColumn.isNull().and(rightColumn.isNull())
          .or(leftColumn.equalTo(rightColumn));
      joinCondition = i == 0
                      ? columnsEqual
                      : joinCondition.and(columnsEqual);
    }

    final Dataset<Row> dataset = leftDatasetWithColumns.getDataset()
        .join(rightDatasetWithColumns.getDataset(), joinCondition, joinType.getSparkName());
    return new DatasetWithColumns(dataset, leftDatasetWithColumns.getColumns());
  }

  @Nonnull
  private static DatasetWithColumns hashColumns(@Nonnull final Dataset<Row> dataset,
      @Nonnull final List<Column> columns) {
    @Nullable Dataset<Row> hashedDataset = dataset;
    final List<Column> hashedColumns = new ArrayList<>();

    for (int i = 0; i < columns.size(); i++) {
      final DatasetWithColumn datasetWithColumn = hashColumn(hashedDataset, columns.get(i),
          Integer.toString(i + 1));
      hashedDataset = datasetWithColumn.getDataset();
      hashedColumns.add(datasetWithColumn.getColumn());
    }

    checkNotNull(hashedDataset);
    return new DatasetWithColumns(hashedDataset, hashedColumns);
  }

  /**
   * @param left A {@link FhirPath} expression
   * @param right A {@link Dataset}
   * @param rightId The ID {@link Column} in the right Dataset
   * @param joinType A {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnReferenceAndId(@Nonnull final FhirPath left,
      @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightId,
      @Nonnull final JoinType joinType) {
    final DatasetWithColumn hashedLeft = hashIdColumn(left);
    final DatasetWithColumn hashedRight = hashIdColumn(right, rightId);

    @Nullable final Column reference = left.getValueColumn().getField("reference");
    checkNotNull(reference);
    final Column joinCondition = reference.equalTo(hashedRight.getColumn());
    return hashedLeft.getDataset()
        .join(hashedRight.getDataset(), joinCondition, joinType.getSparkName());
  }

  /**
   * @param left A {@link FhirPath} expression
   * @param right A {@link Dataset}
   * @param rightReference The reference {@link Column} in the right Dataset
   * @param joinType A {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnIdAndReference(@Nonnull final FhirPath left,
      @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightReference,
      @Nonnull final JoinType joinType) {
    final DatasetWithColumn hashedLeft = hashIdColumn(left);
    final DatasetWithColumn hashedRight = hashIdColumn(right, rightReference);

    @Nullable final Column reference = rightReference.getField("reference");
    checkNotNull(reference);
    final Column joinCondition = hashedLeft.getColumn().equalTo(reference);
    return hashedLeft.getDataset()
        .join(hashedRight.getDataset(), joinCondition, joinType.getSparkName());
  }

  /**
   * @param datasets A bunch of {@link Dataset} objects
   * @return A new Dataset that is the union of all the inputs
   */
  @Nonnull
  public static Dataset<Row> union(@Nonnull final Collection<Dataset<Row>> datasets) {
    final Dataset<Row> result = datasets.stream()
        .reduce(Dataset::union)
        .orElse(null);
    checkNotNull(result);
    return result;
  }

  @Nonnull
  private static Dataset<Row> joinOnId(@Nonnull final DatasetWithColumn left,
      @Nonnull final DatasetWithColumn right, @Nonnull final JoinType joinType) {
    final Column joinCondition = left.getColumn().equalTo(right.getColumn());
    return left.getDataset()
        .join(right.getDataset(), joinCondition, joinType.getSparkName());
  }

  @Nonnull
  private static DatasetWithColumn hashIdColumn(@Nonnull final FhirPath path) {
    return hashIdColumn(path.getDataset(), path.getIdColumn());
  }

  @Nonnull
  private static DatasetWithColumn hashIdColumn(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn) {
    return hashColumn(dataset, idColumn, "id");
  }

  @Nonnull
  public static DatasetWithColumn hashColumn(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column column, @Nonnull final String suffix) {
    final String hash = getHashForDataset(dataset);
    final String columnName = hash + "_" + suffix;
    final Dataset<Row> newDataset = dataset.withColumn(columnName, column);
    final Column newColumn = newDataset.col(columnName);
    return new DatasetWithColumn(newDataset, newColumn);
  }

  @Nonnull
  private static String getHashForDataset(@Nonnull final Dataset<Row> dataset) {
    return Integer.toString(dataset.hashCode(), 36);
  }

  /**
   * Represents a type of join that can be made between two {@link Dataset} objects.
   */
  @SuppressWarnings("unused")
  public enum JoinType {
    /**
     * Inner join.
     */
    INNER("inner"),
    /**
     * Cross join.
     */
    CROSS("cross"),
    /**
     * Outer join.
     */
    OUTER("outer"),
    /**
     * Full join.
     */
    FULL("full"),
    /**
     * Full outer join.
     */
    FULL_OUTER("full_outer"),
    /**
     * Left join.
     */
    LEFT("left"),
    /**
     * Left outer join.
     */
    LEFT_OUTER("left_outer"),
    /**
     * Right join.
     */
    RIGHT("right"),
    /**
     * Right outer join.
     */
    RIGHT_OUTER("right_outer"),
    /**
     * Left semi join.
     */
    LEFT_SEMI("left_semi"),
    /**
     * Left anti join.
     */
    LEFT_ANTI("left_anti");

    @Nonnull
    @Getter
    private final String sparkName;

    JoinType(@Nonnull final String sparkName) {
      this.sparkName = sparkName;
    }

  }

  /**
   * Represents a {@link Dataset} along with a {@link Column} that can be used to refer to one of
   * the columns within.
   */
  @Getter
  public static class DatasetWithColumn {

    @Nonnull
    private final Dataset<Row> dataset;

    @Nonnull
    private final Column column;

    private DatasetWithColumn(@Nonnull final Dataset<Row> dataset,
        @Nonnull final Column column) {
      this.dataset = dataset;
      this.column = column;
    }

  }

  @Getter
  public static class DatasetWithColumns {

    @Nonnull
    private final Dataset<Row> dataset;

    @Nonnull
    private final List<Column> columns;

    public DatasetWithColumns(@Nonnull final Dataset<Row> dataset,
        @Nonnull final List<Column> columns) {
      this.dataset = dataset;
      this.columns = columns;
    }

  }
}
