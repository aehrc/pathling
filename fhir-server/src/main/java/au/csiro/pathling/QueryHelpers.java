/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Strings.randomShortString;

import au.csiro.pathling.fhirpath.FhirPath;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
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
   * String used at the end of column names used for resource identity.
   */
  public static final String ID_COLUMN_SUFFIX = "_id";

  /**
   * String used at the end of column names used for expression values.
   */
  public static final String VALUE_COLUMN_SUFFIX = "_value";

  /**
   * String used at the end of column names used for resource types.
   */
  public static final String TYPE_COLUMN_SUFFIX = "_type";

  /**
   * @param dataset A {@link Dataset} representing a raw resource, with at least 2 columns
   * @return A Dataset with two columns: an ID column and a value column containing all of the
   * columns from the original Dataset
   */
  @Nonnull
  public static DatasetWithIdAndValue convertRawResource(@Nonnull final Dataset<Row> dataset) {
    check(dataset.columns().length > 1);

    final String hash = randomShortString();
    final String idColumnName = hash + ID_COLUMN_SUFFIX;
    final String valueColumnName = hash + VALUE_COLUMN_SUFFIX;

    final String firstColumn = dataset.columns()[0];
    final String[] remainingColumns = Arrays
        .copyOfRange(dataset.columns(), 1, dataset.columns().length);

    Dataset<Row> result = dataset.withColumn(idColumnName, dataset.col("id"));
    result = result.withColumn(valueColumnName, functions.struct(firstColumn, remainingColumns));
    final Column idColumn = result.col(idColumnName);
    final Column valueColumn = result.col(valueColumnName);

    return new DatasetWithIdAndValue(result.select(idColumn, valueColumn), idColumn, valueColumn);
  }

  /**
   * Filters a dataset to only the nominated ID column, plus any pre-existing value columns.
   * Pre-existing ID columns are dropped out. p
   *
   * @param dataset the dataset to perform the operation upon
   * @param idColumn the ID column to preserve
   * @return a new {@link Dataset} with a subset of columns
   */
  @Nonnull
  public static Dataset<Row> applySelection(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn) {
    // Preserve value columns in the existing dataset, drop any pre-existing ID columns.
    final List<Column> columns = new ArrayList<>();
    idColumn.ifPresent(columns::add);
    final List<Column> preservedColumns = Stream.of(dataset.columns())
        .filter(column ->
            column.endsWith(VALUE_COLUMN_SUFFIX) || column.endsWith(TYPE_COLUMN_SUFFIX))
        .map(dataset::col)
        .collect(Collectors.toList());
    columns.addAll(preservedColumns);
    return dataset.select(columns.toArray(new Column[0]));
  }

  /**
   * De-duplicates rows after joining.
   */
  @Nonnull
  private static Dataset<Row> selectJoinTarget(@Nonnull final Dataset<Row> target,
      @Nonnull final Dataset<Row> source) {
    final List<String> targetColumns = Arrays.asList(target.columns());
    final List<String> sourceColumns = Arrays.asList(source.columns());
    final HashSet<String> result = new HashSet<>(targetColumns);
    result.removeAll(sourceColumns);
    if (result.isEmpty()) {
      return target;
    } else {
      final String[] columns = result.toArray(new String[0]);
      if (result.size() == 1) {
        return target.select(columns[0]);
      } else {
        final String[] remainingColumns = Arrays.copyOfRange(columns, 1, columns.length);
        return target.select(columns[0], remainingColumns);
      }
    }
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
    check(left.getIdColumn().isPresent());
    check(right.getIdColumn().isPresent());
    final Column joinCondition = left.getIdColumn().get().equalTo(right.getIdColumn().get());
    final Dataset<Row> target = selectJoinTarget(right.getDataset(), left.getDataset());
    return left.getDataset().join(target, joinCondition, joinType.getSparkName());
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
    // Don't do unnecessary joins between identical datasets.
    if (left.equals(right.getDataset())) {
      return left;
    }
    check(right.getIdColumn().isPresent());
    final Column joinCondition = leftId.equalTo(right.getIdColumn().get());
    final Dataset<Row> target = selectJoinTarget(right.getDataset(), left);
    return left.join(target, joinCondition, joinType.getSparkName());
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
      @Nonnull final Column rightId, @Nonnull final JoinType joinType) {
    // Don't do unnecessary joins between identical datasets.
    if (left.equals(right)) {
      return left;
    }
    final Column joinCondition = leftId.equalTo(rightId);
    final Dataset<Row> target = selectJoinTarget(right, left);
    return left.join(target, joinCondition, joinType.getSparkName());
  }

  /**
   * Returns a list of the first N columns within a {@link Dataset}.
   *
   * @param dataset the Dataset to get the columns from
   * @param numberOfColumns the number of columns to take
   * @return a list of {@link Column} objects
   */
  @Nonnull
  public static List<Column> firstNColumns(@Nonnull final Dataset<Row> dataset,
      final int numberOfColumns) {
    return Arrays.asList(dataset.columns()).subList(0, numberOfColumns)
        .stream()
        .map(dataset::col)
        .collect(Collectors.toList());
  }

  /**
   * Join two datasets based on the equality of an arbitrary set of columns. The same number of
   * columns must be provided for each dataset, and it is assumed that they are matched on their
   * position within their respective lists.
   *
   * @param left the first {@link Dataset}
   * @param leftColumns the columns for the first Dataset
   * @param right the second Dataset
   * @param rightColumns the columns for the second Dataset
   * @param joinType the type of join to use
   * @return a {@link DatasetWithColumns} containing the joined Dataset and the columns that were
   * used in the join
   */
  @Nonnull
  public static DatasetWithColumns joinOnColumns(@Nonnull final Dataset<Row> left,
      @Nonnull final List<Column> leftColumns,
      final Dataset<Row> right, @Nonnull final List<Column> rightColumns,
      @Nonnull final JoinType joinType) {
    check(leftColumns.size() == rightColumns.size());
    // Don't do unnecessary joins between identical datasets.
    if (left.equals(right)) {
      return new DatasetWithColumns(left, leftColumns);
    }

    @Nullable Column joinCondition = null;
    final Dataset<Row> leftAliased = left.as("left");
    final Dataset<Row> rightAliased = right.as("right");

    for (int i = 0; i < leftColumns.size(); i++) {
      final Column leftColumn = leftAliased.col("left." + leftColumns.get(i));
      final Column rightColumn = rightAliased.col("right." + rightColumns.get(i));
      // We need to do an explicit null check here, otherwise the join will nullify the result of 
      // the aggregation when the grouping value is null.
      final Column columnsEqual = leftColumn.isNull().and(rightColumn.isNull())
          .or(leftColumn.equalTo(rightColumn));
      joinCondition = i == 0
                      ? columnsEqual
                      : joinCondition.and(columnsEqual);
    }

    final Dataset<Row> dataset = left.join(right, joinCondition, joinType.getSparkName());
    return new DatasetWithColumns(dataset, leftColumns);
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
    @Nullable final Column reference = left.getValueColumn().getField("reference");
    checkNotNull(reference);
    final Column joinCondition = reference.equalTo(rightId);
    final Dataset<Row> target = selectJoinTarget(right, left.getDataset());
    return left.getDataset().join(target, joinCondition, joinType.getSparkName());
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
      @Nonnull final Dataset<Row> right, @Nonnull final Column rightReference,
      @Nonnull final JoinType joinType) {
    check(left.getIdColumn().isPresent());
    @Nullable final Column reference = rightReference.getField("reference");
    checkNotNull(reference);
    final Column joinCondition = left.getIdColumn().get().equalTo(reference);
    final Dataset<Row> target = selectJoinTarget(right, left.getDataset());
    return left.getDataset().join(target, joinCondition, joinType.getSparkName());
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

  /**
   * Represents a type of join that can be made between two {@link Dataset} objects.
   */
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
  @Value
  public static class DatasetWithColumn {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    Column column;

  }

  /**
   * Represents a {@link Dataset} along with a list of {@link Column} objects that refer to columns
   * within the Dataset.
   */
  @Value
  public static class DatasetWithColumns {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    List<Column> columns;

  }

  /**
   * Represents a {@link Dataset} along with a resource identity column and an expression value
   * {@link Column}.
   */
  @Value
  public static class DatasetWithIdAndValue {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    Column idColumn;

    @Nonnull
    Column valueColumn;

  }

  /**
   * A pair of {@link Column} objects that refer to the resource identity and expression value.
   */
  @Value
  public static class IdAndValue {

    @Nonnull
    Optional<Column> idColumn;

    @Nonnull
    Column valueColumn;

  }

}
