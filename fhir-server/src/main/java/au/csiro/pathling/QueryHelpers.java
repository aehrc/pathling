/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Strings.randomShortString;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
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
   * String used at the end of column names used for element identity (hid).
   */
  public static final String EID_COLUMN_SUFFIX = "_eid";

  /**
   * String used at the end of column names used for expression values.
   */
  public static final String VALUE_COLUMN_SUFFIX = "_value";

  /**
   * String used at the end of column names used for resource types.
   */
  public static final String TYPE_COLUMN_SUFFIX = "_type";

  /**
   * String used at the end of column names used this column.
   */
  public static final String THIS_COLUMN_SUFFIX = "_this";

  /**
   * @param dataset a {@link Dataset} representing a raw resource, with at least 2 columns
   * @return a Dataset with two columns: an ID column and a value column containing all of the
   * columns from the original Dataset
   */
  @Nonnull
  public static DatasetWithIdsAndValue convertRawResource(@Nonnull final Dataset<Row> dataset) {
    checkArgument(dataset.columns().length > 1, "dataset has no columns");

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

    return new DatasetWithIdsAndValue(result.select(idColumn, valueColumn), idColumn, valueColumn);
  }

  /**
   * Filters a dataset to only the nominated ID column, plus any pre-existing value columns.
   * Pre-existing ID columns are dropped out.
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
        .filter(column -> column.endsWith(EID_COLUMN_SUFFIX) ||
            column.endsWith(VALUE_COLUMN_SUFFIX) || column.endsWith(TYPE_COLUMN_SUFFIX) || column
            .endsWith(THIS_COLUMN_SUFFIX))
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
   * Joins two {@link FhirPath} expressions in a way that is aware of the fact that they may be
   * grouped, and not have resource identity columns.
   * <p>
   * Also detects the case where two {@code $this} derived paths are being joined, and adds the
   * {@code $this} column to the join condition.
   *
   * @param context the current {@link ParserContext}
   * @param left a {@link FhirPath} expression
   * @param right another FhirPath expression
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final ParserContext context,
      @Nonnull final FhirPath left, @Nonnull final FhirPath right,
      @Nonnull final JoinType joinType) {
    Dataset<Row> leftDataset = left.getDataset();
    Dataset<Row> rightDataset = right.getDataset();

    if (!context.getGroupingColumns().isEmpty()) {
      final DatasetWithColumns datasetWithColumns = joinOnColumns(leftDataset,
          context.getGroupingColumns(), rightDataset,
          context.getGroupingColumns(), joinType);
      return datasetWithColumns.getDataset();
    }

    final Column leftId = checkPresent(left.getIdColumn());
    final Column rightId = checkPresent(right.getIdColumn());
    Column joinCondition = leftId.equalTo(rightId);

    // If both the left and right expressions are not literal and contain $this columns, we need to
    // add equality of the $this columns to the join condition. This is to scope the join to a
    // single element within the input collection.
    if (left instanceof NonLiteralPath && right instanceof NonLiteralPath) {
      final NonLiteralPath nonLiteralLeft = (NonLiteralPath) left;
      final NonLiteralPath nonLiteralRight = (NonLiteralPath) right;
      if (nonLiteralLeft.getThisColumn().isPresent() && nonLiteralRight.getThisColumn()
          .isPresent()) {
        final String leftThisColumnName = randomShortString() + "_value";
        final String rightThisColumnName = randomShortString() + "_value";
        leftDataset = leftDataset
            .withColumn(leftThisColumnName, nonLiteralLeft.getThisColumn().get());
        rightDataset = rightDataset
            .withColumn(rightThisColumnName, nonLiteralRight.getThisColumn().get());
        final Column leftThisColumn = leftDataset.col(leftThisColumnName);
        final Column rightThisColumn = rightDataset.col(rightThisColumnName);
        joinCondition = joinCondition.and(leftThisColumn.equalTo(rightThisColumn));
      }
    }

    final Dataset<Row> target = selectJoinTarget(rightDataset, leftDataset);
    return leftDataset.join(target, joinCondition, joinType.getSparkName());
  }

  /**
   * Joins a {@link Dataset} to a {@link FhirPath}, using resource identity columns.
   * <p>
   * This should not be used in contexts where there may be grouping columns.
   *
   * @param left a {@link Dataset}
   * @param leftId the ID {@link Column} in the left Dataset
   * @param right a {@link FhirPath} expression
   * @param joinType a {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnId(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftId, @Nonnull final FhirPath right,
      @Nonnull final JoinType joinType) {
    return joinOnId(left, leftId, right, Optional.empty(), joinType);
  }

  /**
   * Joins a {@link Dataset} to a {@link FhirPath}, using resource identity columns.
   * <p>
   * This should not be used in contexts where there may be grouping columns.
   *
   * @param left a {@link Dataset}
   * @param leftId the ID {@link Column} in the left Dataset
   * @param right a {@link FhirPath} expression
   * @param additionalCondition an additional condition that will be combined with the ID equality
   * using AND
   * @param joinType a {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnId(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftId, @Nonnull final FhirPath right,
      @Nonnull final Optional<Column> additionalCondition, @Nonnull final JoinType joinType) {
    checkArgument(right.getIdColumn().isPresent(), "Right expression must have an ID column");
    Column joinCondition = leftId.equalTo(right.getIdColumn().get());
    if (additionalCondition.isPresent()) {
      joinCondition = joinCondition.and(additionalCondition.get());
    }
    final Dataset<Row> target = selectJoinTarget(right.getDataset(), left);
    return left.join(target, joinCondition, joinType.getSparkName());
  }

  /**
   * Joins a {@link Dataset} to another Dataset, using resource identity columns.
   * <p>
   * This should not be used in contexts where there may be grouping columns.
   *
   * @param left a {@link Dataset}
   * @param leftId the ID {@link Column} in the left Dataset
   * @param right another Dataset
   * @param rightId the ID column in the right Dataset
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnId(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftId, @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightId, @Nonnull final JoinType joinType) {
    final Column joinCondition = leftId.equalTo(rightId);
    final Dataset<Row> target = selectJoinTarget(right, left);
    return left.join(target, joinCondition, joinType.getSparkName());
  }

  /**
   * Joins two {@link FhirPath} expressions matching the reference (on the left) to the resource
   * identity (on the right).
   *
   * @param left a {@link FhirPath}
   * @param right a {@link FhirPath}
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnReference(@Nonnull final FhirPath left,
      @Nonnull final FhirPath right, @Nonnull final JoinType joinType) {
    final Column rightId = checkPresent(right.getIdColumn());
    return joinOnReference(left, right.getDataset(), rightId, joinType);
  }

  /**
   * Joins a {@link FhirPath} with a {@link Dataset} based on the reference (on the left) matching
   * the resource identity (on the right).
   *
   * @param left a {@link FhirPath} expression
   * @param right a {@link Dataset}
   * @param rightId the ID {@link Column} in the right Dataset
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnReference(@Nonnull final FhirPath left,
      @Nonnull final Dataset<Row> right, @Nonnull final Column rightId,
      @Nonnull final JoinType joinType) {
    @Nullable final Column reference = left.getValueColumn().getField("reference");
    checkNotNull(reference);
    final Column joinCondition = reference.equalTo(rightId);
    final Dataset<Row> target = selectJoinTarget(right, left.getDataset());
    return left.getDataset().join(target, joinCondition, joinType.getSparkName());
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
   * @param additionalCondition an additional condition that will be combined with the column
   * equality using AND
   * @param joinType the type of join to use
   * @return a {@link DatasetWithColumns} containing the joined Dataset and the columns that were
   * used in the join
   */
  @Nonnull
  public static DatasetWithColumns joinOnColumns(@Nonnull final Dataset<Row> left,
      @Nonnull final List<Column> leftColumns, @Nonnull final Dataset<Row> right,
      @Nonnull final List<Column> rightColumns, @Nonnull final Optional<Column> additionalCondition,
      @Nonnull final JoinType joinType) {
    checkArgument(leftColumns.size() == rightColumns.size(),
        "Left columns should be same size as right columns");

    @Nullable Column joinCondition = null;
    Dataset<Row> leftAliased = left;
    Dataset<Row> rightAliased = right;
    final List<Column> newColumns = new ArrayList<>();

    for (int i = 0; i < leftColumns.size(); i++) {
      final String leftHash = randomShortString();
      final String leftColumnName = leftHash + VALUE_COLUMN_SUFFIX;
      final String rightHash = randomShortString();
      final String rightColumnName = rightHash + VALUE_COLUMN_SUFFIX;

      // We keep the old grouping column in one of the datasets, so as to preserve it without ending
      // up with duplicates in the join.
      leftAliased = leftAliased.withColumn(leftColumnName, leftColumns.get(i))
          .drop(leftColumns.get(i));
      rightAliased = rightAliased.withColumn(rightColumnName, rightColumns.get(i));

      final Column leftColumn = leftAliased.col(leftColumnName);
      final Column rightColumn = rightAliased.col(rightColumnName);
      newColumns.add(leftColumn);

      // We need to do an explicit null check here, otherwise the join will nullify the result of 
      // the aggregation when the grouping value is null.
      final Column columnsEqual = leftColumn.isNull().and(rightColumn.isNull())
          .or(leftColumn.equalTo(rightColumn));
      joinCondition = i == 0
                      ? columnsEqual
                      : joinCondition.and(columnsEqual);
    }
    checkNotNull(joinCondition);

    if (additionalCondition.isPresent()) {
      joinCondition = joinCondition.and(additionalCondition.get());
    }

    final Dataset<Row> target = selectJoinTarget(rightAliased, leftAliased);
    final Dataset<Row> dataset = leftAliased
        .join(target, joinCondition, joinType.getSparkName());
    return new DatasetWithColumns(dataset, newColumns);
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
      @Nonnull final List<Column> leftColumns, @Nonnull final Dataset<Row> right,
      @Nonnull final List<Column> rightColumns, @Nonnull final JoinType joinType) {
    return joinOnColumns(left, leftColumns, right, rightColumns, Optional.empty(), joinType);
  }

  /**
   * @param datasets A bunch of {@link Dataset} objects
   * @return A new Dataset that is the union of all the inputs
   */
  @Nonnull
  public static Dataset<Row> union(@Nonnull final Collection<Dataset<Row>> datasets) {
    return datasets.stream()
        .reduce(Dataset::union)
        .orElseThrow();
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
    return Stream.of(dataset.columns())
        .limit(numberOfColumns)
        .map(dataset::col)
        .collect(Collectors.toList());
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
  public static class DatasetWithIdsAndValue {

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
