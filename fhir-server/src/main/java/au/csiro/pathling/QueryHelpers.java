/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Strings.randomShortString;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Referrer;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.Getter;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Common functionality for executing queries using Spark.
 *
 * @author John Grimes
 */
public abstract class QueryHelpers {

  /**
   * Adds to the columns within a {@link Dataset} with an aliased versions of the supplied column.
   *
   * @param dataset the Dataset on which to perform the operation
   * @param column a new {@link Column}
   * @return a new Dataset, along with the new column, as a {@link DatasetWithColumn}
   */
  @Nonnull
  public static DatasetWithColumn aliasColumn(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column column) {
    final DatasetWithColumnMap datasetWithColumnMap = aliasColumns(dataset,
        Collections.singletonList(column), true);
    return new DatasetWithColumn(datasetWithColumnMap.getDataset(),
        datasetWithColumnMap.getColumnMap().get(column));
  }

  /**
   * Adds aliased versions of the supplied columns to a {@link Dataset}.
   *
   * @param dataset the Dataset on which to perform the operation
   * @param columns a list of new {@link Column} objects
   * @param preserveColumns if set to true, preserves the existing columns in the dataset
   * @return a new Dataset, along with a map from the supplied columns to the new columns, as a
   * {@link DatasetWithColumnMap}
   */
  @Nonnull
  public static DatasetWithColumnMap aliasColumns(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Iterable<Column> columns, final boolean preserveColumns) {
    final Map<Column, Column> columnMap = new HashMap<>();
    final Map<String, Column> aliasToSourceColumn = new HashMap<>();
    final List<Column> selection = preserveColumns
                                   ? Stream.of(dataset.columns())
                                       // Column names starting with an underscore are used to
                                       // denote a temporary column that is not retained within the
                                       // dataset beyond the point at which it is converted into a
                                       // aliased column.
                                       .filter(columnName -> !columnName.startsWith("_"))
                                       .map(dataset::col)
                                       .collect(Collectors.toList())
                                   : new ArrayList<>();

    // Create an aliased column for each of the new columns, and add it to the selection.
    for (final Column column : columns) {
      final String alias;
      // If the selection already contains the column, we don't add a new column to the selection.
      if (selection.contains(column)) {
        alias = column.toString();
      } else {
        alias = randomShortString();
        final Column aliasedColumn = column.alias(alias);
        selection.add(aliasedColumn);
      }
      aliasToSourceColumn.put(alias, column);
    }

    // Create a new dataset from the selection.
    final Dataset<Row> result = dataset.select(selection.toArray(new Column[0]));

    // Harvest all of the resolved aliased columns from the new dataset, and create a map between
    // the original source columns and these new columns.
    for (final String alias : aliasToSourceColumn.keySet()) {
      final Column finalColumn = result.col(alias);
      columnMap.put(aliasToSourceColumn.get(alias), finalColumn);
    }

    return new DatasetWithColumnMap(result, columnMap);
  }

  /**
   * Filters a dataset to only the nominated ID column, plus any pre-existing value columns.
   * Pre-existing ID columns are dropped out.
   *
   * @param dataset the dataset to perform the operation upon
   * @param columnDependencies a list of columns to include in the result
   * @param nameFilter a list of column names to exclude from the result
   * @return a new {@link Dataset} with a subset of columns
   */
  @Nonnull
  private static Dataset<Row> applySelection(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Collection<Column> columnDependencies,
      @Nonnull final Collection<String> nameFilter) {
    final Set<Column> selection = Stream.of(dataset.columns())
        .filter(column -> !nameFilter.contains(column))
        .map(dataset::col)
        .collect(Collectors.toSet());
    selection.addAll(columnDependencies);
    return dataset.select(selection.toArray(new Column[0]));
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
    final Dataset<Row> rightDataset = right.getDataset();

    // If there are grouping columns, we need to join on these columns instead.
    if (!context.getGroupingColumns().isEmpty()) {
      final DatasetWithColumns datasetWithColumns = joinOnColumns(left.getDataset(),
          context.getGroupingColumns(), rightDataset,
          context.getGroupingColumns(), joinType);
      return datasetWithColumns.getDataset();
    }

    // Add equality of the ID columns to the join condition.
    final Collection<AliasedColumnPair> joinConditions = new ArrayList<>();
    final String leftIdAlias = randomShortString();
    final String rightIdAlias = randomShortString();
    final Column leftId = checkPresent(left.getIdColumn()).alias(leftIdAlias);
    final Column rightId = checkPresent(right.getIdColumn()).alias(rightIdAlias);
    joinConditions.add(new AliasedColumnPair(leftId, rightId, leftIdAlias, rightIdAlias));

    // If both the left and right expressions are not literal and contain $this columns, we need to
    // add equality of the $this columns to the join condition. This is to scope the join to a
    // single element within the input collection.
    if (left instanceof NonLiteralPath && right instanceof NonLiteralPath) {
      final NonLiteralPath nonLiteralLeft = (NonLiteralPath) left;
      final NonLiteralPath nonLiteralRight = (NonLiteralPath) right;
      if (nonLiteralLeft.getThisColumns().isPresent() && nonLiteralRight.getThisColumns()
          .isPresent()) {
        final List<Column> leftThisColumns = checkPresent(nonLiteralLeft.getThisColumns());
        final List<Column> rightThisColumns = checkPresent(nonLiteralRight.getThisColumns());
        check(leftThisColumns.size() == rightThisColumns.size());

        for (int i = 0; i < leftThisColumns.size(); i++) {
          final String leftAlias = randomShortString();
          final String rightAlias = randomShortString();
          final Column leftThisColumn = leftThisColumns.get(i).alias(leftAlias);
          final Column rightThisColumn = rightThisColumns.get(i).alias(rightAlias);
          joinConditions
              .add(new AliasedColumnPair(leftThisColumn, rightThisColumn, leftAlias, rightAlias));
        }
      }
    }

    // Create new datasets that include all of the original columns, plus the new aliased columns.
    final Dataset<Row> finalLeft = applySelection(left.getDataset(), joinConditions.stream()
        .map(AliasedColumnPair::getLeft));
    final Stream<Column> finalLeftColumns = Stream.of(finalLeft.columns()).map(finalLeft::col);
    final Dataset<Row> finalRight = applySelection(rightDataset, joinConditions.stream()
        .map(AliasedColumnPair::getRight), finalLeftColumns);

    final Column joinCondition = joinConditions.stream()
        .map(aliased -> new AliasedColumnPair(
            finalLeft.col(aliased.getLeftAlias()),
            finalRight.col(aliased.getRightAlias()),
            aliased.getLeftAlias(), aliased.getRightAlias()))
        .map(resolved -> resolved.getLeft().equalTo(resolved.getRight()))
        .reduce(Column::and)
        .orElseThrow();

    final Dataset<Row> result = finalLeft
        .join(finalRight, joinCondition, joinType.getSparkName());
    final Set<Column> selection = Stream.of(left.getDataset().columns()).map(left.getDataset()::col)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    selection.addAll(
        Stream.of(rightDataset.columns()).map(rightDataset::col).collect(Collectors.toList()));
    return result.select(selection.toArray(new Column[0]));
  }

  @Nonnull
  private static Dataset<Row> applySelection(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Stream<Column> columnStream, @Nonnull final Stream<Column> excludes) {
    final List<Column> selection = Stream.of(dataset.columns())
        .map(dataset::col)
        .collect(Collectors.toList());
    selection.addAll(columnStream
        .collect(Collectors.toList()));
    selection.removeAll(excludes.collect(Collectors.toList()));
    return dataset.select(selection.toArray(new Column[0]));
  }

  @Nonnull
  private static Dataset<Row> applySelection(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Stream<Column> columnStream) {
    return applySelection(dataset, columnStream, Stream.empty());
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
    return left.join(right.getDataset(), joinCondition, joinType.getSparkName());
  }

  /**
   * Joins a {@link Dataset} to a {@link FhirPath}, using resource identity columns.
   * <p>
   * This should not be used in contexts where there may be grouping columns.
   *
   * @param left a {@link FhirPath} expression
   * @param right a {@link Dataset}
   * @param rightId the ID {@link Column} in the right Dataset
   * @param additionalCondition an additional condition that will be combined with the ID equality
   * using AND
   * @param joinType a {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnId(@Nonnull final FhirPath left,
      @Nonnull final Dataset<Row> right, @Nonnull final Column rightId,
      @Nonnull final Optional<Column> additionalCondition, @Nonnull final JoinType joinType) {
    checkArgument(left.getIdColumn().isPresent(), "Left expression must have an ID column");
    Column joinCondition = left.getIdColumn().get().equalTo(rightId);
    if (additionalCondition.isPresent()) {
      joinCondition = joinCondition.and(additionalCondition.get());
    }
    return left.getDataset().join(right, joinCondition, joinType.getSparkName());
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
    return left.join(right, joinCondition, joinType.getSparkName());
  }

  /**
   * Joins two {@link FhirPath} expressions matching the reference (on the left) to the resource
   * identity (on the right).
   *
   * @param left a {@link ReferencePath}
   * @param right a {@link FhirPath}
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnReference(@Nonnull final Referrer left,
      @Nonnull final FhirPath right, @Nonnull final JoinType joinType) {
    final Column rightId = checkPresent(right.getIdColumn());
    return joinOnReference(left, right.getDataset(), rightId, joinType);
  }

  /**
   * Joins a {@link FhirPath} with a {@link Dataset} based on the reference (on the left) matching
   * the resource identity (on the right).
   *
   * @param left a {@link ReferencePath} expression
   * @param right a {@link Dataset}
   * @param rightId the ID {@link Column} in the right Dataset
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> joinOnReference(@Nonnull final Referrer left,
      @Nonnull final Dataset<Row> right, @Nonnull final Column rightId,
      @Nonnull final JoinType joinType) {
    @Nullable final Column reference = left.getValueColumn().getField("reference");
    checkNotNull(reference);
    final Column joinCondition = reference.equalTo(rightId);
    return left.getDataset().join(right, joinCondition, joinType.getSparkName());
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
    final Collection<AliasedColumnPair> joinConditions = new ArrayList<>();

    // Collect a join condition for each pair of matched columns within the left and right datasets.
    for (int i = 0; i < leftColumns.size(); i++) {
      final String leftColumnAlias = randomShortString();
      final String rightColumnAlias = randomShortString();
      final Column leftColumn = leftColumns.get(i).alias(leftColumnAlias);
      final Column rightColumn = rightColumns.get(i).alias(rightColumnAlias);
      joinConditions
          .add(new AliasedColumnPair(leftColumn, rightColumn, leftColumnAlias, rightColumnAlias));
    }

    // Create new datasets that include all of the original columns, plus the new aliased columns.
    final Dataset<Row> finalLeft = applySelection(left, joinConditions.stream()
        .map(AliasedColumnPair::getLeft));
    final Stream<Column> finalLeftColumns = Stream.of(finalLeft.columns()).map(finalLeft::col);
    final Dataset<Row> finalRight = applySelection(right, joinConditions.stream()
        .map(AliasedColumnPair::getRight), finalLeftColumns);

    // Create the join condition by combining all the collected conditions using and (and subject to
    // null checks).
    Column joinCondition = joinConditions.stream()
        .map(aliased -> new AliasedColumnPair(
            finalLeft.col(aliased.getLeftAlias()),
            finalRight.col(aliased.getRightAlias()),
            aliased.getLeftAlias(), aliased.getRightAlias()))
        .map(resolved -> {
          // We need to do an explicit null check here, otherwise the join will nullify the result
          // of the aggregation when the grouping value is null.
          return resolved.getLeft().isNull().and(resolved.getRight().isNull())
              .or(resolved.getLeft().equalTo(resolved.getRight()));
        })
        .reduce(Column::and)
        .orElseThrow();

    if (additionalCondition.isPresent()) {
      joinCondition = joinCondition.and(additionalCondition.get());
    }

    final Dataset<Row> dataset = finalLeft.join(finalRight, joinCondition, joinType.getSparkName());
    final Set<Column> selection = Stream.of(left.columns()).map(left::col)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    selection.addAll(
        Stream.of(right.columns()).map(right::col).collect(Collectors.toList()));
    final Dataset<Row> finalDataset = dataset.select(selection.toArray(new Column[0]));
    return new DatasetWithColumns(finalDataset, leftColumns);
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
  private static class DatasetWithColumn {

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
   * Represents a {@link Dataset} along with a map between two sets of columns.
   */
  @Value
  public static class DatasetWithColumnMap {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    Map<Column, Column> columnMap;

    /**
     * @param sourceColumns a collection of {@link Column} objects present in the source of the map
     * @return the corresponding target columns
     */
    @Nonnull
    public List<Column> getMappedColumns(@Nonnull final Collection<Column> sourceColumns) {
      return sourceColumns.stream()
          .map(columnMap::get)
          .collect(Collectors.toList());
    }

  }

  @Data
  private static class AliasedColumnPair {

    @Nonnull
    Column left;

    @Nonnull
    Column right;

    @Nonnull
    String leftAlias;

    @Nonnull
    String rightAlias;

  }

}
