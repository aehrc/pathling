/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.utilities.Strings;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
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
   * Adds to the columns within a {@link Dataset} with an aliased version of the supplied column.
   *
   * @param dataset the Dataset on which to perform the operation
   * @param column a new {@link Column}
   * @return a new Dataset, along with the new column name, as a {@link DatasetWithColumn}
   */
  @Nonnull
  public static DatasetWithColumn createColumn(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column column) {
    final DatasetWithColumnMap datasetWithColumnMap = aliasColumns(dataset,
        Collections.singletonList(column));
    return new DatasetWithColumn(datasetWithColumnMap.getDataset(),
        datasetWithColumnMap.getColumnMap().get(column));
  }

  /**
   * Adds to the columns within a {@link Dataset} with aliased versions of the supplied columns.
   *
   * @param dataset the Dataset on which to perform the operation
   * @param columns the new {@link Column} objects
   * @return a new Dataset, along with the new column names, as a {@link DatasetWithColumnMap}
   */
  @Nonnull
  public static DatasetWithColumnMap createColumns(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column... columns) {
    return aliasColumns(dataset, Arrays.asList(columns));
  }

  /**
   * Replaces all unaliased columns within a {@link Dataset} with new aliased columns.
   *
   * @param dataset the Dataset on which to perform the operation
   * @return a new Dataset, with a mapping from the old columns to the new as a {@link
   * DatasetWithColumnMap}
   */
  @Nonnull
  public static DatasetWithColumnMap aliasAllColumns(@Nonnull final Dataset<Row> dataset) {

    final List<Column> columns = Stream.of(dataset.columns())
        .map(dataset::col)
        .collect(Collectors.toList());
    final DatasetWithColumnMap datasetWithColumnMap = aliasColumns(dataset, columns);

    final Dataset<Row> finalDataset = datasetWithColumnMap.getDataset();
    final Map<Column, Column> columnMap = datasetWithColumnMap.getColumnMap();
    return new DatasetWithColumnMap(finalDataset, columnMap);
  }

  /**
   * Adds aliased versions of the supplied columns to a {@link Dataset}.
   *
   * @param dataset the Dataset on which to perform the operation
   * @param columns a list of new {@link Column} objects
   * @return a new Dataset, along with a map from the supplied columns to the new columns, as a
   * {@link DatasetWithColumnMap}
   */
  @Nonnull
  private static DatasetWithColumnMap aliasColumns(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Iterable<Column> columns) {

    final Map<Column, Column> columnMap = new HashMap<>();
    final List<Column> selection = Stream.of(dataset.columns())
        // Don't preserve anything that is not already aliased.
        .filter(Strings::looksLikeAlias)
        .map(dataset::col)
        .collect(Collectors.toList());

    // Create an aliased column for each of the new columns, and add it to the selection and the
    // map.
    for (final Column column : columns) {
      final String alias = randomAlias();
      final Column aliasedColumn = column.alias(alias);
      selection.add(aliasedColumn);
      columnMap.put(column, col(alias));
    }

    // Create a new dataset from the selection.
    final Dataset<Row> result = dataset.select(selection.toArray(new Column[0]));

    return new DatasetWithColumnMap(result, columnMap);
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
   * @return the joined Dataset
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final Dataset<Row> left,
      @Nonnull final List<Column> leftColumns, @Nonnull final Dataset<Row> right,
      @Nonnull final List<Column> rightColumns, @Nonnull final JoinType joinType) {
    checkArgument(leftColumns.size() == rightColumns.size(),
        "Left columns should be same size as right columns");

    Dataset<Row> aliasedLeft = left;
    final Collection<Column> joinConditions = new ArrayList<>();
    for (int i = 0; i < leftColumns.size(); i++) {
      final DatasetWithColumn leftWithColumn = createColumn(aliasedLeft, leftColumns.get(i));
      aliasedLeft = leftWithColumn.getDataset();
      joinConditions.add(leftWithColumn.getColumn().equalTo(rightColumns.get(i)));
    }
    final Column joinCondition = joinConditions.stream()
        .reduce(Column::and)
        .orElseThrow();

    // Preserve the columns within the join conditions within the right dataset.
    final List<String> rightColumnNames = rightColumns.stream()
        .map(Column::toString)
        .collect(Collectors.toList());

    // Exclude the columns on the right hand side of the join conditions from the left dataset.
    final Dataset<Row> trimmedLeft = applySelection(aliasedLeft, Collections.emptyList(),
        rightColumnNames);

    // The right dataset will only contain columns that were not in the left dataset, with the 
    // exception of the columns on the right hand side of the join conditions.
    final Dataset<Row> trimmedRight = applySelection(right, rightColumnNames,
        Arrays.asList(aliasedLeft.columns()));

    return trimmedLeft.join(trimmedRight, joinCondition, joinType.getSparkName());
  }

  /**
   * Joins a {@link Dataset} to another Dataset, using the equality of two columns.
   *
   * @param left a {@link Dataset}
   * @param leftColumn the {@link Column} in the left Dataset
   * @param right another Dataset
   * @param rightColumn the column in the right Dataset
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftColumn, @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightColumn, @Nonnull final JoinType joinType) {
    return join(left, Collections.singletonList(leftColumn), right,
        Collections.singletonList(rightColumn), joinType);
  }

  /**
   * Joins two {@link FhirPath} expressions, using equality between their respective resource ID
   * columns.
   *
   * @param left a {@link FhirPath} expression
   * @param right another FhirPath expression
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final FhirPath left, @Nonnull final FhirPath right,
      @Nonnull final JoinType joinType) {
    return join(left.getDataset(), left.getIdColumn(), right.getDataset(), right.getIdColumn(),
        joinType);
  }

  /**
   * Joins a {@link Dataset} to a {@link FhirPath}, using equality between the resource ID in the
   * FhirPath and the supplied column.
   *
   * @param left a {@link FhirPath} expression
   * @param right a {@link Dataset}
   * @param rightColumn the {@link Column} in the right Dataset
   * @param joinType a {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final FhirPath left, @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightColumn, @Nonnull final JoinType joinType) {
    return join(left.getDataset(), left.getIdColumn(), right, rightColumn, joinType);
  }

  /**
   * Joins a {@link Dataset} to a {@link FhirPath}, using equality between the resource ID in the
   * FhirPath and the supplied column.
   *
   * @param left a {@link Dataset}
   * @param leftColumn the {@link Column} in the left Dataset
   * @param right a {@link FhirPath} expression
   * @param joinType a {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftColumn, @Nonnull final FhirPath right,
      @Nonnull final JoinType joinType) {
    return join(left, leftColumn, right.getDataset(), right.getIdColumn(), joinType);
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

  @Nonnull
  private static Dataset<Row> applySelection(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Collection<String> includes, @Nonnull final Collection<String> excludes) {
    return dataset.select(Stream.of(dataset.columns())
        .filter(column -> includes.contains(column) || !excludes.contains(column))
        .map(dataset::col)
        .toArray(Column[]::new));
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
   * Represents a {@link Dataset} along with a map between two sets of columns.
   */
  @Value
  public static class DatasetWithColumnMap {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    Map<Column, Column> columnMap;

    @Nonnull
    public Column getColumn(@Nonnull Column originalColumn) {
      return columnMap.get(originalColumn);
    }

  }

}
