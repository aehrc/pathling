/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.utilities.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import jakarta.annotation.Nonnull;
import lombok.Getter;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

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
   * @return a new Dataset, with a mapping from the old columns to the new as a
   * {@link DatasetWithColumnMap}
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

    // Use LinkedHashMap to preserve the original order of columns while iterating map entries.
    final Map<Column, Column> columnMap = new LinkedHashMap<>();
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

  private static Dataset<Row> join(@Nonnull final Dataset<Row> left,
      @Nonnull final List<Column> leftColumns, @Nonnull final Dataset<Row> right,
      @Nonnull final List<Column> rightColumns, @Nonnull final Optional<Column> additionalCondition,
      @Nonnull final JoinType joinType) {
    checkArgument(leftColumns.size() == rightColumns.size(),
        "Left columns should be same size as right columns");

    Dataset<Row> aliasedLeft = left;
    final Collection<Column> joinConditions = new ArrayList<>();
    for (int i = 0; i < leftColumns.size(); i++) {
      // We alias the join columns on the left-hand side to disambiguate them from columns named the
      // same on the right-hand side.
      final DatasetWithColumn leftWithColumn = createColumn(aliasedLeft, leftColumns.get(i));
      aliasedLeft = leftWithColumn.getDataset();
      joinConditions.add(leftWithColumn.getColumn().eqNullSafe(rightColumns.get(i)));
    }
    additionalCondition.ifPresent(joinConditions::add);
    final Column joinCondition = joinConditions.stream()
        .reduce(Column::and)
        .orElse(lit(true));

    final List<String> leftColumnNames = Arrays.asList(aliasedLeft.columns());
    final List<String> rightColumnNames = rightColumns.stream()
        .map(Column::toString)
        .collect(Collectors.toList());

    // Exclude the columns in the right dataset from the trimmed left dataset.
    final Dataset<Row> trimmedLeft = applySelection(aliasedLeft, Collections.emptyList(),
        rightColumnNames);

    // The right dataset will only contain columns that were not in the left dataset, except for the 
    // columns on the right-hand side of the join conditions.
    final Dataset<Row> trimmedRight = applySelection(right, rightColumnNames, leftColumnNames);

    return trimmedLeft.join(trimmedRight, joinCondition, joinType.getSparkName());
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
    return join(left, leftColumns, right, rightColumns, Optional.empty(), joinType);
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
   * Joins a {@link Dataset} to another Dataset, using the equality of two columns.
   *
   * @param left a {@link Dataset}
   * @param leftColumn the {@link Column} in the left Dataset
   * @param right another Dataset
   * @param rightColumn the column in the right Dataset
   * @param additionalCondition an additional Column to be added to the join condition, using AND
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final Dataset<Row> left,
      @Nonnull final Column leftColumn, @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightColumn, @Nonnull final Column additionalCondition,
      @Nonnull final JoinType joinType) {
    return join(left, Collections.singletonList(leftColumn), right,
        Collections.singletonList(rightColumn), Optional.of(additionalCondition), joinType);
  }

  /**
   * Joins a {@link Dataset} to another Dataset, using the equality of two columns.
   *
   * @param left a {@link Dataset}
   * @param leftColumns the columns for the first Dataset
   * @param right another Dataset
   * @param rightColumns the columns for the second Dataset
   * @param additionalCondition an additional Column to be added to the join condition, using AND
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final Dataset<Row> left,
      @Nonnull final List<Column> leftColumns, @Nonnull final Dataset<Row> right,
      @Nonnull final List<Column> rightColumns, @Nonnull final Column additionalCondition,
      @Nonnull final JoinType joinType) {
    return join(left, leftColumns, right, rightColumns, Optional.of(additionalCondition), joinType);
  }

  /**
   * Joins a {@link Dataset} to another Dataset, using a custom join condition.
   *
   * @param left a {@link Dataset}
   * @param right another Dataset
   * @param joinCondition a custom join condition
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final Dataset<Row> left,
      @Nonnull final Dataset<Row> right, @Nonnull final Column joinCondition,
      @Nonnull final JoinType joinType) {
    return join(left, Collections.emptyList(), right, Collections.emptyList(),
        Optional.of(joinCondition), joinType);
  }

  /**
   * Joins two {@link FhirPath} expressions, using equality between their respective resource ID
   * columns.
   *
   * @param parserContext the current {@link ParserContext}
   * @param left a {@link FhirPath} expression
   * @param right another FhirPath expression
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final ParserContext parserContext,
      @Nonnull final FhirPath left, @Nonnull final FhirPath right,
      @Nonnull final JoinType joinType) {
    return join(parserContext, Arrays.asList(left, right), joinType);
  }

  /**
   * Joins any number of {@link FhirPath} expressions, using equality between their respective
   * resource ID columns.
   *
   * @param parserContext the current {@link ParserContext}
   * @param fhirPaths a list of {@link FhirPath} expressions
   * @param joinType a {@link JoinType}
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final ParserContext parserContext,
      @Nonnull final List<FhirPath> fhirPaths, @Nonnull final JoinType joinType) {
    checkArgument(fhirPaths.size() > 1, "fhirPaths must contain more than one FhirPath");

    final FhirPath left = fhirPaths.get(0);
    final List<FhirPath> joinTargets = fhirPaths.subList(1, fhirPaths.size());

    // Only non-literal paths will trigger a join.
    final List<FhirPath> nonLiteralTargets = joinTargets.stream()
        .filter(t -> t instanceof NonLiteralPath)
        .collect(Collectors.toList());
    if (left instanceof NonLiteralPath && nonLiteralTargets.isEmpty()) {
      // If the only non-literal path is on the left, we can just return the left without any need 
      // to join.
      return left.getDataset();
    } else if (left instanceof LiteralPath && !nonLiteralTargets.isEmpty()) {
      // If non-literal paths are confined to the right, we can just return the first dataset on the  
      // right without any need to join.
      return nonLiteralTargets.get(0).getDataset();
    }

    Dataset<Row> dataset = left.getDataset();
    final List<Column> groupingColumns = parserContext.getGroupingColumns();
    final Column idColumn = parserContext.getInputContext().getIdColumn();
    final List<Column> leftColumns = checkColumnsAndFallback(left.getDataset(), groupingColumns,
        idColumn);
    for (final FhirPath right : nonLiteralTargets) {
      final List<Column> resolvedGroupingColumns = checkColumnsAndFallback(right.getDataset(),
          leftColumns, idColumn);
      dataset = join(dataset, resolvedGroupingColumns, right.getDataset(), resolvedGroupingColumns,
          joinType);
    }
    return dataset;
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
   * @param left a {@link FhirPath} expression
   * @param right a {@link Dataset}
   * @param rightColumn the {@link Column} in the right Dataset
   * @param additionalCondition an additional Column to be added to the join condition, using AND
   * @param joinType a {@link JoinType}
   * @return A new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> join(@Nonnull final FhirPath left, @Nonnull final Dataset<Row> right,
      @Nonnull final Column rightColumn, @Nonnull final Column additionalCondition,
      @Nonnull final JoinType joinType) {
    return join(left.getDataset(), Collections.singletonList(left.getIdColumn()), right,
        Collections.singletonList(rightColumn), Optional.of(additionalCondition), joinType);
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
   * This is used to find a set of fallback join columns in cases where a path does not contain all
   * grouping columns.
   * <p>
   * This can happen in the context of a function's arguments, when a path originates from something
   * other than `$this`, e.g. `%resource`.
   */
  private static List<Column> checkColumnsAndFallback(@Nonnull final Dataset<Row> dataset,
      @Nonnull final List<Column> groupingColumns, @Nonnull final Column fallback) {
    final Set<String> columnList = new HashSet<>(List.of(dataset.columns()));
    final Set<String> groupingColumnNames = groupingColumns.stream().map(Column::toString)
        .collect(Collectors.toSet());
    if (columnList.containsAll(groupingColumnNames)) {
      return groupingColumns;
    } else {
      final Set<String> fallbackGroupingColumnNames = new HashSet<>(groupingColumnNames);
      fallbackGroupingColumnNames.retainAll(columnList);
      fallbackGroupingColumnNames.add(fallback.toString());
      return fallbackGroupingColumnNames.stream().map(dataset::col).collect(Collectors.toList());
    }
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

  @Nonnull
  public static List<Column> getUnionableColumns(@Nonnull final FhirPath source,
      @Nonnull final FhirPath target) {
    // The columns will be those common to both datasets, plus the value column.
    final Set<String> commonColumnNames = new HashSet<>(List.of(source.getDataset().columns()));
    commonColumnNames.retainAll(List.of(target.getDataset().columns()));
    final List<Column> selection = commonColumnNames.stream()
        .map(functions::col)
        // We sort the columns so that they line up when we execute the union.
        .sorted(Comparator.comparing(Column::toString))
        .collect(Collectors.toList());
    selection.add(source.getValueColumn());
    return selection;
  }

  /**
   * Creates an empty dataset with the schema of the supplied resource type.
   *
   * @param spark a {@link SparkSession}
   * @param fhirEncoders a {@link FhirEncoders} object
   * @param resourceType the {@link ResourceType} that will determine the shape of the empty
   * dataset
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> createEmptyDataset(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final ResourceType resourceType) {
    final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    return spark.emptyDataset(encoder).toDF();
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

    /**
     * @param originalColumn a Column on the left-hand side of the Column map
     * @return the corresponding target Column
     */
    @Nonnull
    public Column getColumn(@Nonnull final Column originalColumn) {
      return columnMap.get(originalColumn);
    }

  }

}
