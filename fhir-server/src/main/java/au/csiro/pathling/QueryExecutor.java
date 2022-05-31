/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import ca.uhn.fhir.context.FhirContext;
import lombok.Getter;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.utilities.Preconditions.*;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;

/**
 * Contains functionality common to query executors.
 *
 * @author John Grimes
 */
@Getter
public abstract class QueryExecutor {

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final Database database;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  protected QueryExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final Database database,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.database = database;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  protected ParserContext buildParserContext(@Nonnull final FhirPath inputContext,
      @Nonnull final List<Column> groupingColumns) {
    return new ParserContext(inputContext, fhirContext, sparkSession, database,
        terminologyServiceFactory, groupingColumns, new HashMap<>());
  }

  @Nonnull
  protected List<FhirPathAndContext> parseMaterializableExpressions(
      @Nonnull final ParserContext parserContext, @Nonnull final Collection<String> expressions,
      @Nonnull final String display) {
    return expressions.stream()
        .map(expression -> {
          final ParserContext currentContext = new ParserContext(parserContext.getInputContext(),
              parserContext.getFhirContext(), parserContext.getSparkSession(),
              parserContext.getDatabase(), parserContext.getTerminologyServiceFactory(),
              parserContext.getGroupingColumns(), new HashMap<>());
          final Parser parser = new Parser(currentContext);
          final FhirPath result = parser.parse(expression);
          // Each expression must evaluate to a Materializable path, or a user error will be thrown.
          // There is no requirement for it to be singular.
          checkUserInput(result instanceof Materializable,
              display + " expression is not of a supported type: " + expression);
          return new FhirPathAndContext(result, parser.getContext());
        }).collect(Collectors.toList());
  }

  @Nonnull
  protected List<FhirPath> parseFilters(@Nonnull final Parser parser,
      @Nonnull final Collection<String> filters) {
    return filters.stream().map(expression -> {
      final FhirPath result = parser.parse(expression);
      // Each filter expression must evaluate to a singular Boolean value, or a user error will be
      // thrown.
      checkUserInput(result instanceof BooleanPath,
          "Filter expression is not a non-literal boolean: " + expression);
      checkUserInput(result.isSingular(),
          "Filter expression must represent a singular value: " + expression);
      return result;
    }).collect(Collectors.toList());
  }

  @Nonnull
  protected Dataset<Row> joinExpressionsAndFilters(final FhirPath inputContext,
      @Nonnull final Collection<FhirPath> expressions,
      @Nonnull final Collection<FhirPath> filters, @Nonnull final Column idColumn) {

    // We need to remove any trailing null values from non-empty collections, so that aggregations do
    // not count non-empty collections in the empty collection grouping.
    // We start from the inputContext's dataset and then outer join subsequent expressions datasets
    // where the value is not null.
    final Dataset<Row> combinedGroupings = expressions.stream()
        .map(expr -> expr.getDataset().filter(expr.getValueColumn().isNotNull()))
        // the use of RIGHT_OUTER join seems to be necessary to preserve the original
        // id column in the result
        .reduce(inputContext.getDataset(),
            ((result, element) -> join(element, idColumn, result, idColumn, JoinType.RIGHT_OUTER)));

    return filters.stream()
        .map(FhirPath::getDataset)
        .reduce(combinedGroupings,
            ((result, element) -> join(element, idColumn, result, idColumn, JoinType.RIGHT_OUTER)));
  }


  /**
   * Joins the datasets in a list together the provided set of shared columns.
   *
   * @param expressions a list of expressions to join
   * @param joinColumns the columns to join by; all columns must be present in all expressions.
   * @return the joined {@link Dataset}
   */
  @Nonnull
  protected static Dataset<Row> joinExpressionsByColumns(
      @Nonnull final Collection<FhirPath> expressions, @Nonnull final List<Column> joinColumns) {
    checkArgument(!expressions.isEmpty(), "expressions must not be empty");

    final Optional<Dataset<Row>> maybeJoinResult = expressions.stream()
        .map(FhirPath::getDataset)
        .reduce((l, r) -> join(l, joinColumns, r, joinColumns, JoinType.LEFT_OUTER));
    return maybeJoinResult.orElseThrow();
  }

  @Nonnull
  protected static Dataset<Row> applyFilters(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Collection<FhirPath> filters) {
    // Get the value column from each filter expression, and combine them with AND logic.
    return filters.stream()
        .map(FhirPath::getValueColumn)
        .reduce(Column::and)
        .flatMap(filter -> Optional.of(dataset.filter(filter)))
        .orElse(dataset);
  }

  protected Dataset<Row> filterDataset(@Nonnull final ResourcePath inputContext,
      @Nonnull final Collection<String> filters, @Nonnull final Dataset<Row> dataset,
      @Nonnull final BinaryOperator<Column> operator) {
    return filterDataset(inputContext, filters, dataset, inputContext.getIdColumn(), operator);
  }

  protected Dataset<Row> filterDataset(@Nonnull final ResourcePath inputContext,
      @Nonnull final Collection<String> filters, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final BinaryOperator<Column> operator) {
    final Dataset<Row> filteredDataset;
    if (filters.isEmpty()) {
      filteredDataset = dataset;
    } else {
      final DatasetWithColumn filteredIdsResult = getFilteredIds(filters, inputContext, operator);
      final Dataset<Row> filteredIds = filteredIdsResult.getDataset();
      final Column filteredIdColumn = filteredIdsResult.getColumn();
      filteredDataset = dataset.join(filteredIds,
          idColumn.equalTo(filteredIdColumn), "left_semi");
    }
    return filteredDataset;
  }

  @Nonnull
  private DatasetWithColumn getFilteredIds(@Nonnull final Iterable<String> filters,
      @Nonnull final ResourcePath inputContext, @Nonnull final BinaryOperator<Column> operator) {
    ResourcePath currentContext = inputContext;
    @Nullable Column filterColumn = null;

    for (final String filter : filters) {
      // Parse the filter expression.
      final ParserContext parserContext = buildParserContext(currentContext,
          Collections.singletonList(currentContext.getIdColumn()));
      final Parser parser = new Parser(parserContext);
      final FhirPath fhirPath = parser.parse(filter);

      // Check that it is a Boolean expression.
      checkUserInput(fhirPath instanceof BooleanPath || fhirPath instanceof BooleanLiteralPath,
          "Filter expression must be of Boolean type: " + fhirPath.getExpression());

      // Add the filter column to the overall filter expression using the supplied operator.
      final Column filterValue = fhirPath.getValueColumn();
      filterColumn = filterColumn == null
                     ? filterValue
                     : operator.apply(filterColumn, filterValue);

      // Update the context to build the next expression from the same dataset.
      currentContext = currentContext
          .copy(currentContext.getExpression(), fhirPath.getDataset(), currentContext.getIdColumn(),
              currentContext.getEidColumn(), currentContext.getValueColumn(),
              currentContext.isSingular(), currentContext.getThisColumn());
    }
    checkNotNull(filterColumn);

    // Return a dataset of filtered IDs with an aliased ID column, ready for joining.
    final String filterIdAlias = randomAlias();
    final Dataset<Row> dataset = currentContext.getDataset().select(
        currentContext.getIdColumn().alias(filterIdAlias));
    return new DatasetWithColumn(dataset.filter(filterColumn), col(filterIdAlias));
  }

  @Nonnull
  protected FhirPathContextAndResult joinColumns(
      @Nonnull final Collection<FhirPathAndContext> columnsAndContexts) {
    // Sort the columns in descending order of expression length.
    final List<FhirPathAndContext> sortedColumnsAndContexts = columnsAndContexts.stream()
        .sorted(Comparator.<FhirPathAndContext>comparingInt(p ->
            p.getFhirPath().getExpression().length()).reversed())
        .collect(Collectors.toList());

    FhirPathContextAndResult result = null;
    check(sortedColumnsAndContexts.size() > 0);
    for (final FhirPathAndContext current : sortedColumnsAndContexts) {
      if (result != null) {
        // Get the set of unique prefixes from the two parser contexts, and sort them in descending
        // order of prefix length.
        final Set<String> prefixes = new HashSet<>();
        final Map<String, Column> resultNodeIds = result.getContext().getNodeIdColumns();
        final Map<String, Column> currentNodeIds = current.getContext().getNodeIdColumns();
        prefixes.addAll(resultNodeIds.keySet());
        prefixes.addAll(currentNodeIds.keySet());
        final List<String> sortedCommonPrefixes = new ArrayList<>(prefixes).stream()
            .sorted(Comparator.comparingInt(String::length).reversed())
            .collect(Collectors.toList());
        final FhirPathContextAndResult finalResult = result;

        // Find the longest prefix that is common to the two expressions.
        final Optional<String> commonPrefix = sortedCommonPrefixes.stream()
            .filter(p -> finalResult.getFhirPath().getExpression().startsWith(p) &&
                current.getFhirPath().getExpression().startsWith(p))
            .findFirst();

        if (commonPrefix.isPresent() &&
            resultNodeIds.containsKey(commonPrefix.get()) &&
            currentNodeIds.containsKey(commonPrefix.get())) {
          // If there is a common prefix, we add the corresponding node identifier column to the
          // join condition.
          final Column previousNodeId = resultNodeIds
              .get(commonPrefix.get());
          final List<Column> previousJoinColumns = Arrays.asList(result.getFhirPath().getIdColumn(),
              previousNodeId);
          final Column currentNodeId = currentNodeIds
              .get(commonPrefix.get());
          final List<Column> currentJoinColumns = Arrays.asList(current.getFhirPath().getIdColumn(),
              currentNodeId);
          final Dataset<Row> dataset = join(result.getResult(), previousJoinColumns,
              current.getFhirPath().getDataset(),
              currentJoinColumns, JoinType.LEFT_OUTER);
          result = new FhirPathContextAndResult(current.getFhirPath(), current.getContext(),
              dataset);
        } else {
          // If there is no common prefix, we join using only the resource ID.
          final Dataset<Row> dataset = join(result.getResult(), result.getFhirPath().getIdColumn(),
              current.getFhirPath().getDataset(), current.getFhirPath().getIdColumn(),
              JoinType.LEFT_OUTER);
          result = new FhirPathContextAndResult(current.getFhirPath(), current.getContext(),
              dataset);
        }
      } else {
        result = new FhirPathContextAndResult(current.getFhirPath(), current.getContext(),
            current.getFhirPath().getDataset());
      }
    }

    return result;
  }

  @Nonnull
  protected Dataset<Row> trimTrailingNulls(@Nonnull final ParserContext parserContext,
      final @Nonnull Column idColumn, @Nonnull final List<FhirPath> expressions,
      @Nonnull final Dataset<Row> dataset) {
    checkArgument(!expressions.isEmpty(), "At least one expression is required");

    final Column[] nonSingularColumns = expressions.stream()
        .filter(fhirPath -> !fhirPath.isSingular())
        .map(FhirPath::getValueColumn)
        .toArray(Column[]::new);

    if (nonSingularColumns.length == 0) {
      return dataset;
    } else {
      final Column additionalCondition = coalesce(nonSingularColumns).isNotNull();
      final List<Column> filteringColumns = new ArrayList<>();
      filteringColumns.add(idColumn);
      final List<Column> singularColumns = expressions.stream()
          .filter(FhirPath::isSingular)
          .map(FhirPath::getValueColumn)
          .collect(Collectors.toList());
      filteringColumns.addAll(singularColumns);
      final Dataset<Row> filteringDataset = dataset
          .select(filteringColumns.toArray(new Column[0]))
          .distinct();
      return join(dataset, filteringColumns, filteringDataset, filteringColumns,
          additionalCondition, JoinType.RIGHT_OUTER);
    }
  }

  @Value
  protected static class FhirPathAndContext {

    @Nonnull
    FhirPath fhirPath;

    @Nonnull
    ParserContext context;

  }

  @Value
  protected static class FhirPathContextAndResult {

    @Nonnull
    FhirPath fhirPath;

    @Nonnull
    ParserContext context;

    @Nonnull
    Dataset<Row> result;
  }
}
