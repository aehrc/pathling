/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.apache.spark.sql.functions.col;

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
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
  private final ResourceReader resourceReader;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  protected QueryExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  protected ParserContext buildParserContext(@Nonnull final FhirPath inputContext,
      @Nonnull final List<Column> groupingColumns) {
    return new ParserContext(inputContext, fhirContext, sparkSession, resourceReader,
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
              parserContext.getResourceReader(), parserContext.getTerminologyServiceFactory(),
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
    final List<Dataset<Row>> datasets = expressions.stream()
        .map(expression -> trimTrailingNulls(inputContext, idColumn, expression))
        .collect(Collectors.toList());
    datasets.addAll(filters.stream()
        .map(FhirPath::getDataset)
        .collect(Collectors.toList()));

    return datasets.stream()
        .reduce((a, b) -> join(a, idColumn, b, idColumn, JoinType.LEFT_OUTER))
        .orElseThrow();
  }

  /**
   * We need to remove any trailing null values from non-empty collections, so that aggregations do
   * not count non-empty collections in the empty collection grouping. We do this by joining the
   * distinct set of resource IDs to the dataset using an outer join, where the value is not null.
   */
  @Nonnull
  protected Dataset<Row> trimTrailingNulls(@Nonnull final FhirPath inputContext,
      final @Nonnull Column idColumn, @Nonnull final FhirPath expression) {
    if (expression.isSingular()) {
      // It is not necessary to perform a join to remove trailing nulls for a singular expression.
      return expression.getDataset();
    } else {
      return join(expression.getDataset(), idColumn, inputContext.getDataset(),
          idColumn, expression.getValueColumn().isNotNull(), JoinType.RIGHT_OUTER);
    }
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

  @Value
  protected static class FhirPathAndContext {

    @Nonnull
    FhirPath fhirPath;

    @Nonnull
    ParserContext context;

  }

}
