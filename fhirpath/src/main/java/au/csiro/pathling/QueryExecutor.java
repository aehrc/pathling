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

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
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
  private final QueryConfiguration configuration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  protected QueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Nonnull
  protected List<FhirPath> parseExpressions(
      @Nonnull final ParserContext parserContext, @Nonnull final Collection<String> expressions) {
    return parseExpressions(parserContext, expressions, Optional.empty());
  }

  @Nonnull
  protected List<FhirPath> parseExpressions(
      @Nonnull final ParserContext parserContext, @Nonnull final Collection<String> expressions,
      @Nonnull final Optional<Dataset<Row>> contextDataset) {
    final List<FhirPath> parsed = new ArrayList<>();
    ParserContext currentContext = contextDataset.map(parserContext::withContextDataset).orElse(
        parserContext);
    for (final String expression : expressions) {
      if (parsed.size() > 0) {
        final FhirPath lastParsed = parsed.get(parsed.size() - 1);
        // If there are no grouping columns and the root nesting level has been erased by the 
        // parsing of the previous expression (i.e. there has been aggregation or use of where), 
        // disaggregate the input context before parsing the right expression.
        final boolean disaggregationRequired = currentContext.getNesting().isRootErased() &&
            !(currentContext.getGroupingColumns().size() == 1
                && currentContext.getGroupingColumns().get(0)
                .equals(currentContext.getInputContext().getIdColumn()));
        currentContext = disaggregationRequired
                         ? currentContext.disaggregate(lastParsed)
                         : currentContext.withContextDataset(lastParsed.getDataset());
      }
      final Parser parser = new Parser(currentContext);
      // Add the parse result to the list of parsed expressions.
      parsed.add(parser.parse(expression));
    }
    return parsed;
  }

  @Nonnull
  protected void validateFilters(@Nonnull final Collection<FhirPath> filters) {
    for (final FhirPath filter : filters) {
      // Each filter expression must evaluate to a singular Boolean value, or a user error will be
      // thrown.
      checkUserInput(filter instanceof BooleanPath || filter instanceof BooleanLiteralPath,
          "Filter expression must be a Boolean: " + filter.getExpression());
      checkUserInput(filter.isSingular(),
          "Filter expression must be a singular value: " + filter.getExpression());
    }
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
  protected static Stream<Column> labelColumns(@Nonnull final Stream<Column> columns,
      @Nonnull final Stream<Optional<String>> labels) {
    //noinspection UnstableApiUsage
    return Streams.zip(
        columns, labels,
        (column, maybeLabel) -> (maybeLabel.map(column::alias).orElse(column)));
  }

  @Nonnull
  private DatasetWithColumn getFilteredIds(@Nonnull final Iterable<String> filters,
      @Nonnull final ResourcePath inputContext, @Nonnull final BinaryOperator<Column> operator) {
    ResourcePath currentContext = inputContext;
    @Nullable Column filterColumn = null;

    for (final String filter : filters) {
      // Parse the filter expression.
      final ParserContext parserContext = new ParserContext(currentContext, fhirContext,
          sparkSession, dataSource,
          terminologyServiceFactory, Collections.singletonList(currentContext.getIdColumn()));
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
      currentContext = currentContext.copy(currentContext.getExpression(), fhirPath.getDataset(),
          currentContext.getIdColumn(), currentContext.getValueColumn(),
          currentContext.getOrderingColumn(), currentContext.isSingular(),
          currentContext.getThisColumn());
    }
    requireNonNull(filterColumn);

    // Return a dataset of filtered IDs with an aliased ID column, ready for joining.
    final String filterIdAlias = randomAlias();
    final Dataset<Row> dataset = currentContext.getDataset().select(
        currentContext.getIdColumn().alias(filterIdAlias));
    return new DatasetWithColumn(dataset.filter(filterColumn), col(filterIdAlias));
  }

}
