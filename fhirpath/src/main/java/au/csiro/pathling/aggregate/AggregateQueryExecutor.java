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

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.QueryHelpers.createColumns;
import static au.csiro.pathling.query.ExpressionWithLabel.labelsAsStream;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.sql.SqlExpressions;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import jakarta.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class AggregateQueryExecutor extends QueryExecutor {

  /**
   * Constructs a new {@link AggregateQueryExecutor}.
   *
   * @param configuration A {@link QueryConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param dataSource A {@link Database} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   */
  public AggregateQueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
  }

  /**
   * @param query an {@link AggregateRequest}
   * @return a {@link ResultWithExpressions}, which includes the uncollected {@link Dataset}
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public ResultWithExpressions buildQuery(@Nonnull final AggregateRequest query) {
    log.info("Executing request: {}", query);

    // Build a new expression parser, and parse all of the filter and grouping expressions within
    // the query.
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getDataSource(), query.getSubjectResource(),
            query.getSubjectResource().toCode(), true);
    final ParserContext groupingAndFilterContext = buildParserContext(inputContext,
        Collections.singletonList(inputContext.getIdColumn()));
    final Parser parser = new Parser(groupingAndFilterContext);
    final List<FhirPath> filters = parseFilters(parser, query.getFilters());
    final List<FhirPathAndContext> groupingParseResult = parseMaterializableExpressions(
        groupingAndFilterContext, query.getGroupings(), "Grouping");
    final List<FhirPath> groupings = groupingParseResult.stream()
        .map(FhirPathAndContext::getFhirPath)
        .collect(Collectors.toList());

    // Join all filter and grouping expressions together.
    final Column idColumn = inputContext.getIdColumn();
    Dataset<Row> groupingsAndFilters = joinExpressionsAndFilters(inputContext, groupings, filters,
        idColumn);
    // Apply filters.
    groupingsAndFilters = applyFilters(groupingsAndFilters, filters);

    // Remove synthetic fields from struct values (such as _fid) before grouping.
    final QueryHelpers.DatasetWithColumnMap datasetWithNormalizedGroupings = createColumns(
        groupingsAndFilters, groupings.stream().map(FhirPath::getValueColumn)
            .map(SqlExpressions::pruneSyntheticFields).toArray(Column[]::new));

    groupingsAndFilters = datasetWithNormalizedGroupings.getDataset();

    final List<Column> groupingColumns = new ArrayList<>(
        datasetWithNormalizedGroupings.getColumnMap().values());

    // The input context will be identical to that used for the groupings and filters, except that
    // it will use the dataset that resulted from the parsing of the groupings and filters,
    // instead of just the raw resource. This is so that any aggregations that are performed
    // during the parse can use these columns for grouping, rather than the identity of each
    // resource.
    final ResourcePath aggregationContext = inputContext
        .copy(inputContext.getExpression(), groupingsAndFilters, idColumn,
            inputContext.getEidColumn(), inputContext.getValueColumn(), inputContext.isSingular(),
            Optional.empty());
    final ParserContext aggregationParserContext = buildParserContext(aggregationContext,
        groupingColumns);
    final Parser aggregationParser = new Parser(aggregationParserContext);

    // Parse the aggregations, and grab the updated grouping columns. When aggregations are
    // performed during an aggregation parse, the grouping columns need to be updated, as any
    // aggregation operation erases the previous columns that were built up within the dataset.
    final List<FhirPath> aggregations = parseAggregations(aggregationParser,
        query.getAggregations());

    // Join the aggregations together, using equality of the grouping column values as the join
    // condition.
    final List<Column> aggregationColumns =
        aggregations.stream().map(FhirPath::getValueColumn).collect(Collectors.toList());
    Dataset<Row> joinedAggregations = joinExpressionsByColumns(aggregations, groupingColumns);
    if (groupingColumns.isEmpty()) {
      joinedAggregations = joinedAggregations.limit(1);
    }

    // The final column selection will be the grouping columns, followed by the aggregation
    // columns.
    final Column[] finalSelection = Stream.concat(
            labelColumns(groupingColumns.stream(), labelsAsStream(query.getGroupingsWithLabels())),
            labelColumns(aggregationColumns.stream(), labelsAsStream(query.getAggregationsWithLabels()))
        )
        .toArray(Column[]::new);

    final Dataset<Row> finalDataset = joinedAggregations
        .select(finalSelection)
        // This is needed to cater for the scenario where a literal value is used within an
        // aggregation expression.
        .distinct();
    return new ResultWithExpressions(finalDataset, aggregations, groupings, filters);
  }

  @Nonnull
  private List<FhirPath> parseAggregations(@Nonnull final Parser parser,
      @Nonnull final Collection<String> aggregations) {
    return aggregations.stream().map(aggregation -> {
      final FhirPath result = parser.parse(aggregation);
      // Aggregation expressions must evaluate to a singular, Materializable path, or a user error
      // will be returned.
      checkUserInput(result instanceof Materializable,
          "Aggregation expression is not of a supported type: " + aggregation);
      checkUserInput(result.isSingular(),
          "Aggregation expression does not evaluate to a singular value: " + aggregation);
      return result;
    }).collect(Collectors.toList());
  }

  @Value
  public static class ResultWithExpressions {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    List<FhirPath> parsedAggregations;

    @Nonnull
    List<FhirPath> parsedGroupings;

    @Nonnull
    Collection<FhirPath> parsedFilters;

  }
}
