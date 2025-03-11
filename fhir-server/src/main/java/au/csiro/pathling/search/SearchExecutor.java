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

package au.csiro.pathling.search;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.execution.EvaluatedPath;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.MultiFhirpathEvaluator.ManyFactory;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IQueryParameterAnd;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Encapsulates the execution of a search query, implemented as an IBundleProvider for integration
 * into the HAPI mechanism for returning paged search results.
 *
 * @author John Grimes
 */
@Slf4j
public class SearchExecutor extends QueryExecutor implements IBundleProvider {

  @Nonnull
  private final Parser parser = new Parser();

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final ResourceType subjectResource;

  @Nonnull
  private final Optional<StringAndListParam> filters;

  @Nonnull
  private Dataset<Row> result;

  @Nonnull
  private Optional<Integer> count;


  /**
   * @param configuration A {@link QueryConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param database A {@link Database} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param subjectResource The type of resource that is the subject for this query
   * @param filters A list of filters that should be applied within queries
   */
  public SearchExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource database,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<StringAndListParam> filters) {
    super(configuration, fhirContext, sparkSession, database, terminologyServiceFactory);
    this.fhirEncoders = fhirEncoders;
    this.subjectResource = subjectResource;
    this.filters = filters;
    this.result = initializeDataset();
    this.count = Optional.empty();

    final String filterStrings = filters
        .map(SearchExecutor::filtersToString)
        .orElse("none");
    log.info("Received search request: filters=[{}]", filterStrings);

  }


  @Nonnull
  private FhirPath toFilter(@Nonnull final StringParam param) {
    final String filterExpression = requireNonNull(param.getValue());
    checkUserInput(!filterExpression.isBlank(), "Filter expression cannot be blank");
    return parser.parse(filterExpression);
  }


  @Nonnull
  private static Consumer<EvaluatedPath> checkValidFilter(
      @Nonnull final Dataset<Row> contextDataset) {
    return evaluatedPath -> {
      checkUserInput(evaluatedPath.getResult() instanceof BooleanCollection,
          "Filter expression must be a Boolean: " + evaluatedPath.toExpression());
      checkUserInput(evaluatedPath.bind(contextDataset).isSingular(),
          "Filter expression must evaluate to a singular Boolean value: "
              + evaluatedPath.toExpression());
    };
  }

  @Nonnull
  private Dataset<Row> initializeDataset() {

    // Parse the filter expressions into the list of lists
    // the outer list is combined with and 
    // the inner list is combined with or
    final List<List<FhirPath>> andPlusOrFilters = filters.stream()
        .flatMap(f -> f.getValuesAsQueryTokens().stream())
        .map(StringOrListParam::getValuesAsQueryTokens)
        .map(l -> l.stream().map(this::toFilter).toList())
        .toList();

    final List<FhirPath> filterPaths = andPlusOrFilters.stream()
        .flatMap(List::stream)
        .toList();

    final FhirpathEvaluator fhirEvaluator = ManyFactory.fromPaths(
        subjectResource,
        fhirContext, dataSource,
        filterPaths).create(subjectResource);

    final Dataset<Row> initialDataset = fhirEvaluator.createInitialDataset();

    final List<List<EvaluatedPath>> evaluatedAndPlusOrFilters =
        andPlusOrFilters.stream().map(fhirEvaluator::evaluateWithPath)
            .toList();

    // validate the filter components
    evaluatedAndPlusOrFilters.stream()
        .flatMap(List::stream).forEach(checkValidFilter(initialDataset));

    Optional<Column> filterColumn = evaluatedAndPlusOrFilters.stream()
        .flatMap(ors -> ors.stream().map(EvaluatedPath::getColumnValue).reduce(Column::or)
            .stream())
        .reduce(Column::and);

    final Dataset<Row> filteredDataset = filterColumn.map(initialDataset::where)
        .orElse(initialDataset);

    // I think I just need to inline the main resource
    // TODO: there might be a better way to do this just by looking at the structur
    final Dataset<Row> resourceDataset = filteredDataset.select(functions.inline(
        fhirEvaluator.evaluate(Paths.thisPath()).getColumn().toArray().getValue()
    ));

    final WindowSpec window = Window.orderBy(col("id"));
    final Dataset<Row> withRowNumbers = resourceDataset.withColumn("row_number",
        functions.row_number().over(window));
    if (getConfiguration().getCacheResults()) {
      // We cache the dataset because we know it will be accessed for both the total and the record
      // retrieval.
      log.debug("Caching search dataset");
      withRowNumbers.cache();
    }

    return withRowNumbers;
  }

  @Override
  @Nonnull
  public IPrimitiveType<Date> getPublished() {
    return new InstantType(new Date());
  }

  @Nonnull
  @Override
  public List<IBaseResource> getResources(final int theFromIndex, final int theToIndex) {
    log.info("Retrieving search results ({}-{})", theFromIndex + 1, theToIndex);

    @Nullable Column filterCondition = null;
    if (theFromIndex != 0) {
      filterCondition = col("row_number").geq(theFromIndex + 1);
    }
    if (theToIndex != 0) {
      filterCondition = filterCondition == null
                        ? col("row_number").lt(theToIndex + 1)
                        : filterCondition.and(col("row_number").lt(theToIndex + 1));
    }
    final Dataset<Row> filtered = Optional.ofNullable(filterCondition)
        .map(result::filter).orElse(result);
    final Dataset<Row> resources = filtered.drop("row_number");

    // The requested resources are encoded into HAPI FHIR objects, and then collected.
    @Nullable final ExpressionEncoder<IBaseResource> encoder = fhirEncoders
        .of(subjectResource.toCode());
    requireNonNull(encoder);
    reportQueryPlan(resources);
    return resources.as(encoder).collectAsList();
  }

  private void reportQueryPlan(@Nonnull final Dataset<Row> resources) {
    if (getConfiguration().getExplainQueries()) {
      log.debug("Search query plan:");
      resources.explain(true);
    }
  }

  @Nullable
  @Override
  public String getUuid() {
    return null;
  }

  @Nullable
  @Override
  public Integer preferredPageSize() {
    return null;
  }

  @Nullable
  @Override
  public Integer size() {
    if (count.isEmpty()) {
      reportQueryPlan(result);
      count = Optional.of(Math.toIntExact(result.count()));
    }
    return count.get();
  }

  @Nonnull
  private static String filtersToString(
      @Nonnull final IQueryParameterAnd<StringOrListParam> stringAndListParam) {
    return stringAndListParam
        .getValuesAsQueryTokens().stream()
        .map(andParam -> andParam.getValuesAsQueryTokens().stream()
            .map(StringParam::getValue)
            .collect(Collectors.joining(",")))
        .collect(Collectors.joining(" & "));
  }

}
