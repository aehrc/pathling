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

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
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
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final ResourceType subjectResource;

  @Nonnull
  private final Optional<StringAndListParam> filters;

  @Nonnull
  private final Dataset<Row> result;

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
  @NotImplemented
  private Dataset<Row> initializeDataset() {
    // final ResourceCollection resourceCollection = ResourceCollection
    //     .build(getFhirContext(), getDataSource(), subjectResource);
    // final Dataset<Row> subjectDataset = resourceCollection.getDataset();
    // final Dataset<Row> dataset;
    //
    // if (filters.isEmpty() || filters.get().getValuesAsQueryTokens().isEmpty()) {
    //   // If there are no filters, return all resources.
    //   dataset = subjectDataset;
    //
    // } else {
    //   final ParserContext parserContext = new ParserContext(resourceCollection,
    //       resourceCollection,
    //       fhirContext, sparkSession,
    //       dataSource,
    //       StaticFunctionRegistry.getInstance(),
    //       terminologyServiceFactory,
    //       Optional.empty()
    //   );
    //   Dataset<Row> currentDataset = subjectDataset;
    //   @Nullable Column filterCondition = null;
    //
    //   // Parse each of the supplied filter expressions, building up a filter column. This captures 
    //   // the AND/OR conditions possible through the FHIR API, see 
    //   // https://hl7.org/fhir/R4/search.html#combining.
    //   for (final StringOrListParam orParam : filters.get().getValuesAsQueryTokens()) {
    //
    //     // Parse all the filter expressions within this AND condition.
    //     final List<String> filterExpressions = orParam.getValuesAsQueryTokens().stream()
    //         .map(StringParam::getValue)
    //         .collect(toList());
    //     checkUserInput(filterExpressions.stream().noneMatch(String::isBlank),
    //         "Filter expression cannot be blank");
    //     final List<Collection> filters = parseExpressions(parserContext, filterExpressions,
    //         Optional.of(currentDataset));
    //     validateFilters(filters);
    //
    //     // Get the dataset from the last filter.
    //     currentDataset = filters.get(filters.size() - 1).getDataset();
    //
    //     // Combine all the columns with OR logic.
    //     final Column orColumn = filters.stream()
    //         .map(Collection::getValueColumn)
    //         .reduce(Column::or)
    //         .orElseThrow();
    //
    //     // Combine OR-grouped columns with AND logic.
    //     filterCondition = filterCondition == null
    //                       ? orColumn
    //                       : filterCondition.and(orColumn);
    //   }
    //   dataset = currentDataset.filter(filterCondition);
    // }
    //
    // final Column[] resourceColumns = resourceCollection.getElementsToColumns().keySet().stream()
    //     .map(colName -> resourceCollection.getElementsToColumns().get(colName).alias(colName))
    //     .toArray(Column[]::new);
    // // Resources are ordered by ID to ensure consistent paging.
    // final Dataset<Row> result = dataset.select(resourceColumns);
    // final WindowSpec window = Window.orderBy(col("id"));
    // final Dataset<Row> withRowNumbers = result.withColumn("row_number",
    //     row_number().over(window));
    //
    // if (getConfiguration().getCacheResults()) {
    //   // We cache the dataset because we know it will be accessed for both the total and the record
    //   // retrieval.
    //   log.debug("Caching search dataset");
    //   withRowNumbers.cache();
    // }
    //
    // return withRowNumbers;
    return null;
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
    resources.explain();

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
