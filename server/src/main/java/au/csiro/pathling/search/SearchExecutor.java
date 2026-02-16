/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Date;
import java.util.List;
import java.util.Optional;
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
public class SearchExecutor implements IBundleProvider {

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final String subjectResourceCode;

  @Nonnull private final Dataset<Row> result;

  @Nonnull private Optional<Integer> count;

  private final boolean cacheResults;

  /**
   * Constructs a new SearchExecutor.
   *
   * @param fhirContext the FHIR context for FHIR model operations
   * @param dataSource the data source containing the resources to query
   * @param fhirEncoders the encoders for converting Spark rows to FHIR resources
   * @param subjectResourceCode the type code of the resource to search (e.g., "Patient",
   *     "ViewDefinition")
   * @param standardSearchQueryString an optional query string containing standard FHIR search
   *     parameters (e.g., "gender=male&amp;birthdate=ge1990-01-01"), or null if none
   * @param filters the optional FHIRPath filter expressions to apply
   * @param cacheResults whether to cache the result dataset
   */
  public SearchExecutor(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final String subjectResourceCode,
      @Nullable final String standardSearchQueryString,
      @Nonnull final Optional<StringAndListParam> filters,
      final boolean cacheResults) {
    this.fhirEncoders = fhirEncoders;
    this.subjectResourceCode = subjectResourceCode;
    this.cacheResults = cacheResults;
    this.count = Optional.empty();

    final String filterStrings = filters.map(SearchExecutor::filtersToString).orElse("none");
    log.info(
        "Received search request: resource={}, standardParams=[{}], filters=[{}]",
        subjectResourceCode,
        standardSearchQueryString != null ? standardSearchQueryString : "none",
        filterStrings);

    this.result = initializeDataset(fhirContext, dataSource, standardSearchQueryString, filters);
  }

  @Nonnull
  private Dataset<Row> initializeDataset(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
      @Nullable final String standardSearchQueryString,
      @Nonnull final Optional<StringAndListParam> filters) {

    // Try to read the resource type from the data source. This allows data sources that support
    // dynamic discovery (like DynamicDeltaSource) to find newly created Delta tables.
    final Dataset<Row> flatDataset;
    try {
      flatDataset = dataSource.read(subjectResourceCode);
    } catch (final IllegalArgumentException e) {
      // This happens when the resource type doesn't exist in the data source.
      log.info("No data found for resource type: {}, returning empty result", subjectResourceCode);
      return createEmptyDataset();
    }

    final boolean hasStandardParams =
        standardSearchQueryString != null && !standardSearchQueryString.isBlank();
    final boolean hasFhirPathFilters =
        filters.isPresent() && !filters.get().getValuesAsQueryTokens().isEmpty();

    // If there are no filters of any kind, return the flat dataset directly.
    if (!hasStandardParams && !hasFhirPathFilters) {
      return cacheIfEnabled(flatDataset);
    }

    Dataset<Row> resultDataset = flatDataset;

    // Apply standard FHIR search parameters using SearchColumnBuilder.
    if (hasStandardParams) {
      final SearchColumnBuilder builder = SearchColumnBuilder.withDefaultRegistry(fhirContext);
      final ResourceType resourceType = ResourceType.fromCode(subjectResourceCode);
      final Column standardFilter =
          builder.fromQueryString(resourceType, standardSearchQueryString);
      final Column safeFilter = coalesce(standardFilter, lit(false));
      resultDataset = resultDataset.filter(safeFilter);
    }

    // Apply FHIRPath filter expressions using DatasetEvaluator. This preserves FHIRPath boolean
    // conversion semantics (truthy evaluation of non-boolean collections) and supports custom
    // resource types like ViewDefinition.
    if (hasFhirPathFilters) {
      resultDataset = applyFhirPathFilters(fhirContext, resultDataset, filters.get());
    }

    return cacheIfEnabled(resultDataset);
  }

  /**
   * Applies FHIRPath filter expressions to a dataset using the DatasetEvaluator, preserving the
   * AND/OR combining semantics from the StringAndListParam structure.
   *
   * @param fhirContext the FHIR context
   * @param dataset the dataset to filter
   * @param filters the FHIRPath filter expressions
   * @return the filtered dataset
   */
  @Nonnull
  private Dataset<Row> applyFhirPathFilters(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final StringAndListParam filters) {

    // Create a FHIRPath evaluator for the subject resource type.
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(subjectResourceCode, fhirContext)
            .withDataset(dataset)
            .build();
    final ResourceCollection inputContext = evaluator.getDefaultInputContext();
    final Parser parser = new Parser();

    // Parse and evaluate each filter expression, building up a combined filter column.
    // See https://hl7.org/fhir/R4/search.html#combining.
    @Nullable Column filterColumn = null;

    for (final StringOrListParam orParam : filters.getValuesAsQueryTokens()) {
      @Nullable Column orColumn = null;

      for (final StringParam param : orParam.getValuesAsQueryTokens()) {
        final String expression = param.getValue();
        checkUserInput(!expression.isBlank(), "Filter expression cannot be blank");

        final FhirPath fhirPath = parser.parse(expression);
        final Collection filterResult = evaluator.evaluateToCollection(fhirPath, inputContext);
        final Column filterValue = filterResult.asBooleanSingleton().getColumn().getValue();

        orColumn = orColumn == null ? filterValue : orColumn.or(filterValue);
      }

      filterColumn = filterColumn == null ? orColumn : filterColumn.and(orColumn);
    }

    requireNonNull(filterColumn);

    final Column safeFilterColumn = coalesce(filterColumn, lit(false));

    // Join the flat dataset with filtered IDs to preserve the flat schema for encoding.
    final String filterIdAlias = randomAlias();
    final Dataset<Row> evaluatorDataset = evaluator.getDataset();
    final Dataset<Row> filteredIds =
        evaluatorDataset
            .select(evaluatorDataset.col("id").alias(filterIdAlias))
            .filter(safeFilterColumn);

    return dataset.join(filteredIds, dataset.col("id").equalTo(col(filterIdAlias)), "left_semi");
  }

  @Nonnull
  private Dataset<Row> cacheIfEnabled(@Nonnull final Dataset<Row> dataset) {
    if (cacheResults) {
      // Cache the dataset because it will be accessed for both the total count and record
      // retrieval.
      log.debug("Caching search dataset");
      dataset.cache();
    }
    return dataset;
  }

  /**
   * Creates an empty dataset with the correct schema for the subject resource type.
   *
   * @return an empty dataset
   */
  @Nonnull
  private Dataset<Row> createEmptyDataset() {
    final SparkSession spark = SparkSession.active();
    final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(subjectResourceCode);
    requireNonNull(encoder, "No encoder found for resource type: " + subjectResourceCode);
    return spark.emptyDataset(encoder).toDF();
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

    Dataset<Row> resources = result;

    // Spark does not have an "offset" concept, so we create a list of rows to exclude and
    // subtract them from the dataset using a left anti-join.
    if (theFromIndex != 0) {
      final String excludeAlias = randomAlias();
      final Dataset<Row> exclude =
          resources.limit(theFromIndex).select(resources.col("id").alias(excludeAlias));
      resources =
          resources.join(exclude, resources.col("id").equalTo(col(excludeAlias)), "left_anti");
    }

    // Trim the dataset to the requested size.
    if (theToIndex != 0) {
      resources = resources.limit(theToIndex - theFromIndex);
    }

    // Encode the resources into HAPI FHIR objects and collect.
    @Nullable final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(subjectResourceCode);
    requireNonNull(encoder);

    return resources.as(encoder).collectAsList();
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
      count = Optional.of(Math.toIntExact(result.count()));
    }
    return count.get();
  }

  @Nonnull
  private static String filtersToString(@Nonnull final StringAndListParam stringAndListParam) {
    return stringAndListParam.getValuesAsQueryTokens().stream()
        .map(
            andParam ->
                String.join(
                    ",",
                    andParam.getValuesAsQueryTokens().stream().map(StringParam::getValue).toList()))
        .reduce((a, b) -> a + " & " + b)
        .orElse("");
  }
}
