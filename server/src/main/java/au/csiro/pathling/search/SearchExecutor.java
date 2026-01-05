/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluator;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluators;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
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
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
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
   * @param filters the optional filter expressions to apply
   * @param cacheResults whether to cache the result dataset
   */
  public SearchExecutor(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final String subjectResourceCode,
      @Nonnull final Optional<StringAndListParam> filters,
      final boolean cacheResults) {
    this.fhirEncoders = fhirEncoders;
    this.subjectResourceCode = subjectResourceCode;
    this.cacheResults = cacheResults;
    this.count = Optional.empty();

    final String filterStrings = filters.map(SearchExecutor::filtersToString).orElse("none");
    log.info(
        "Received search request: resource={}, filters=[{}]", subjectResourceCode, filterStrings);

    this.result = initializeDataset(fhirContext, dataSource, filters);
  }

  @Nonnull
  private Dataset<Row> initializeDataset(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
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

    // If there are no filters, return the flat dataset directly.
    if (filters.isEmpty() || filters.get().getValuesAsQueryTokens().isEmpty()) {
      return cacheIfEnabled(flatDataset);
    }

    // Create a FHIRPath evaluator for the subject resource type.
    final FhirPathEvaluator evaluator =
        FhirPathEvaluators.createSingle(
            subjectResourceCode,
            fhirContext,
            StaticFunctionRegistry.getInstance(),
            Map.of(),
            dataSource);

    // Get the input context for FHIRPath evaluation.
    final ResourceCollection inputContext = evaluator.createDefaultInputContext();

    // Parse and evaluate each filter expression, building up a combined filter column.
    // This captures the AND/OR conditions possible through the FHIR API.
    // See https://hl7.org/fhir/R4/search.html#combining.
    final Parser parser = new Parser();
    @Nullable Column filterColumn = null;

    for (final StringOrListParam orParam : filters.get().getValuesAsQueryTokens()) {
      @Nullable Column orColumn = null;

      for (final StringParam param : orParam.getValuesAsQueryTokens()) {
        final String expression = param.getValue();
        checkUserInput(!expression.isBlank(), "Filter expression cannot be blank");

        // Parse the FHIRPath expression.
        final FhirPath fhirPath = parser.parse(expression);

        // Evaluate the expression against the input context.
        final Collection filterResult = evaluator.evaluate(fhirPath, inputContext);

        // Validate that the expression evaluates to a Boolean.
        checkUserInput(
            filterResult instanceof BooleanCollection,
            "Filter expression must be of Boolean type: " + expression);

        // Get the filter column value.
        final Column filterValue = filterResult.getColumn().getValue();

        // Combine OR conditions within this parameter group.
        orColumn = orColumn == null ? filterValue : orColumn.or(filterValue);
      }

      // Combine AND conditions between parameter groups.
      filterColumn = filterColumn == null ? orColumn : filterColumn.and(orColumn);
    }

    requireNonNull(filterColumn);

    // Get the filtered IDs by selecting the ID column from the evaluator's dataset and applying
    // the filter. The evaluator's dataset has the standardized structure with columns compatible
    // with the filter column.
    final String filterIdAlias = randomAlias();
    final Dataset<Row> evaluatorDataset = evaluator.createInitialDataset();
    final Dataset<Row> filteredIds =
        evaluatorDataset
            .select(evaluatorDataset.col("id").alias(filterIdAlias))
            .filter(filterColumn);

    // Join the flat dataset with the filtered IDs using left_semi to keep only matching rows
    // while preserving the flat schema for encoding.
    final Dataset<Row> filteredDataset =
        flatDataset.join(
            filteredIds, flatDataset.col("id").equalTo(col(filterIdAlias)), "left_semi");

    return cacheIfEnabled(filteredDataset);
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
