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

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IQueryParameterAnd;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
      @Nonnull final Database database,
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
  private Dataset<Row> initializeDataset() {
    final ResourcePath resourcePath = ResourcePath
        .build(getFhirContext(), getDataSource(), subjectResource, subjectResource.toCode(),
            true, true);
    final Dataset<Row> subjectDataset = resourcePath.getDataset();
    final Column subjectIdColumn = resourcePath.getIdColumn();

    final Dataset<Row> dataset;

    if (filters.isEmpty() || filters.get().getValuesAsQueryTokens().isEmpty()) {
      // If there are no filters, return all resources.
      dataset = subjectDataset;

    } else {
      final Collection<FhirPath> fhirPaths = new ArrayList<>();
      @Nullable Column filterIdColumn = null;
      @Nullable Column filterColumn = null;

      ResourcePath currentContext = ResourcePath
          .build(getFhirContext(), getDataSource(), subjectResource, subjectResource.toCode(),
              true);

      // Parse each of the supplied filter expressions, building up a filter column. This captures 
      // the AND/OR conditions possible through the FHIR API, see 
      // https://hl7.org/fhir/R4/search.html#combining.
      for (final StringOrListParam orParam : filters.get().getValuesAsQueryTokens()) {
        @Nullable Column orColumn = null;

        for (final StringParam param : orParam.getValuesAsQueryTokens()) {
          final ParserContext parserContext = buildParserContext(currentContext,
              Collections.singletonList(currentContext.getIdColumn()));
          final Parser parser = new Parser(parserContext);
          final String expression = param.getValue();
          checkUserInput(!expression.isBlank(), "Filter expression cannot be blank");

          final FhirPath fhirPath = parser.parse(expression);
          checkUserInput(fhirPath instanceof BooleanPath || fhirPath instanceof BooleanLiteralPath,
              "Filter expression must be of Boolean type: " + fhirPath.getExpression());
          final Column filterValue = fhirPath.getValueColumn();

          // Add each expression to a list that will later be joined.
          fhirPaths.add(fhirPath);

          // Combine all the OR columns with OR logic.
          orColumn = orColumn == null
                     ? filterValue
                     : orColumn.or(filterValue);

          // We save away the first encountered ID column so that we can use it later to join the
          // subject resource dataset with the joined filter datasets.
          if (filterIdColumn == null) {
            filterIdColumn = fhirPath.getIdColumn();
          }

          // Update the context to build the next expression from the same dataset.
          currentContext = currentContext
              .copy(currentContext.getExpression(), fhirPath.getDataset(), fhirPath.getIdColumn(),
                  currentContext.getEidColumn(), fhirPath.getValueColumn(),
                  currentContext.isSingular(), currentContext.getThisColumn());
        }

        // Combine all the columns at this level with AND logic.
        filterColumn = filterColumn == null
                       ? orColumn
                       : filterColumn.and(orColumn);
      }
      requireNonNull(filterIdColumn);
      requireNonNull(filterColumn);
      check(!fhirPaths.isEmpty());

      // Get the full resources which are present in the filtered dataset.
      final String filterIdAlias = randomAlias();
      final Dataset<Row> filteredIds = currentContext.getDataset().select(filterIdColumn.alias(
          filterIdAlias)).filter(filterColumn);
      dataset = subjectDataset
          .join(filteredIds, subjectIdColumn.equalTo(col(filterIdAlias)), "left_semi");
    }

    if (getConfiguration().getCacheResults()) {
      // We cache the dataset because we know it will be accessed for both the total and the record
      // retrieval.
      log.debug("Caching search dataset");
      dataset.cache();
    }

    return dataset;
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
    if (theFromIndex != 0) {
      // Spark does not have an "offset" concept, so we create a list of rows to exclude and
      // subtract them from the dataset using a left anti-join.
      final String excludeAlias = randomAlias();
      final Dataset<Row> exclude = resources.limit(theFromIndex)
          .select(resources.col("id").alias(excludeAlias));
      resources = resources
          .join(exclude, resources.col("id").equalTo(exclude.col(excludeAlias)), "left_anti");
    }
    // The dataset is trimmed to the requested size.
    if (theToIndex != 0) {
      resources = resources.limit(theToIndex - theFromIndex);
    }

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
