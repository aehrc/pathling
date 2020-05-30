/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.search;

import static au.csiro.pathling.QueryHelpers.joinOnId;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IQueryParameterAnd;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 * into HAPI's mechanism for returning paged search results.
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
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyClient A {@link TerminologyClient} for resolving terminology queries
   * @param terminologyClientFactory A {@link TerminologyClientFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param subjectResource The type of resource that is the subject for this query
   * @param filters A list of filters that should be applied within queries
   */
  public SearchExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<StringAndListParam> filters) {
    super(configuration, fhirContext, sparkSession, resourceReader, terminologyClient,
        terminologyClientFactory);
    this.fhirEncoders = fhirEncoders;
    this.subjectResource = subjectResource;
    this.filters = filters;
    this.result = initializeDataset();
    this.count = Optional.empty();

    final String filterStrings = filters.map(SearchExecutor::filtersToString).orElse("none");
    log.info("Received search request: filters=[" + filterStrings + "]");

  }

  @Nonnull
  private Dataset<Row> initializeDataset() {
    final ParserContext context = buildParserContext(subjectResource);
    final Dataset<Row> subjectDataset = context.getInputContext().getDataset();

    if (!filters.isPresent() || filters.get().getValuesAsQueryTokens().isEmpty()) {
      // If there are no filters, return all resources.
      return subjectDataset;
    } else {
      final Parser parser = new Parser(context);
      final List<FhirPath> fhirPaths = new ArrayList<>();
      @Nullable Column idColumn = null;
      @Nullable Column filterColumn = null;

      // Parse each of the supplied filter expressions, building up a filter column. This captures 
      // the AND/OR conditions possible through the FHIR API, see 
      // https://hl7.org/fhir/R4/search.html#combining.
      for (final StringOrListParam orParam : filters.get().getValuesAsQueryTokens()) {
        @Nullable Column orColumn = null;

        for (final StringParam param : orParam.getValuesAsQueryTokens()) {
          final FhirPath fhirPath = parser.parse(param.getValue());

          // Add each expression to a list that will later be joined.
          fhirPaths.add(fhirPath);

          // Combine all the OR columns with OR logic.
          orColumn = orColumn == null
                     ? fhirPath.getValueColumn()
                     : orColumn.or(fhirPath.getValueColumn());

          // We save away the first encountered ID column so that we can use it later to join the
          // subject resource dataset with the joined filter datasets.
          if (idColumn == null) {
            idColumn = fhirPath.getIdColumn();
          }
        }

        // Combine all the columns at this level with AND logic.
        filterColumn = filterColumn == null
                       ? orColumn
                       : filterColumn.and(orColumn);
      }
      checkNotNull(idColumn);
      checkNotNull(filterColumn);
      check(!fhirPaths.isEmpty());

      // Join all of the datasets from the parsed filter expressions together.
      final Dataset<Row> filterDataset = joinExpressions(fhirPaths).where(filterColumn);

      // Get the full resources which are present in the filtered dataset.
      final Dataset<Row> dataset = joinOnId(subjectDataset.alias("subject"),
          subjectDataset.col("id"), filterDataset, idColumn, JoinType.LEFT_SEMI);

      // We cache the dataset because we know it will be accessed for both the total and the
      // record retrieval.
      dataset.cache();

      return dataset;
    }
  }

  @Override
  @Nonnull
  public IPrimitiveType<Date> getPublished() {
    return new InstantType(new Date());
  }

  @Nonnull
  @Override
  public List<IBaseResource> getResources(final int theFromIndex, final int theToIndex) {
    log.info("Retrieving search results (" + (theFromIndex + 1) + "-" + theToIndex + ")");

    Dataset<Row> resources = result;
    if (theFromIndex != 0) {
      // Spark does not have an "offset" concept, so we create a list of rows to exclude and
      // subtract them from the dataset using a left anti-join.
      final Dataset<Row> exclude = resources.limit(theFromIndex)
          .select(resources.col("id").alias("excludeId"));
      resources = joinOnId(resources, resources.col("id"), exclude, exclude.col("excludeId"),
          JoinType.LEFT_ANTI);
    }
    // The dataset is trimmed to the requested size.
    if (theToIndex != 0) {
      resources = resources.limit(theToIndex - theFromIndex);
    }

    // The requested resources are encoded into HAPI FHIR objects, and then collected.
    @Nullable final ExpressionEncoder<IBaseResource> encoder = fhirEncoders
        .of(subjectResource.toCode());
    checkNotNull(encoder);
    reportQueryPlan(resources);
    return resources.select("value.*").as(encoder).collectAsList();
  }

  private void reportQueryPlan(@Nonnull final Dataset<Row> resources) {
    if (getConfiguration().getExplainQueries()) {
      log.info("Search query plan:");
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
    if (!count.isPresent()) {
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
