/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collection;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A component that can filter a dataset of resources based on the passport scope of the current
 * request.
 *
 * @author John Grimes
 */
@Slf4j
public class PassportScopeEnforcer extends QueryExecutor {

  @Nonnull
  private final PassportScope passportScope;

  /**
   * @param configuration a {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext a {@link FhirContext} for doing FHIR stuff
   * @param sparkSession a {@link SparkSession} for resolving Spark queries
   * @param resourceReader a {@link ResourceReader} for retrieving resources
   * @param terminologyServiceFactory a {@link TerminologyServiceFactory} for resolving terminology
   * queries
   * @param passportScope a request-scoped {@link PassportScope} that provides the filters that need
   * to be applied
   */
  public PassportScopeEnforcer(
      @Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final PassportScope passportScope) {
    super(configuration, fhirContext, sparkSession, resourceReader, terminologyServiceFactory);
    this.passportScope = passportScope;
  }

  /**
   * @param subjectResource the resource type of the input context
   * @param dataset the dataset to filter
   * @return a filtered copy of the dataset
   */
  public Dataset<Row> enforce(@Nonnull final ResourceType subjectResource,
      @Nonnull final Dataset<Row> dataset) {
    // Apply the filters, if any are present for the subject resource type.
    final Collection<String> filters = passportScope.get(subjectResource);
    if (filters == null || filters.isEmpty()) {
      return dataset;
    } else {
      log.debug("Enforcing scope {} on {} resources", filters, subjectResource.toCode());

      // Build a new expression parser, and parse all of the column expressions within the query.
      final ResourcePath inputContext = ResourcePath
          .build(getFhirContext(), getResourceReader(), subjectResource,
              subjectResource.toCode(), true);

      return filterDataset(inputContext, filters, dataset, dataset.col("id"), Column::or);
    }
  }

}
