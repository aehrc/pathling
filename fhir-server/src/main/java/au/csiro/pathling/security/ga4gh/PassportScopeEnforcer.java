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

package au.csiro.pathling.security.ga4gh;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
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
   * @param configuration a {@link QueryConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext a {@link FhirContext} for doing FHIR stuff
   * @param sparkSession a {@link SparkSession} for resolving Spark queries
   * @param dataSource a {@link Database} for retrieving resources
   * @param terminologyServiceFactory a {@link TerminologyServiceFactory} for resolving terminology
   * queries
   * @param passportScope a request-scoped {@link PassportScope} that provides the filters that need
   * to be applied
   */
  public PassportScopeEnforcer(
      @Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final PassportScope passportScope) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
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

      // Build a new expression parser, and parse all the column expressions within the query.
      final ResourceCollection inputContext = ResourceCollection
          .build(getFhirContext(), subjectResource);
      return filterDataset(inputContext, filters, dataset, dataset.col("id"), Column::or);
    }
  }

}
