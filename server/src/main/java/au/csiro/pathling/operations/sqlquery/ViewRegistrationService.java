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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Manages the lifecycle of Spark temporary views for SQL query execution. Views are registered
 * using the label names from the SQLQuery Library's relatedArtifact entries, so the SQL can
 * reference them directly. All registered views are dropped after execution completes.
 */
@Slf4j
@Component
public class ViewRegistrationService {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final QueryConfiguration queryConfiguration;

  /**
   * Constructs a new ViewRegistrationService.
   *
   * @param sparkSession the Spark session
   * @param fhirContext the FHIR context
   * @param serverConfiguration the server configuration
   */
  @Autowired
  public ViewRegistrationService(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.sparkSession = sparkSession;
    this.fhirContext = fhirContext;
    this.queryConfiguration = serverConfiguration.getQuery();
  }

  /**
   * Registers resolved ViewDefinitions as Spark temporary views. Each view is registered using its
   * label name directly, so the SQL query can reference it without rewriting.
   *
   * @param resolvedViews a map from label (table alias) to the parsed FhirView
   * @param dataSource the data source to use for view execution
   * @return the list of registered view names (for cleanup)
   */
  @Nonnull
  public List<String> registerViews(
      @Nonnull final Map<String, FhirView> resolvedViews, @Nonnull final DataSource dataSource) {

    final List<String> registeredViewNames = new ArrayList<>();

    for (final Map.Entry<String, FhirView> entry : resolvedViews.entrySet()) {
      final String label = entry.getKey();
      final FhirView view = entry.getValue();

      final FhirViewExecutor executor =
          new FhirViewExecutor(fhirContext, dataSource, queryConfiguration);
      final Dataset<Row> result;
      try {
        result = executor.buildQuery(view);
      } catch (final Exception e) {
        // Drop any views that were already registered before failing.
        dropViews(registeredViewNames);
        throw new InvalidRequestException(
            "Failed to execute ViewDefinition for label '" + label + "': " + e.getMessage());
      }

      result.createOrReplaceTempView(label);
      registeredViewNames.add(label);
      log.info("Registered temporary view '{}' for resource type '{}'", label, view.getResource());
    }

    return registeredViewNames;
  }

  /**
   * Drops the specified temporary views from the Spark session.
   *
   * @param viewNames the names of the temporary views to drop
   */
  public void dropViews(@Nonnull final Collection<String> viewNames) {
    for (final String viewName : viewNames) {
      try {
        sparkSession.catalog().dropTempView(viewName);
        log.debug("Dropped temporary view '{}'", viewName);
      } catch (final Exception e) {
        log.warn("Failed to drop temporary view '{}': {}", viewName, e.getMessage());
      }
    }
  }
}
