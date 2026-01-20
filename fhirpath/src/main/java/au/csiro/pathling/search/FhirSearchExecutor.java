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

import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Executes FHIR search queries against a data source.
 * <p>
 * This executor uses {@link ResourceFilterFactory} to create filters from FHIR search criteria,
 * then applies those filters to resource datasets from the data source.
 *
 * @see <a href="https://hl7.org/fhir/search.html">FHIR Search</a>
 * @see ResourceFilterFactory
 * @see ResourceFilter
 */
@Getter
@RequiredArgsConstructor
public class FhirSearchExecutor {

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final ResourceFilterFactory filterFactory;

  /**
   * Creates an executor with the default bundled search parameter registry for FHIR R4.
   * <p>
   * This method requires an R4 FhirContext and uses the bundled R4 search parameters from the HL7
   * FHIR specification.
   *
   * @param fhirContext the FHIR context (must be R4)
   * @param dataSource the data source
   * @return a new executor with the default R4 registry
   * @throws IllegalArgumentException if the FhirContext is not R4
   */
  @Nonnull
  public static FhirSearchExecutor withDefaultRegistry(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource) {
    return new FhirSearchExecutor(dataSource,
        ResourceFilterFactory.withDefaultRegistry(fhirContext));
  }

  /**
   * Creates an executor with an explicit registry.
   *
   * @param fhirContext the FHIR context
   * @param dataSource the data source
   * @param registry the search parameter registry
   * @return a new executor
   */
  @Nonnull
  public static FhirSearchExecutor withRegistry(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
      @Nonnull final SearchParameterRegistry registry) {
    return new FhirSearchExecutor(dataSource,
        ResourceFilterFactory.withRegistry(fhirContext, registry));
  }

  /**
   * Executes a FHIR search query.
   *
   * @param resourceType the resource type to search
   * @param search the search criteria
   * @return a filtered dataset with the same schema as the original resource data
   */
  @Nonnull
  public Dataset<Row> execute(@Nonnull final ResourceType resourceType,
      @Nonnull final FhirSearch search) {
    // Read the flat resource dataset
    final Dataset<Row> dataset = dataSource.read(resourceType.toCode());

    // Create filter from search and apply it
    final ResourceFilter filter = filterFactory.fromSearch(resourceType, search);
    return filter.apply(dataset);
  }
}
