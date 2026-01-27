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

package au.csiro.pathling.library.query;

import au.csiro.pathling.search.FhirSearch;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIR search query that can be configured and executed to filter FHIR resources.
 *
 * <p>The query can be configured in multiple ways:
 *
 * <ul>
 *   <li>Using individual search criteria via {@link #criterion(String, String...)}
 *   <li>Using a URL query string via {@link #queryString(String)}
 *   <li>Using a pre-built {@link FhirSearch} object via {@link #query(FhirSearch)}
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Using individual criteria
 * Dataset<Row> patients = dataSource.search("Patient")
 *     .criterion("gender", "male")
 *     .criterion("birthdate:ge", "1990-01-01")
 *     .execute();
 *
 * // Using query string
 * Dataset<Row> patients = dataSource.search("Patient")
 *     .queryString("gender=male&birthdate=ge1990-01-01")
 *     .execute();
 *
 * // Using pre-built FhirSearch
 * FhirSearch search = FhirSearch.builder()
 *     .criterion("gender", "male")
 *     .build();
 * Dataset<Row> patients = dataSource.search("Patient")
 *     .query(search)
 *     .execute();
 * }</pre>
 *
 * @author Piotr Szul
 * @see FhirSearch
 */
public class FhirSearchQuery {

  @Nonnull private final ResourceType resourceType;

  @Nonnull private final Function<FhirSearch, Dataset<Row>> executor;

  @Nullable private FhirSearch fhirSearch;

  @Nullable private FhirSearch.Builder searchBuilder;

  /**
   * Creates a new FhirSearchQuery instance.
   *
   * @param resourceType the FHIR resource type to search
   * @param executor the function responsible for executing the search query
   */
  public FhirSearchQuery(
      @Nonnull final ResourceType resourceType,
      @Nonnull final Function<FhirSearch, Dataset<Row>> executor) {
    this.resourceType = resourceType;
    this.executor = executor;
  }

  /**
   * Adds a search criterion with the specified parameter code and values.
   *
   * <p>The parameter code may include a modifier suffix (e.g., "gender:not" or "family:exact").
   * Multiple values for the same criterion are combined with OR logic.
   *
   * <p>Multiple calls to this method add additional criteria, which are combined with AND logic.
   *
   * @param parameterCode the search parameter code, optionally with modifier suffix
   * @param values the search values
   * @return this query instance for method chaining
   */
  @Nonnull
  public FhirSearchQuery criterion(
      @Nonnull final String parameterCode, @Nonnull final String... values) {
    getOrCreateBuilder().criterion(parameterCode, values);
    return this;
  }

  /**
   * Configures the query using a URL query string.
   *
   * <p>The query string should be in standard URL query format without the leading '?'. Values
   * should be URL-encoded per RFC 3986.
   *
   * <p>FHIR escape sequences are supported: {@code \,} for literal comma, {@code \\} for literal
   * backslash.
   *
   * <p>This method replaces any previously configured criteria or query.
   *
   * @param queryString the URL query string (e.g., "gender=male&amp;birthdate=ge1990")
   * @return this query instance for method chaining
   * @see FhirSearch#fromQueryString(String)
   */
  @Nonnull
  public FhirSearchQuery queryString(@Nonnull final String queryString) {
    this.fhirSearch = FhirSearch.fromQueryString(queryString);
    this.searchBuilder = null;
    return this;
  }

  /**
   * Configures the query using a pre-built {@link FhirSearch} object.
   *
   * <p>This method replaces any previously configured criteria or query.
   *
   * @param search the FHIR search configuration to use
   * @return this query instance for method chaining
   */
  @Nonnull
  public FhirSearchQuery query(@Nonnull final FhirSearch search) {
    this.fhirSearch = search;
    this.searchBuilder = null;
    return this;
  }

  /**
   * Executes the configured FHIR search query and returns the filtered results.
   *
   * <p>If no criteria have been configured, this returns all resources of the specified type.
   *
   * @return a Spark Dataset containing the filtered FHIR resources
   */
  @Nonnull
  public Dataset<Row> execute() {
    final FhirSearch search = buildSearch();
    return executor.apply(search);
  }

  /**
   * Gets the resource type being searched.
   *
   * @return the resource type
   */
  @Nonnull
  public ResourceType getResourceType() {
    return resourceType;
  }

  /**
   * Gets or creates the internal FhirSearch.Builder for accumulating criteria.
   *
   * @return the builder instance
   */
  @Nonnull
  private FhirSearch.Builder getOrCreateBuilder() {
    if (searchBuilder == null) {
      searchBuilder = FhirSearch.builder();
      fhirSearch = null; // Clear any pre-set search when using builder
    }
    return searchBuilder;
  }

  /**
   * Builds the final FhirSearch from either the builder or the pre-set search.
   *
   * @return the FhirSearch to execute
   */
  @Nonnull
  private FhirSearch buildSearch() {
    if (searchBuilder != null) {
      return searchBuilder.build();
    }
    if (fhirSearch != null) {
      return fhirSearch;
    }
    // No criteria configured - return empty search (will return all resources)
    return FhirSearch.builder().build();
  }
}
