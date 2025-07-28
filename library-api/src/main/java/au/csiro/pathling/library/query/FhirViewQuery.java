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

package au.csiro.pathling.library.query;

import static au.csiro.pathling.validation.ValidationUtils.ensureValid;

import au.csiro.pathling.views.FhirView;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIR view query that can be configured and executed to extract structured data from
 * FHIR resources.
 * <p>
 * The query can be configured in two ways:
 * <ul>
 *   <li>Using a JSON string representation of the view via {@link #json(String)}</li>
 *   <li>Using a {@link FhirView} object directly via {@link #view(FhirView)}</li>
 * </ul>
 * <p>
 *
 * @author John Grimes
 * @see QueryBuilder
 * @see FhirView
 * @see QueryDispatcher
 */
public class FhirViewQuery extends QueryBuilder {

  @Nonnull
  private final Gson gson;

  @Nullable
  private FhirView fhirView;

  /**
   * Creates a new FhirViewQuery instance.
   *
   * @param dispatcher the query dispatcher responsible for executing the view query
   * @param subjectResource the primary FHIR resource type being queried
   * @param gson the Gson instance used for JSON serialization/deserialization
   */
  public FhirViewQuery(@Nonnull final QueryDispatcher dispatcher,
      @Nonnull final ResourceType subjectResource, @Nonnull final Gson gson) {
    super(dispatcher, subjectResource);
    this.gson = gson;
  }

  /**
   * Configures the query using a JSON string representation of the FHIR view.
   * <p>
   * This method deserializes the JSON string into a {@link FhirView} object and validates its
   * structure. The JSON should conform to the FhirView schema, defining the resource type, select
   * clauses, where conditions, and other view configuration.
   * <p>
   * If the JSON is null, the view configuration remains unchanged.
   *
   * @param json the JSON string representation of the FHIR view, or null
   * @return this query instance for method chaining
   * @throws IllegalArgumentException if the JSON is invalid or the resulting view is not valid
   */
  @Nonnull
  public FhirViewQuery json(@Nullable final String json) {
    if (json != null) {
      fhirView = gson.fromJson(json, FhirView.class);
      ensureValid(fhirView, "View is not valid");
    }
    return this;
  }

  /**
   * Configures the query using a {@link FhirView} object directly.
   * <p>
   * This method sets the FHIR view configuration directly using a FhirView object. The view is
   * validated to ensure it has a valid structure and configuration. This is the preferred method
   * when the view is constructed programmatically rather than from JSON.
   *
   * @param fhirView the FHIR view configuration to use for this query
   * @return this query instance for method chaining
   * @throws IllegalArgumentException if the view is not valid
   */
  @Nonnull
  public FhirViewQuery view(@Nonnull final FhirView fhirView) {
    this.fhirView = fhirView;
    ensureValid(fhirView, "View is not valid");
    return this;
  }

  /**
   * Executes the configured FHIR view query and returns the results.
   * <p>
   * This method dispatches the configured view to the query dispatcher for execution against the
   * FHIR data source. The result is a Spark Dataset containing rows of extracted data according to
   * the view's select clauses and filtering conditions.
   *
   * @return a Spark Dataset containing the query results
   * @throws IllegalStateException if no view has been configured using {@link #json(String)} or
   * {@link #view(FhirView)}
   */
  @Nonnull
  @Override
  public Dataset<Row> execute() {
    if (fhirView == null) {
      throw new IllegalStateException("No view has been set");
    }
    return dispatcher.dispatch(fhirView);
  }
}
