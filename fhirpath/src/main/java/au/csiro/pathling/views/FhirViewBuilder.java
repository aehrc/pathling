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

package au.csiro.pathling.views;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Builder class for creating {@link FhirView} instances.
 *
 * <p>This builder provides a fluent interface for constructing FHIR views with various components
 * such as resource types, constants, select clauses, and where conditions. The builder follows the
 * builder pattern, allowing method chaining for convenient configuration.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * FhirView view = new FhirViewBuilder()
 *     .resource("Patient")
 *     .constant("birthYear", new IntegerType(1990))
 *     .select(SelectClause.of("name", "name.given"))
 *     .where("birthDate >= %birthYear")
 *     .build();
 * }</pre>
 *
 * @author John Grimes
 * @see FhirView
 * @see ConstantDeclaration
 * @see SelectClause
 * @see WhereClause
 */
public class FhirViewBuilder {

  @Nullable private String resource;

  @Nonnull private final List<ConstantDeclaration> constant = new ArrayList<>();

  @Nonnull private final List<SelectClause> select = new ArrayList<>();

  @Nonnull private final List<WhereClause> where = new ArrayList<>();

  /**
   * Sets the FHIR resource type for this view.
   *
   * <p>This is a required field that specifies which FHIR resource type the view will operate on.
   * Common examples include "Patient", "Observation", "Condition", etc.
   *
   * @param resource the FHIR resource type name (e.g., "Patient", "Observation")
   * @return this builder instance for method chaining
   * @throws IllegalArgumentException if resource is null
   */
  public FhirViewBuilder resource(@NotNull final String resource) {
    this.resource = resource;
    return this;
  }

  /**
   * Adds one or more constant declarations to the view.
   *
   * <p>Constants can be referenced in FHIRPath expressions using the % prefix (e.g., %myConstant).
   * This is useful for parameterizing queries or providing reusable values.
   *
   * @param constant one or more constant declarations to add
   * @return this builder instance for method chaining
   */
  public FhirViewBuilder constant(@Nonnull final ConstantDeclaration... constant) {
    Collections.addAll(this.constant, constant);
    return this;
  }

  /**
   * Adds a constant declaration with the specified name and value.
   *
   * <p>This is a convenience method for creating a constant declaration inline. The constant can be
   * referenced in FHIRPath expressions using %name.
   *
   * @param name the name of the constant (used with % prefix in expressions)
   * @param value the FHIR value to associate with this constant
   * @return this builder instance for method chaining
   */
  public FhirViewBuilder constant(@Nonnull final String name, @Nonnull final IBase value) {
    this.constant.add(new ConstantDeclaration(name, value));
    return this;
  }

  /**
   * Adds one or more select clauses to the view.
   *
   * <p>Select clauses define what data should be extracted from the FHIR resources. Each clause
   * typically consists of a column name and a FHIRPath expression.
   *
   * @param select one or more select clauses to add
   * @return this builder instance for method chaining
   */
  public FhirViewBuilder select(@Nonnull final SelectClause... select) {
    Collections.addAll(this.select, select);
    return this;
  }

  /**
   * Adds one or more where clauses to the view.
   *
   * <p>Where clauses define filtering conditions that resources must satisfy to be included in the
   * view results. Each clause contains a FHIRPath expression that evaluates to a boolean.
   *
   * @param where one or more where clauses to add
   * @return this builder instance for method chaining
   */
  public FhirViewBuilder where(@Nonnull final WhereClause... where) {
    Collections.addAll(this.where, where);
    return this;
  }

  /**
   * Adds one or more where conditions as strings.
   *
   * <p>This is a convenience method for adding where clauses when you only need to specify the
   * condition expression without a description.
   *
   * @param where one or more FHIRPath condition expressions
   * @return this builder instance for method chaining
   */
  public FhirViewBuilder where(@Nonnull final String... where) {
    for (final String condition : where) {
      this.where.add(new WhereClause(condition, null));
    }
    return this;
  }

  /**
   * Adds a where condition with an optional description.
   *
   * <p>The description can be used for documentation purposes or to provide context about what the
   * condition is checking for.
   *
   * @param condition the FHIRPath expression that defines the filtering condition
   * @param description optional human-readable description of the condition
   * @return this builder instance for method chaining
   */
  public FhirViewBuilder where(
      @Nonnull final String condition, @Nullable final String description) {
    this.where.add(new WhereClause(condition, description));
    return this;
  }

  /**
   * Builds and returns a new {@link FhirView} instance with the configured settings.
   *
   * <p>This method validates that all required fields are set and creates the final view object.
   * The resource type must be set before calling this method.
   *
   * @return a new FhirView instance configured with the builder's settings
   * @throws IllegalArgumentException if the resource type has not been set
   */
  public FhirView build() {
    checkArgument(resource != null, "Resource must be set");
    return new FhirView(resource, constant, select, where);
  }
}
