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

package au.csiro.pathling.operations.sql;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintViolationException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Parses a ViewDefinition resource into a {@link FhirView} and checks that it can actually be
 * executed, reporting either failure against a caller-nominated request parameter.
 *
 * <p>Both the subject of an operation and an inline supporting artefact are ViewDefinitions subject
 * to the same rules, and both are checked before any subject runs, so a view that cannot be
 * executed is reported at request time rather than part-way through a job. Only the parameter named
 * in the outcome differs, which is why the caller supplies it.
 *
 * @author John Grimes
 */
@Component
public class FhirViewValidator {

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final Gson gson;

  /**
   * Constructs a new FhirViewValidator.
   *
   * @param fhirContext the FHIR context, used to serialise a view for parsing
   * @param serverConfiguration the server configuration, consulted for the query configuration
   * @param deltaLake the data source a view's query plan is built against
   */
  @Autowired
  public FhirViewValidator(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final QueryableDataSource deltaLake) {
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
    this.deltaLake = deltaLake;
    this.gson = ViewDefinitionGson.create();
  }

  /**
   * Parses a ViewDefinition resource into a view, via JSON round-tripping.
   *
   * @param viewResource the ViewDefinition resource
   * @param expression the request parameter to name in an error outcome
   * @return the parsed view
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) if the resource cannot
   *     be parsed as a ViewDefinition
   */
  @Nonnull
  public FhirView parse(
      @Nonnull final IBaseResource viewResource, @Nonnull final String expression) {
    try {
      return gson.fromJson(
          fhirContext.newJsonParser().encodeResourceToString(viewResource), FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          expression,
          "A '%s' ViewDefinition could not be parsed: %s".formatted(expression, e.getMessage()));
    }
  }

  /**
   * Checks that a view can be executed, by building its query plan.
   *
   * @param view the parsed view
   * @param expression the request parameter to name in an error outcome
   * @throws ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException (422) if the view is
   *     semantically invalid
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) if the view uses an
   *     expression this server does not support
   */
  public void validateSemantically(@Nonnull final FhirView view, @Nonnull final String expression) {
    try {
      new FhirViewExecutor(fhirContext, deltaLake, serverConfiguration.getQuery()).buildQuery(view);
    } catch (final ConstraintViolationException e) {
      throw SqlOperationError.unprocessable(
          expression, "A '%s' ViewDefinition is invalid: %s".formatted(expression, e.getMessage()));
    } catch (final UnsupportedOperationException | UnsupportedFhirPathFeatureError e) {
      throw SqlOperationError.badRequest(
          IssueType.NOTSUPPORTED,
          expression,
          "A '%s' ViewDefinition uses an unsupported expression: %s"
              .formatted(expression, e.getMessage()));
    }
  }
}
