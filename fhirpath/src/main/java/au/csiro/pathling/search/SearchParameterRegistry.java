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

import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.SearchParameter;

/**
 * Registry for FHIR search parameter definitions.
 *
 * <p>This is a pure data holder that maps resource types to their available search parameters. Use
 * the static factory methods to create instances.
 *
 * @see SearchParameterDefinition
 * @see #fromInputStream(FhirContext, InputStream)
 * @see #fromSearchParameters(List)
 */
public class SearchParameterRegistry {

  @Nonnull private final Map<ResourceType, Map<String, SearchParameterDefinition>> parameters;

  /**
   * Creates a registry with pre-loaded parameters.
   *
   * <p>This constructor is for subclasses (e.g., test registries) that provide static parameters.
   *
   * @param parameters the pre-loaded parameter map
   */
  protected SearchParameterRegistry(
      @Nonnull final Map<ResourceType, Map<String, SearchParameterDefinition>> parameters) {
    this.parameters = parameters;
  }

  /**
   * Creates a registry by loading from a JSON input stream.
   *
   * @param fhirContext the FHIR context for parsing
   * @param inputStream the input stream containing JSON (a FHIR Bundle of SearchParameter
   *     resources)
   * @return a new registry with loaded parameters
   */
  @Nonnull
  public static SearchParameterRegistry fromInputStream(
      @Nonnull final FhirContext fhirContext, @Nonnull final InputStream inputStream) {
    return new SearchParameterRegistry(
        new JsonSearchParameterLoader(fhirContext).load(inputStream));
  }

  /**
   * Creates a registry from a list of HAPI SearchParameter objects.
   *
   * @param searchParameters the search parameters to include
   * @return a new registry with the provided parameters
   */
  @Nonnull
  public static SearchParameterRegistry fromSearchParameters(
      @Nonnull final List<SearchParameter> searchParameters) {
    return new SearchParameterRegistry(
        JsonSearchParameterLoader.processSearchParameters(searchParameters));
  }

  /**
   * Gets the search parameter definition for a given resource type and parameter code.
   *
   * @param resourceType the resource type
   * @param code the parameter code
   * @return the parameter definition, or empty if not found
   */
  @Nonnull
  public Optional<SearchParameterDefinition> getParameter(
      @Nonnull final ResourceType resourceType, @Nonnull final String code) {
    return Optional.ofNullable(parameters.get(resourceType)).map(params -> params.get(code));
  }
}
