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
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Registry for FHIR search parameter definitions.
 * <p>
 * By default, loads search parameters from a bundled JSON resource file. The registry maps
 * resource types to their available search parameters.
 *
 * @see SearchParameterDefinition
 * @see JsonSearchParameterLoader
 */
public class SearchParameterRegistry {

  private static final String DEFAULT_RESOURCE = "/search-parameters.json";

  @Nonnull
  private final Map<ResourceType, Map<String, SearchParameterDefinition>> parameters;

  /**
   * Creates a registry by loading from the bundled JSON resource.
   * <p>
   * Uses the default FHIR R4 context for parsing.
   */
  public SearchParameterRegistry() {
    this(FhirContext.forR4());
  }

  /**
   * Creates a registry by loading from the bundled JSON resource.
   *
   * @param fhirContext the FHIR context to use for parsing
   */
  public SearchParameterRegistry(@Nonnull final FhirContext fhirContext) {
    this.parameters = loadFromClasspathResource(fhirContext);
  }

  /**
   * Creates a registry with pre-loaded parameters.
   * <p>
   * This constructor is for subclasses (e.g., test registries) that provide static parameters.
   *
   * @param parameters the pre-loaded parameter map
   */
  protected SearchParameterRegistry(
      @Nonnull final Map<ResourceType, Map<String, SearchParameterDefinition>> parameters) {
    this.parameters = parameters;
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
      @Nonnull final ResourceType resourceType,
      @Nonnull final String code) {
    return Optional.ofNullable(parameters.get(resourceType))
        .map(params -> params.get(code));
  }

  /**
   * Loads parameters from the bundled classpath resource.
   *
   * @param fhirContext the FHIR context for parsing
   * @return the loaded parameters map
   */
  @Nonnull
  private static Map<ResourceType, Map<String, SearchParameterDefinition>> loadFromClasspathResource(
      @Nonnull final FhirContext fhirContext) {
    try (final InputStream is = SearchParameterRegistry.class.getResourceAsStream(
        DEFAULT_RESOURCE)) {
      if (is == null) {
        throw new IllegalStateException(
            "Search parameters resource not found: " + DEFAULT_RESOURCE);
      }
      return new JsonSearchParameterLoader(fhirContext).load(is);
    } catch (final IOException e) {
      throw new UncheckedIOException(
          "Failed to load search parameters from: " + DEFAULT_RESOURCE, e);
    }
  }
}
