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
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.parser.LenientErrorHandler;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Enumerations.SearchParamType;
import org.hl7.fhir.r4.model.SearchParameter;

/**
 * Loads search parameter definitions from a FHIR Bundle JSON file.
 * <p>
 * Parses the JSON using HAPI FHIR's parser and transforms each SearchParameter into
 * {@link SearchParameterDefinition} entries mapped by resource type.
 * <p>
 * This is an internal implementation class. Use {@link SearchParameterRegistry} factory methods
 * for creating registries.
 *
 * @see SearchParameterDefinition
 * @see FhirPathUnionParser
 */
class JsonSearchParameterLoader {

  @Nonnull
  private final FhirContext fhirContext;

  /**
   * Creates a loader with the specified FHIR context.
   *
   * @param fhirContext the FHIR context to use for parsing
   */
  public JsonSearchParameterLoader(@Nonnull final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
  }

  /**
   * Loads search parameter definitions from a JSON file.
   *
   * @param jsonPath the path to the JSON file
   * @return map of resource type to map of parameter code to definition
   * @throws UncheckedIOException if the file cannot be read or parsed
   */
  @Nonnull
  public Map<ResourceType, Map<String, SearchParameterDefinition>> load(
      @Nonnull final Path jsonPath) {
    try (final InputStream is = Files.newInputStream(jsonPath)) {
      return load(is);
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to load search parameters from: " + jsonPath, e);
    }
  }

  /**
   * Loads search parameter definitions from an input stream.
   *
   * @param jsonStream the input stream containing JSON
   * @return map of resource type to map of parameter code to definition
   */
  @Nonnull
  public Map<ResourceType, Map<String, SearchParameterDefinition>> load(
      @Nonnull final InputStream jsonStream) {

    final IParser parser = fhirContext.newJsonParser();
    // Use lenient parsing to skip unknown types (e.g., "resource" type from R5)
    parser.setParserErrorHandler(new LenientErrorHandler());
    final Bundle bundle = parser.parseResource(Bundle.class, jsonStream);

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        new EnumMap<>(ResourceType.class);

    for (final Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      if (entry.getResource() instanceof final SearchParameter sp) {
        processSearchParameter(sp, result);
      }
    }

    return result;
  }

  /**
   * Processes a list of SearchParameter objects into registry format.
   * <p>
   * This method is static because it doesn't require a FhirContext - the SearchParameter
   * objects are already parsed.
   *
   * @param searchParameters the search parameters to process
   * @return map of resource type to map of parameter code to definition
   */
  @Nonnull
  static Map<ResourceType, Map<String, SearchParameterDefinition>> processSearchParameters(
      @Nonnull final List<SearchParameter> searchParameters) {

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        new EnumMap<>(ResourceType.class);

    for (final SearchParameter sp : searchParameters) {
      processSearchParameter(sp, result);
    }

    return result;
  }

  /**
   * Processes a single SearchParameter and adds entries to the result map.
   *
   * @param sp the search parameter to process
   * @param result the result map to populate
   */
  private static void processSearchParameter(
      @Nonnull final SearchParameter sp,
      @Nonnull final Map<ResourceType, Map<String, SearchParameterDefinition>> result) {

    final String code = sp.getCode();
    final String expression = sp.getExpression();

    // Skip parameters without expression (e.g., composite parameters without expression)
    if (expression == null || expression.isBlank()) {
      return;
    }

    final SearchParameterType type = mapType(sp.getType());

    // Skip unsupported types
    if (type == null) {
      return;
    }

    final List<ResourceType> bases = sp.getBase().stream()
        .map(baseCode -> {
          try {
            return ResourceType.fromCode(baseCode.getCode());
          } catch (final FHIRException e) {
            // Skip unknown resource types (e.g., new R5 resources)
            return null;
          }
        })
        .filter(Objects::nonNull)
        .toList();

    if (bases.isEmpty()) {
      return;
    }

    // Parse union expression into per-resource expressions
    final Map<ResourceType, List<String>> parsed = FhirPathUnionParser.parse(expression, bases);

    // Create definition for each resource
    for (final var entry : parsed.entrySet()) {
      final ResourceType resourceType = entry.getKey();
      final List<String> expressions = entry.getValue();

      final SearchParameterDefinition def = new SearchParameterDefinition(code, type, expressions);

      result.computeIfAbsent(resourceType, k -> new HashMap<>())
          .put(code, def);
    }
  }

  /**
   * Maps HAPI FHIR's SearchParamType to our SearchParameterType.
   *
   * @param hapiType the HAPI type (may be null for unknown types from lenient parsing)
   * @return the mapped type, or null if unsupported or unknown
   */
  private static SearchParameterType mapType(final SearchParamType hapiType) {
    if (hapiType == null) {
      return null;
    }
    return switch (hapiType) {
      case STRING -> SearchParameterType.STRING;
      case TOKEN -> SearchParameterType.TOKEN;
      case DATE -> SearchParameterType.DATE;
      case NUMBER -> SearchParameterType.NUMBER;
      case QUANTITY -> SearchParameterType.QUANTITY;
      case REFERENCE -> SearchParameterType.REFERENCE;
      case URI -> SearchParameterType.URI;
      case COMPOSITE -> SearchParameterType.COMPOSITE;
      case SPECIAL -> SearchParameterType.SPECIAL;
      case NULL -> null;
    };
  }
}
