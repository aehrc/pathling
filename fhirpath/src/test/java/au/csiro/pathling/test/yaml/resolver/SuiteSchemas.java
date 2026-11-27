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
package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.utilities.StructureMerge;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructType;

/**
 * The schemas a YAML suite loads its FHIR resources with (path 1 of R11, FR-021b): for each
 * resource type, the schema inferred from all of the suite's input resources of that type, which
 * are the input files of its resource directory and its inline FHIR subjects. Each schema is
 * derived on first use and kept for the life of the suite.
 *
 * @author Piotr Szul
 */
public final class SuiteSchemas {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The suite's input resources, by resource type. */
  @Nonnull private final Map<String, List<String>> documentsByType;

  @Nonnull private final Map<String, StructType> schemasByType = new ConcurrentHashMap<>();

  /** The schema each resource of the suite is loaded with, by its document. */
  @Nonnull private final Map<String, StructType> definedSchemas = new ConcurrentHashMap<>();

  private SuiteSchemas(@Nonnull final List<String> documents) {
    this.documentsByType =
        documents.stream().collect(Collectors.groupingBy(SuiteSchemas::resourceTypeOf));
  }

  /**
   * Returns the schemas of a suite whose input resources are the given documents.
   *
   * @param documents the suite's input resources, one FHIR JSON document each, of any type
   * @return the schemas of the suite
   */
  @Nonnull
  public static SuiteSchemas of(@Nonnull final List<String> documents) {
    return new SuiteSchemas(documents);
  }

  /**
   * Returns the schema inferred from all of the suite's input resources of a type, deriving it on
   * the first request for the type and returning the same schema on every later one.
   *
   * @param rt the runtime context to infer the schema with
   * @param resourceType the resource type
   * @return the schema of the suite's resources of that type, empty where the suite has none
   */
  @Nonnull
  public StructType schemaFor(
      @Nonnull final RuntimeContext rt, @Nonnull final String resourceType) {
    return schemasByType.computeIfAbsent(
        resourceType,
        type -> {
          final List<String> documents = documentsByType.getOrDefault(type, List.of());
          return documents.isEmpty() ? new StructType() : inferred(rt, type, documents);
        });
  }

  /**
   * Returns the schema a resource of the suite is loaded with: the suite's schema of its type,
   * merged by name with the schema inferred from the resource alone. The schema is derived once for
   * each document, since many cases load the same input file.
   *
   * @param rt the runtime context to infer the schemas with
   * @param resourceType the type of the resource
   * @param document the resource, as FHIR JSON
   * @return the schema to load the resource with
   */
  @Nonnull
  public StructType definedSchemaFor(
      @Nonnull final RuntimeContext rt,
      @Nonnull final String resourceType,
      @Nonnull final String document) {
    return definedSchemas.computeIfAbsent(
        document,
        json ->
            StructureMerge.merge(
                schemaFor(rt, resourceType),
                inferred(rt, resourceType, List.of(json)),
                DefinitionCanonicalStructure.forResource(
                    FhirDefinitionContext.of(rt.getFhirEncoders().getContext()), resourceType)));
  }

  @Nonnull
  private static StructType inferred(
      @Nonnull final RuntimeContext rt,
      @Nonnull final String resourceType,
      @Nonnull final List<String> documents) {
    return PofDatasets.fromJson(
            rt.getSpark(), rt.getFhirEncoders().getContext(), resourceType, documents)
        .schema();
  }

  @Nonnull
  private static String resourceTypeOf(@Nonnull final String document) {
    try {
      return MAPPER.readTree(document).path("resourceType").asText();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
