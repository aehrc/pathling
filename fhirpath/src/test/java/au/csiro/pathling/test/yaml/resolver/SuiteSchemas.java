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

import jakarta.annotation.Nonnull;
import java.util.List;
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

  @Nonnull private final List<String> documents;

  private SuiteSchemas(@Nonnull final List<String> documents) {
    this.documents = List.copyOf(documents);
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
   * @return the schema of the suite's resources of that type
   */
  @Nonnull
  public StructType schemaFor(
      @Nonnull final RuntimeContext rt, @Nonnull final String resourceType) {
    throw new UnsupportedOperationException(
        "The suite schema of " + resourceType + " is not derived until T047");
  }
}
