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
package au.csiro.pathling.io.transform;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads FHIR JSON and transforms it into the definition-derived schema (R-008).
 *
 * <p>A skeleton, pending implementation.
 */
public final class ResourceTransformer {

  private ResourceTransformer() {}

  /**
   * Returns a transformer over a set of definitions.
   *
   * @param definitions the definitions the schema is derived from
   * @param configuration the configuration of the derivation and the strictness switch
   * @param maxNestingLevel how many times a type may recur within itself, in the dense mode
   * @param enableExtensions whether extensions are carried, in the dense mode
   * @param enabledOpenTypes the types an open choice expands to, in the dense mode
   * @return the transformer
   */
  @Nonnull
  public static ResourceTransformer of(
      @Nonnull final DefinitionContext definitions,
      @Nonnull final SchemaConfiguration configuration,
      final int maxNestingLevel,
      final boolean enableExtensions,
      @Nonnull final Set<String> enabledOpenTypes) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Reads newline-delimited FHIR JSON and transforms it into the derived schema.
   *
   * @param spark the Spark session to read with
   * @param resourceType the type of the resources the source carries
   * @param path the path to read from
   * @return the transformed dataset
   */
  @Nonnull
  public Dataset<Row> read(
      @Nonnull final SparkSession spark,
      @Nonnull final String resourceType,
      @Nonnull final String path) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Transforms a source read with an inferred schema into the derived schema.
   *
   * @param resourceType the type of the resources the source carries
   * @param source the source, read with an inferred schema
   * @return the transformed dataset
   */
  @Nonnull
  public Dataset<Row> transform(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> source) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
