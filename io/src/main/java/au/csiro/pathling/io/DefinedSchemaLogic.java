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

package au.csiro.pathling.io;

import au.csiro.pathling.definition.DefinitionContext;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Derives from a schema defined up front what a defined-schema read needs: the observed shape that
 * the ingest walk is given, and the layout schema the read returns (data-model.md).
 */
final class DefinedSchemaLogic {

  private DefinedSchemaLogic() {}

  /**
   * Returns the logic over a session and a set of definitions.
   *
   * @param spark the session, over which the layout schema is computed without reading any data
   * @param definitions the definitions the layout is taken from
   * @return the logic
   */
  @Nonnull
  static DefinedSchemaLogic of(
      @Nonnull final SparkSession spark, @Nonnull final DefinitionContext definitions) {
    throw new UnsupportedOperationException("Defined schemas are not yet implemented.");
  }

  /**
   * Returns the JSON-model shape that the ingest walk is given for a schema: its annotation fields
   * dropped, and each leaf typed as the ingest converters read it.
   *
   * @param resourceType the type of the resources the schema describes
   * @param schema the schema, in this layout
   * @return the observed shape
   */
  @Nonnull
  StructType observedShape(@Nonnull final String resourceType, @Nonnull final StructType schema) {
    throw new UnsupportedOperationException("Defined schemas are not yet implemented.");
  }

  /**
   * Returns the schema that a read with a schema returns before annotation: the output of the
   * ingest walk for the observed shape, in the schema's field order at every depth. It equals the
   * schema's base schema, with every field nullable and no field metadata (FR-008).
   *
   * @param resourceType the type of the resources the schema describes
   * @param schema the schema, in this layout
   * @return the layout schema
   */
  @Nonnull
  StructType layoutSchema(@Nonnull final String resourceType, @Nonnull final StructType schema) {
    throw new UnsupportedOperationException("Defined schemas are not yet implemented.");
  }
}
