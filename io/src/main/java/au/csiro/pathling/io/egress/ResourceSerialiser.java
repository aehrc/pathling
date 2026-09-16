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

package au.csiro.pathling.io.egress;

import au.csiro.pathling.definition.DefinitionContext;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Writes resources stored in this layout back out as FHIR JSON (FR-016, FR-019). */
public final class ResourceSerialiser {

  private ResourceSerialiser(@Nonnull final DefinitionContext definitions) {}

  /**
   * Returns a serialiser over a set of definitions.
   *
   * @param definitions the definitions the layout was derived from
   * @return the serialiser
   */
  @Nonnull
  public static ResourceSerialiser of(@Nonnull final DefinitionContext definitions) {
    return new ResourceSerialiser(definitions);
  }

  /**
   * Returns one FHIR JSON document per stored resource.
   *
   * @param resourceType the type of the resources the dataset carries
   * @param stored the resources, in this layout
   * @return the documents
   */
  @Nonnull
  public Dataset<String> serialise(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> stored) {
    throw new UnsupportedOperationException("Egress is not implemented");
  }
}
