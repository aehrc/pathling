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

import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.types.StructType;

/** Finds the content the bounds configured for the dense mode would drop (FR-017). */
public final class BoundsCheck {

  private BoundsCheck() {}

  /**
   * Returns a check comparing what the definitions describe against what the dense schema carries.
   *
   * @param canonical the canonical structure of the resource type
   * @param derived the dense schema derived for it
   * @param resourceType the type of the resource, which roots the reported paths
   * @return the check
   */
  @Nonnull
  public static BoundsCheck of(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final StructType derived,
      @Nonnull final String resourceType) {
    throw new UnsupportedOperationException("Bounds checking is not implemented");
  }

  /**
   * Returns the content of a source that the configured bounds would drop.
   *
   * @param observed the schema the source was read with
   * @return the findings, in the order they were reached
   */
  @Nonnull
  public List<NonConformantContent> check(@Nonnull final StructType observed) {
    throw new UnsupportedOperationException("Bounds checking is not implemented");
  }
}
