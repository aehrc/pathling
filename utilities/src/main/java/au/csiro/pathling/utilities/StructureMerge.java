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
package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.types.StructType;

/**
 * The recursive field-wise union of structure types, under a supplied canonical field order.
 *
 * <p>This is the single implementation serving both the reconciliation of collections whose SQL
 * shapes differ and the merging of divergent file schemas.
 */
public abstract class StructureMerge {

  private StructureMerge() {}

  /**
   * Merges two structures into their recursive field-wise union.
   *
   * @param left the first structure to merge
   * @param right the second structure to merge
   * @param canonical the canonical structure supplying the field order at each node
   * @return the merged structure
   */
  @Nonnull
  public static StructType merge(
      @Nonnull final StructType left,
      @Nonnull final StructType right,
      @Nonnull final CanonicalStructure canonical) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Merges any number of structures into their recursive field-wise union.
   *
   * @param structures the structures to merge
   * @param canonical the canonical structure supplying the field order at each node
   * @return the merged structure
   */
  @Nonnull
  public static StructType merge(
      @Nonnull final List<StructType> structures, @Nonnull final CanonicalStructure canonical) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
