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

package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.operator.CombiningLogic;
import jakarta.annotation.Nonnull;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;

/**
 * Package-private utility class containing deduplication logic used by {@link ExistenceFunctions}.
 *
 * @author Piotr Szul
 */
@UtilityClass
class ExistenceLogic {

  /**
   * Deduplicates the items in {@code input} using the equals ({@code =}) operation to determine
   * distinctness. The caller is responsible for handling the statically empty case.
   *
   * @param input the collection to deduplicate, must not be statically empty
   * @return the deduplicated array column
   * @throws UnsupportedFhirPathFeatureError if {@code input} has no FHIRPath type (i.e. is a
   *     complex, non-equatable type)
   */
  @Nonnull
  static Column distinct(@Nonnull final Collection input) {
    if (input.getType().isEmpty()) {
      throw new UnsupportedFhirPathFeatureError("Unsupported equality for complex types");
    }
    return CombiningLogic.dedupeArray(input.getColumn().plural().getValue(), input.getComparator());
  }
}
