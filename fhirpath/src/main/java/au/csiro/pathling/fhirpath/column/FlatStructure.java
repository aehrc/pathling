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

package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

/**
 * The canonical structure of an engine value that is one flat structure, such as a date (decision
 * 152): a fixed order of its content and annotation fields, and the order of the fields of those of
 * them that are structures themselves, such as a quantity's exact canonical value. The reconciler
 * orders the merge of such values by it (FR-056).
 */
@AllArgsConstructor(staticName = "of")
public final class FlatStructure implements CanonicalStructure {

  @Nonnull private final List<String> fieldOrder;

  @Nonnull private final Map<String, CanonicalStructure> nested;

  /**
   * Returns the canonical structure of a flat value with no structure beneath its fields.
   *
   * @param fieldOrder the order of its fields
   * @return the canonical structure
   */
  @Nonnull
  public static FlatStructure of(@Nonnull final List<String> fieldOrder) {
    return of(fieldOrder, Map.of());
  }

  @Override
  @Nonnull
  public List<String> fieldOrder() {
    return fieldOrder;
  }

  @Override
  @Nonnull
  public Optional<CanonicalStructure> field(@Nonnull final String name) {
    return Optional.ofNullable(nested.get(name));
  }
}
