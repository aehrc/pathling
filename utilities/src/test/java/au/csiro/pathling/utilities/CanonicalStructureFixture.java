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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A hand-built canonical structure, so that the merge can be tested without depending on any
 * definition source.
 *
 * <p>Children are held as suppliers rather than as structures, so that a structure can name itself
 * as its own child and a test can observe exactly how far the merge descends.
 */
final class CanonicalStructureFixture implements CanonicalStructure {

  @Nonnull private final List<String> fieldOrder;

  @Nonnull private final Map<String, Supplier<CanonicalStructure>> children = new HashMap<>();

  private CanonicalStructureFixture(@Nonnull final List<String> fieldOrder) {
    this.fieldOrder = fieldOrder;
  }

  /**
   * Creates a structure whose fields appear in the supplied order and which has no children.
   *
   * @param fieldOrder the canonical field order at this node
   * @return the new structure
   */
  @Nonnull
  static CanonicalStructureFixture ordering(@Nonnull final String... fieldOrder) {
    return new CanonicalStructureFixture(List.of(fieldOrder));
  }

  /**
   * Adds a child beneath the named field.
   *
   * @param name the name of the field
   * @param structure the supplier of the structure beneath it
   * @return this structure, for chaining
   */
  @Nonnull
  CanonicalStructureFixture child(
      @Nonnull final String name, @Nonnull final Supplier<CanonicalStructure> structure) {
    children.put(name, structure);
    return this;
  }

  @Nonnull
  @Override
  public List<String> fieldOrder() {
    return fieldOrder;
  }

  @Nonnull
  @Override
  public Optional<CanonicalStructure> field(@Nonnull final String name) {
    return Optional.ofNullable(children.get(name)).map(Supplier::get);
  }
}
