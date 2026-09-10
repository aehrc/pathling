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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The rule an import uses to assign dense identifiers to concepts. Dense identifiers are internal
 * to a store, so the choice affects only how compactly the runtime indexes represent themselves.
 *
 * @author John Grimes
 */
public enum DenseIdOrder {

  /**
   * Concept code order. The default, and the only ordering used before this option existed. A
   * concept keeps the same identifier across re-imports of any release that contains it, but a
   * concept's descendants are scattered across the whole identifier range, because a code's numeric
   * value bears no relation to its position in the hierarchy.
   */
  CODE_ORDER("code-order"),

  /**
   * Depth-first pre-order over the active is-a hierarchy. Each subtree occupies a near-contiguous
   * interval, which makes the hierarchy index materially smaller, at the cost of identifiers that
   * shift whenever the hierarchy changes shape between releases.
   */
  PRE_ORDER("pre-order");

  @Nonnull private final String value;

  DenseIdOrder(@Nonnull final String value) {
    this.value = value;
  }

  /**
   * Returns the option's external name, as accepted on the command line and in the language
   * libraries.
   *
   * @return the external name
   */
  @Nonnull
  public String getValue() {
    return value;
  }

  /**
   * Resolves an ordering from its external name.
   *
   * @param value the external name, for example {@code pre-order}
   * @return the matching ordering
   * @throws IllegalArgumentException if the name matches no ordering
   */
  @Nonnull
  public static DenseIdOrder fromValue(@Nonnull final String value) {
    return Arrays.stream(values())
        .filter(order -> order.value.equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unknown dense identifier order '"
                        + value
                        + "', expected one of: "
                        + Arrays.stream(values())
                            .map(DenseIdOrder::getValue)
                            .collect(Collectors.joining(", "))));
  }
}
