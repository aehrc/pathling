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

package au.csiro.pathling.schema.annotation.kinds;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;

/**
 * The canonical form of a quantity: its value expressed in the base units of UCUM, and the code of
 * those units.
 *
 * @author Piotr Szul
 */
public final class CanonicalQuantity {

  @Nonnull private final BigDecimal value;

  @Nonnull private final String code;

  /**
   * Creates a canonical form.
   *
   * @param value the value, in the canonical units
   * @param code the code of the canonical units
   */
  public CanonicalQuantity(@Nonnull final BigDecimal value, @Nonnull final String code) {
    this.value = value;
    this.code = code;
  }

  /**
   * Returns the value, in the canonical units.
   *
   * @return the value, with every digit canonicalisation gave it
   */
  @Nonnull
  public BigDecimal getValue() {
    return value;
  }

  /**
   * Returns the code of the canonical units.
   *
   * @return the code
   */
  @Nonnull
  public String getCode() {
    return code;
  }
}
