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

package au.csiro.pathling.schema.unit;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import lombok.Value;

/**
 * The canonical form of a quantity: its value expressed in the base units of UCUM, and the code of
 * those units.
 *
 * @author Piotr Szul
 */
@Value
public class CanonicalQuantity {

  /** The value, in the canonical units, with every digit canonicalisation gave it. */
  @Nonnull BigDecimal value;

  /** The code of the canonical units. */
  @Nonnull String code;
}
