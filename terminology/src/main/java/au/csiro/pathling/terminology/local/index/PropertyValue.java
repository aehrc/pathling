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

package au.csiro.pathling.terminology.local.index;

import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * A single scalar property value of a concept: the property code, the declared FHIR value type
 * ({@code integer}, {@code boolean}, {@code code}, {@code string}, {@code decimal}, or {@code
 * dateTime}), and the value encoded as a string.
 *
 * @author John Grimes
 */
@Value
public class PropertyValue {

  /** The property code. */
  @Nonnull String code;

  /** The declared FHIR value type. */
  @Nonnull String valueType;

  /** The value encoded as a canonical string. */
  @Nonnull String value;
}
