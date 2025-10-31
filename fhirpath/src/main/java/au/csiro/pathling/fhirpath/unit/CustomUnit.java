/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.unit;

import jakarta.annotation.Nonnull;

/**
 * Represents a custom unit from a system other than UCUM or calendar durations.
 * <p>
 * Custom units are used when a quantity has a unit from a coding system that is neither UCUM
 * nor the FHIRPath calendar duration system. This allows for representing domain-specific or
 * proprietary units of measure that may not have standard conversions.
 * <p>
 * Unlike {@link UcumUnit} and {@link CalendarDurationUnit}, custom units do not support
 * automatic unit conversion, as their semantics are defined by their respective coding systems
 * and may not have well-defined conversion factors.
 * <p>
 * Example custom units might include:
 * <ul>
 *   <li>Institution-specific units for lab values</li>
 *   <li>Industry-specific measurement systems</li>
 *   <li>Legacy or proprietary unit codes</li>
 * </ul>
 *
 * @param system the system URI for this custom unit
 * @param code the code within that system
 */
public record CustomUnit(
    @Nonnull String system,
    @Nonnull String code
) implements FhirPathUnit {

}
