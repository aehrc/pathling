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

package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.time.temporal.ChronoUnit;
import lombok.Getter;

/** Enumeration of supported temporal precision levels from year to millisecond. */
public enum TemporalPrecision {
  /** Year precision (e.g., 2023) */
  YEAR(ChronoUnit.YEARS),

  /** Month precision (e.g., 2023-06) */
  MONTH(ChronoUnit.MONTHS),

  /** Day precision (e.g., 2023-06-15) */
  DAY(ChronoUnit.DAYS),

  /** Hour precision (e.g., 2023-06-15T14) */
  HOUR(ChronoUnit.HOURS),

  /** Minute precision (e.g., 2023-06-15T14:30) */
  MINUTE(ChronoUnit.MINUTES),

  /** Second precision (e.g., 2023-06-15T14:30:45) */
  SECOND(ChronoUnit.SECONDS),

  /** Up to nanoseconds precision (e.g., 2023-06-15T14:30:45.123456789) */
  FRACS(ChronoUnit.NANOS);

  @Getter @Nonnull private final ChronoUnit chronoUnit;

  TemporalPrecision(@Nonnull final ChronoUnit chronoUnit) {
    this.chronoUnit = chronoUnit;
  }

  /**
   * Returns true if the underlying ChronoUnit is time-based.
   *
   * @return true if the underlying ChronoUnit is time-based, false otherwise
   */
  public boolean isTimeBased() {
    return chronoUnit.isTimeBased();
  }
}
