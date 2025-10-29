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

package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import lombok.Getter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enumeration of valid FHIRPath calendar duration units from year to millisecond.
 */
public enum CalendarDurationUnit {
  YEAR("year", false, "a"),
  MONTH("month", false, "mo"),
  WEEK("week", false, "wk"),
  DAY("day", false, "d"),
  HOUR("hour", false, "h"),
  MINUTE("minute", false, "min"),
  SECOND("second", true, "s"),
  MILLISECOND("millisecond", true, "ms");

  @Nonnull
  private final String unit;

  /**
   * Indicates whether the unitCode is a definite duration (i.e., has a fixed length in time). For
   * example seconds, and milliseconds are definite units, while years and months are not because
   * their lengths can vary (e.g., due to leap years or different month lengths).
   */
  @Getter
  private final boolean definite;

  /**
   * The UCUM equivalent of the calendar duration unitCode.
   */
  @Nonnull
  private final String ucumEquivalent;

  private static final Map<String, CalendarDurationUnit> NAME_MAP = new HashMap<>();

  static {
    for (CalendarDurationUnit unit : values()) {
      NAME_MAP.put(unit.unit, unit);
      NAME_MAP.put(unit.unit + "s", unit); // plural
    }
  }

  private static final Map<String, CalendarDurationUnit> UCUM_EQUIVALENT_MAP = Stream.of(values())
      .collect(
          Collectors.toUnmodifiableMap(CalendarDurationUnit::getUcumEquivalent,
              Function.identity()));


  CalendarDurationUnit(@Nonnull String name, boolean definite, @Nonnull String ucumEquivalent) {
    this.unit = name;
    this.definite = definite;
    this.ucumEquivalent = ucumEquivalent;
  }

  /**
   * Gets the canonical (that is singular) name of the calendar duration unitCode.
   *
   * @return the name of the unitCode
   */
  @Nonnull
  public String getUnit() {
    return unit;
  }

  /**
   * Gets the UCUM equivalent of the calendar duration unitCode.
   *
   * @return the UCUM equivalent string
   */
  @Nonnull
  public String getUcumEquivalent() {
    return ucumEquivalent;
  }

  /**
   * Gets the CalendarDurationUnit from its string representation (case-insensitive, singular or
   * plural).
   *
   * @param name the name of the unitCode (e.g. "year", "years")
   * @return the corresponding CalendarDurationUnit
   * @throws IllegalArgumentException if the name is not valid
   */
  @Nonnull
  public static CalendarDurationUnit parseString(@Nonnull String name) {
    return fromString(name).orElseThrow(
        () -> new IllegalArgumentException("Unknown calendar duration unitCode: " + name));
  }

  /**
   * Gets the CalendarDurationUnit from its string representation (case-insensitive, singular or
   * plural).
   *
   * @param name the name of the unitCode (e.g. "year", "years")
   * @return an Optional containing the corresponding CalendarDurationUnit, or empty if not found
   */
  @Nonnull
  public static Optional<CalendarDurationUnit> fromString(@Nonnull String name) {
    return Optional.ofNullable(NAME_MAP.get(name.toLowerCase(Locale.ROOT)));
  }

  /**
   * Gets the CalendarDurationUnit from its UCUM equivalent code (e.g., "s" for second, "ms" for
   * millisecond).
   *
   * @param name the UCUM code (e.g., "s", "ms", "min", "h", "d", "wk", "mo", "a")
   * @return an Optional containing the corresponding CalendarDurationUnit, or empty if not found
   */
  @Nonnull
  public static Optional<CalendarDurationUnit> fromUcumUnit(@Nonnull String name) {
    return Optional.ofNullable(UCUM_EQUIVALENT_MAP.get(name));
  }


}

