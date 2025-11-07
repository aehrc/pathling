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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.fhirpath.unit.CalendarDurationUnit;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FhirPathQuantityTest {

  static Stream<Arguments> quantityLiterals() {
    return Stream.of(
        // UCUM cases
        Arguments.of("5.4 'mg'", FhirPathQuantity.ofUcum(new BigDecimal("5.4"), "mg")),
        Arguments.of("-2 'kg'", FhirPathQuantity.ofUcum(new BigDecimal("-2"), "kg")),
        Arguments.of("1.0 'mL'", FhirPathQuantity.ofUcum(new BigDecimal("1.0"), "mL")),
        // Calendar duration cases (singular and plural)
        Arguments.of("1 year",
            FhirPathQuantity.ofUnit(new BigDecimal("1"), CalendarDurationUnit.YEAR)),
        Arguments.of("2 years",
            FhirPathQuantity.ofUnit(new BigDecimal("2"), CalendarDurationUnit.YEAR, "years")),
        Arguments.of("3 month",
            FhirPathQuantity.ofUnit(new BigDecimal("3"), CalendarDurationUnit.MONTH)),
        Arguments.of("4 months",
            FhirPathQuantity.ofUnit(new BigDecimal("4"), CalendarDurationUnit.MONTH, "months")),
        Arguments.of("1 week",
            FhirPathQuantity.ofUnit(new BigDecimal("1"), CalendarDurationUnit.WEEK)),
        Arguments.of("2 weeks",
            FhirPathQuantity.ofUnit(new BigDecimal("2"), CalendarDurationUnit.WEEK, "weeks")),
        Arguments.of("5 day",
            FhirPathQuantity.ofUnit(new BigDecimal("5"), CalendarDurationUnit.DAY)),
        Arguments.of("6 days",
            FhirPathQuantity.ofUnit(new BigDecimal("6"), CalendarDurationUnit.DAY, "days")),
        Arguments.of("7 hour",
            FhirPathQuantity.ofUnit(new BigDecimal("7"), CalendarDurationUnit.HOUR)),
        Arguments.of("8 hours",
            FhirPathQuantity.ofUnit(new BigDecimal("8"), CalendarDurationUnit.HOUR, "hours")),
        Arguments.of("9 minute",
            FhirPathQuantity.ofUnit(new BigDecimal("9"), CalendarDurationUnit.MINUTE)),
        Arguments.of("10 minutes",
            FhirPathQuantity.ofUnit(new BigDecimal("10"), CalendarDurationUnit.MINUTE,
                "minutes")),
        Arguments.of("11 second",
            FhirPathQuantity.ofUnit(new BigDecimal("11"), CalendarDurationUnit.SECOND)),
        Arguments.of("12 seconds",
            FhirPathQuantity.ofUnit(new BigDecimal("12"), CalendarDurationUnit.SECOND,
                "seconds")),
        Arguments.of("13 millisecond",
            FhirPathQuantity.ofUnit(new BigDecimal("13"), CalendarDurationUnit.MILLISECOND)),
        Arguments.of("14 milliseconds",
            FhirPathQuantity.ofUnit(new BigDecimal("14"), CalendarDurationUnit.MILLISECOND,
                "milliseconds"))
    );
  }

  @ParameterizedTest
  @MethodSource("quantityLiterals")
  void testParseQuantity(final String literal, final FhirPathQuantity expected) {
    final FhirPathQuantity actual = FhirPathQuantity.parse(literal);
    assertEquals(expected, actual, "Parsed quantity should match expected");
  }

  static Stream<Arguments> calendarDurationConversions() {
    return Stream.of(
        // 1. Identity Conversions - unit to itself (preserves target unit plurality)
        Arguments.of(CalendarDurationUnit.YEAR, "year", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.YEAR, "years", new BigDecimal("2"), new BigDecimal("2")),
        Arguments.of(CalendarDurationUnit.MONTH, "month", new BigDecimal("3"), new BigDecimal("3")),
        Arguments.of(CalendarDurationUnit.WEEK, "week", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "day", new BigDecimal("5"), new BigDecimal("5")),
        Arguments.of(CalendarDurationUnit.HOUR, "hour", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.MINUTE, "minute", new BigDecimal("10"),
            new BigDecimal("10")),
        Arguments.of(CalendarDurationUnit.SECOND, "second", new BigDecimal("5"),
            new BigDecimal("5")),
        Arguments.of(CalendarDurationUnit.MILLISECOND, "millisecond", new BigDecimal("1000"),
            new BigDecimal("1000")),

        // 2. Direct Conversions (one-hop)
        Arguments.of(CalendarDurationUnit.YEAR, "months", new BigDecimal("1"),
            new BigDecimal("12")),
        Arguments.of(CalendarDurationUnit.YEAR, "days", new BigDecimal("1"), new BigDecimal("365")),
        Arguments.of(CalendarDurationUnit.MONTH, "days", new BigDecimal("1"), new BigDecimal("30")),
        Arguments.of(CalendarDurationUnit.WEEK, "days", new BigDecimal("1"), new BigDecimal("7")),
        Arguments.of(CalendarDurationUnit.DAY, "hours", new BigDecimal("1"), new BigDecimal("24")),
        Arguments.of(CalendarDurationUnit.HOUR, "minutes", new BigDecimal("1"),
            new BigDecimal("60")),
        Arguments.of(CalendarDurationUnit.MINUTE, "seconds", new BigDecimal("1"),
            new BigDecimal("60")),
        Arguments.of(CalendarDurationUnit.SECOND, "milliseconds", new BigDecimal("1"),
            new BigDecimal("1000")),

        // 3. Reverse Conversions
        Arguments.of(CalendarDurationUnit.MONTH, "years", new BigDecimal("12"),
            new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "years", new BigDecimal("365"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "months", new BigDecimal("30"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "weeks", new BigDecimal("7"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.HOUR, "days", new BigDecimal("24"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.MINUTE, "hours", new BigDecimal("60"),
            new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.SECOND, "minutes", new BigDecimal("60"),
            new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.MILLISECOND, "seconds", new BigDecimal("1000"),
            new BigDecimal("1")),

        // 4. Transitive Conversions (multi-hop via shortest path)
        // year -> day -> hour
        Arguments.of(CalendarDurationUnit.YEAR, "hours", new BigDecimal("1"),
            new BigDecimal("8760")),
        // year -> day -> hour -> minute
        Arguments.of(CalendarDurationUnit.YEAR, "minutes", new BigDecimal("1"),
            new BigDecimal("525600")),
        // year -> day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.YEAR, "seconds", new BigDecimal("1"),
            new BigDecimal("31536000")),
        // year -> day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.YEAR, "milliseconds", new BigDecimal("1"),
            new BigDecimal("31536000000")),

        // month -> day -> hour
        Arguments.of(CalendarDurationUnit.MONTH, "hours", new BigDecimal("1"),
            new BigDecimal("720")),
        // month -> day -> hour -> minute
        Arguments.of(CalendarDurationUnit.MONTH, "minutes", new BigDecimal("1"),
            new BigDecimal("43200")),
        // month -> day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.MONTH, "seconds", new BigDecimal("1"),
            new BigDecimal("2592000")),
        // month -> day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.MONTH, "milliseconds", new BigDecimal("1"),
            new BigDecimal("2592000000")),

        // week -> day -> hour
        Arguments.of(CalendarDurationUnit.WEEK, "hours", new BigDecimal("1"),
            new BigDecimal("168")),
        // week -> day -> hour -> minute
        Arguments.of(CalendarDurationUnit.WEEK, "minutes", new BigDecimal("1"),
            new BigDecimal("10080")),
        // week -> day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.WEEK, "seconds", new BigDecimal("1"),
            new BigDecimal("604800")),
        // week -> day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.WEEK, "milliseconds", new BigDecimal("1"),
            new BigDecimal("604800000")),

        // day -> hour -> minute
        Arguments.of(CalendarDurationUnit.DAY, "minutes", new BigDecimal("1"),
            new BigDecimal("1440")),
        // day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.DAY, "seconds", new BigDecimal("1"),
            new BigDecimal("86400")),
        // day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.DAY, "milliseconds", new BigDecimal("1"),
            new BigDecimal("86400000")),

        // hour -> minute -> second
        Arguments.of(CalendarDurationUnit.HOUR, "seconds", new BigDecimal("1"),
            new BigDecimal("3600")),
        // hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.HOUR, "milliseconds", new BigDecimal("1"),
            new BigDecimal("3600000")),

        // minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.MINUTE, "milliseconds", new BigDecimal("1"),
            new BigDecimal("60000")),

        // 5. Calendar Duration to UCUM Conversions (only 's' and 'ms' as targets)
        // Direct UCUM conversions for definite duration units
        Arguments.of(CalendarDurationUnit.SECOND, "s", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.SECOND, "ms", new BigDecimal("1"),
            new BigDecimal("1000")),
        Arguments.of(CalendarDurationUnit.MILLISECOND, "ms", new BigDecimal("1500"),
            new BigDecimal("1500")),

        // Transitive Calendar → UCUM (via second/millisecond bridge)
        // All calendar units can convert to 's' via: calendar → second → 's'
        Arguments.of(CalendarDurationUnit.MINUTE, "s", new BigDecimal("2"), new BigDecimal("120")),
        Arguments.of(CalendarDurationUnit.HOUR, "s", new BigDecimal("1"), new BigDecimal("3600")),
        Arguments.of(CalendarDurationUnit.DAY, "s", new BigDecimal("1"), new BigDecimal("86400")),
        Arguments.of(CalendarDurationUnit.WEEK, "s", new BigDecimal("1"), new BigDecimal("604800")),
        Arguments.of(CalendarDurationUnit.MONTH, "s", new BigDecimal("1"),
            new BigDecimal("2592000")),
        Arguments.of(CalendarDurationUnit.YEAR, "s", new BigDecimal("1"),
            new BigDecimal("31536000")),

        // All calendar units can convert to 'ms' via: calendar → millisecond → 'ms'
        Arguments.of(CalendarDurationUnit.MINUTE, "ms", new BigDecimal("1"),
            new BigDecimal("60000")),
        Arguments.of(CalendarDurationUnit.HOUR, "ms", new BigDecimal("1"),
            new BigDecimal("3600000")),
        Arguments.of(CalendarDurationUnit.DAY, "ms", new BigDecimal("1"),
            new BigDecimal("86400000")),

        // Transitive Calendar → UCUM via 's' bridge: calendar → second → 's' → UCUM time unit
        // NOTE: Calendar durations and UCUM time units have different definitions!
        // - Calendar minute = 60s, UCUM 'min' = 60s → 1:1 conversion ✓
        // - Calendar hour = 3600s, UCUM 'h' = 3600s → 1:1 conversion ✓
        // - Calendar day = 86400s, UCUM 'd' = 86400s → 1:1 conversion ✓
        // - Calendar week = 7d, UCUM 'wk' = 7d → 1:1 conversion ✓
        // - Calendar month = 30d = 2592000s, UCUM 'mo' = 30.4375d = 2629800s → NOT 1:1 but convertible
        // - Calendar year = 365d = 31536000s, UCUM 'a' = 365.25d = 31557600s → NOT 1:1 but convertible

        Arguments.of(CalendarDurationUnit.MINUTE, "min", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.HOUR, "h", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "d", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.WEEK, "wk", new BigDecimal("1"), new BigDecimal("1")),

        // Calendar month/year to UCUM with non-1:1 factors
        // 1 calendar month (2592000s) to 'mo' (2629800s): factor = 2592000/2629800 (15 decimals)
        Arguments.of(CalendarDurationUnit.MONTH, "mo", new BigDecimal("1"),
            new BigDecimal("0.985626283367556")),

        // 1 calendar year (31536000s) to 'a' (31557600s): factor = 31536000/31557600 (15 decimals)
        Arguments.of(CalendarDurationUnit.YEAR, "a", new BigDecimal("1"),
            new BigDecimal("0.999315537303217")),

        // 1 calendar year to UCUM 'mo': 31536000s / 2629800s (15 decimals)
        Arguments.of(CalendarDurationUnit.YEAR, "mo", new BigDecimal("1"),
            new BigDecimal("11.991786447638604")),

        // Additional cross-unit UCUM conversions via 's' bridge
        Arguments.of(CalendarDurationUnit.HOUR, "min", new BigDecimal("1"), new BigDecimal("60")),
        Arguments.of(CalendarDurationUnit.DAY, "h", new BigDecimal("1"), new BigDecimal("24")),
        Arguments.of(CalendarDurationUnit.WEEK, "d", new BigDecimal("1"), new BigDecimal("7"))
    );
  }

  @ParameterizedTest
  @MethodSource("calendarDurationConversions")
  void testCalendarDurationConversion(final CalendarDurationUnit sourceUnit,
      final String targetUnit,
      final BigDecimal sourceValue, final BigDecimal expectedValue) {
    final FhirPathQuantity source = FhirPathQuantity.ofUnit(sourceValue, sourceUnit);
    final FhirPathQuantity result = source.convertToUnit(targetUnit).orElse(null);

    // Compare values with compareTo for proper BigDecimal equality (ignoring scale)
    assertNotNull(result);
    assertEquals(0, expectedValue.compareTo(result.getValue()),
        String.format("Converting %s %s to %s should produce %s, but got %s",
            sourceValue, sourceUnit.code(), targetUnit, expectedValue, result.getValue()));

    // Check that result has correct system and unit
    // Determine if target is calendar duration or UCUM by attempting to parse
    final boolean isCalendarDuration = CalendarDurationUnit.fromString(targetUnit).isPresent();
    if (isCalendarDuration) {
      // Calendar duration conversion
      assertEquals(CalendarDurationUnit.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI, result.getSystem(),
          "Result should be calendar duration system");
      assertEquals(targetUnit, result.getUnitName(), "Result unit should be " + targetUnit);
    } else {
      // UCUM conversion (includes 's', 'ms', 'min', 'h', 'd', 'wk', 'mo', 'a', etc.)
      assertEquals(UcumUnit.UCUM_SYSTEM_URI, result.getSystem(),
          "Result should be UCUM system");
      assertEquals(targetUnit, result.getUnitName(), "Result unit should be " + targetUnit);
    }
  }

  static Stream<Arguments> ucumToCalendarDurationConversions() {
    return Stream.of(
        // 6. UCUM to Calendar Duration Conversions (reflexive of calendar → UCUM)
        // Direct conversions: 's' ↔ 'second', 'ms' ↔ 'millisecond'
        Arguments.of("s", "second", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of("s", "seconds", new BigDecimal("5"), new BigDecimal("5")),
        Arguments.of("ms", "millisecond", new BigDecimal("1000"), new BigDecimal("1000")),
        Arguments.of("ms", "milliseconds", new BigDecimal("500"), new BigDecimal("500")),

        // Transitive UCUM → Calendar via 's' bridge: UCUM → 's' → 'second' → calendar unit
        // 's' → 'second' → 'minute'
        Arguments.of("s", "minute", new BigDecimal("60"), new BigDecimal("1")),
        Arguments.of("s", "minutes", new BigDecimal("120"), new BigDecimal("2")),
        // 's' → 'second' → 'hour'
        Arguments.of("s", "hour", new BigDecimal("3600"), new BigDecimal("1")),
        Arguments.of("s", "hours", new BigDecimal("7200"), new BigDecimal("2")),
        // 's' → 'second' → 'day'
        Arguments.of("s", "day", new BigDecimal("86400"), new BigDecimal("1")),
        Arguments.of("s", "days", new BigDecimal("172800"), new BigDecimal("2")),
        // 's' → 'second' → 'week'
        Arguments.of("s", "week", new BigDecimal("604800"), new BigDecimal("1")),
        Arguments.of("s", "weeks", new BigDecimal("1209600"), new BigDecimal("2")),
        // 's' → 'second' → 'month'
        Arguments.of("s", "month", new BigDecimal("2592000"), new BigDecimal("1")),
        Arguments.of("s", "months", new BigDecimal("5184000"), new BigDecimal("2")),
        // 's' → 'second' → 'year'
        Arguments.of("s", "year", new BigDecimal("31536000"), new BigDecimal("1")),
        Arguments.of("s", "years", new BigDecimal("63072000"), new BigDecimal("2")),

        // Transitive UCUM → Calendar via 'ms' bridge: UCUM → 'ms' → 'millisecond' → calendar unit
        // 'ms' → 'millisecond' → 'second'
        Arguments.of("ms", "second", new BigDecimal("1000"), new BigDecimal("1")),
        Arguments.of("ms", "seconds", new BigDecimal("5000"), new BigDecimal("5")),
        // 'ms' → 'millisecond' → 'minute'
        Arguments.of("ms", "minute", new BigDecimal("60000"), new BigDecimal("1")),
        Arguments.of("ms", "minutes", new BigDecimal("120000"), new BigDecimal("2")),
        // 'ms' → 'millisecond' → 'hour'
        Arguments.of("ms", "hour", new BigDecimal("3600000"), new BigDecimal("1")),
        // 'ms' → 'millisecond' → 'day'
        Arguments.of("ms", "day", new BigDecimal("86400000"), new BigDecimal("1")),

        // Transitive UCUM time unit → Calendar via 's' bridge: UCUM → 's' → 'second' → calendar
        // NOTE: Only UCUM units with same definition as calendar can convert 1:1

        // UCUM 'min' → 's' → 'second' → calendar units (1:1 since both = 60s)
        Arguments.of("min", "minute", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of("min", "minutes", new BigDecimal("2"), new BigDecimal("2")),
        Arguments.of("min", "second", new BigDecimal("1"), new BigDecimal("60")),
        Arguments.of("min", "hour", new BigDecimal("60"), new BigDecimal("1")),

        // UCUM 'h' → 's' → 'second' → calendar units (1:1 since both = 3600s)
        Arguments.of("h", "hour", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of("h", "hours", new BigDecimal("3"), new BigDecimal("3")),
        Arguments.of("h", "minute", new BigDecimal("1"), new BigDecimal("60")),
        Arguments.of("h", "day", new BigDecimal("24"), new BigDecimal("1")),

        // UCUM 'd' → 's' → 'second' → calendar units (1:1 since both = 86400s)
        Arguments.of("d", "day", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of("d", "days", new BigDecimal("5"), new BigDecimal("5")),
        Arguments.of("d", "hour", new BigDecimal("1"), new BigDecimal("24")),
        Arguments.of("d", "week", new BigDecimal("7"), new BigDecimal("1")),

        // UCUM 'wk' → 's' → 'second' → calendar units (1:1 since both = 7d)
        Arguments.of("wk", "week", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of("wk", "weeks", new BigDecimal("2"), new BigDecimal("2")),
        Arguments.of("wk", "day", new BigDecimal("1"), new BigDecimal("7")),

        // UCUM 'mo' → 's' → 'second' → calendar units (NOT 1:1 but convertible)
        // 1 'mo' (2629800s) to calendar month (2592000s): factor = 2629800/2592000 (15 decimals)
        Arguments.of("mo", "month", new BigDecimal("1"), new BigDecimal("1.014583333333333")),
        Arguments.of("mo", "months", new BigDecimal("2"), new BigDecimal("2.029166666666667")),
        // 1 'mo' to calendar day: 2629800 / 86400 = 30.4375
        Arguments.of("mo", "day", new BigDecimal("1"), new BigDecimal("30.4375")),

        // UCUM 'a' → 's' → 'second' → calendar units (NOT 1:1 but convertible)
        // 1 'a' (31557600s) to calendar year (31536000s): factor = 31557600/31536000 (15 decimals)
        Arguments.of("a", "year", new BigDecimal("1"), new BigDecimal("1.000684931506849")),
        Arguments.of("a", "years", new BigDecimal("2"), new BigDecimal("2.001369863013699")),
        // 1 'a' to calendar month: 31557600 / 2592000 = 12.175
        Arguments.of("a", "month", new BigDecimal("1"), new BigDecimal("12.175")),
        // 1 'a' to calendar day: 31557600 / 86400 = 365.25
        Arguments.of("a", "day", new BigDecimal("1"), new BigDecimal("365.25"))
    );
  }

  @ParameterizedTest
  @MethodSource("ucumToCalendarDurationConversions")
  void testUcumToCalendarDurationConversion(final String sourceUcumUnit,
      final String targetCalendarUnit,
      final BigDecimal sourceValue, final BigDecimal expectedValue) {
    final FhirPathQuantity source = FhirPathQuantity.ofUcum(sourceValue, sourceUcumUnit);
    final FhirPathQuantity result = source.convertToUnit(targetCalendarUnit).orElse(null);

    // Compare values with compareTo for proper BigDecimal equality (ignoring scale)
    assertNotNull(result);
    assertEquals(0, expectedValue.compareTo(result.getValue()),
        String.format("Converting %s '%s' to %s should produce %s, but got %s",
            sourceValue, sourceUcumUnit, targetCalendarUnit, expectedValue, result.getValue()));

    // Check that result has correct system and unit
    assertEquals(CalendarDurationUnit.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI, result.getSystem(),
        "Result should be calendar duration system");
    assertEquals(targetCalendarUnit, result.getUnitName(),
        "Result unit should be " + targetCalendarUnit);
  }

  static Stream<Arguments> unsupportedCalendarDurationConversions() {
    return Stream.of(
        // Blocked Conversions (week <-> month/year explicitly disallowed per FHIRPath spec)
        Arguments.of(CalendarDurationUnit.WEEK, "months"),
        Arguments.of(CalendarDurationUnit.MONTH, "weeks"),
        Arguments.of(CalendarDurationUnit.WEEK, "years"),
        Arguments.of(CalendarDurationUnit.YEAR, "weeks"),

        // Invalid targets (non-time units)
        Arguments.of(CalendarDurationUnit.DAY, "kg"),       // kilogram - mass unit
        Arguments.of(CalendarDurationUnit.HOUR, "L"),       // liter - volume unit
        Arguments.of(CalendarDurationUnit.SECOND, "m"),     // meter - length unit
        Arguments.of(CalendarDurationUnit.MINUTE, "g"),     // gram - mass unit
        Arguments.of(CalendarDurationUnit.HOUR, "invalid_unit")  // non-existent unit
    );
  }

  @ParameterizedTest
  @MethodSource("unsupportedCalendarDurationConversions")
  void testUnsupportedCalendarDurationConversion(final CalendarDurationUnit sourceUnit,
      final String targetUnit) {
    final FhirPathQuantity source = FhirPathQuantity.ofUnit(new BigDecimal("1"), sourceUnit);
    @Nullable final FhirPathQuantity result = source.convertToUnit(targetUnit).orElse(null);

    assertNull(result,
        String.format("Converting %s to %s should return null (not supported)",
            sourceUnit.code(), targetUnit));
  }

  static Stream<Arguments> unsupportedUcumToCalendarConversions() {
    return Stream.of(
        // Non-time UCUM units to calendar durations (invalid - wrong dimension)
        Arguments.of("kg", "second"),        // kilogram - mass unit
        Arguments.of("g", "minute"),         // gram - mass unit
        Arguments.of("m", "hour"),           // meter - length unit
        Arguments.of("cm", "day"),           // centimeter - length unit
        Arguments.of("L", "day"),            // liter - volume unit
        Arguments.of("mL", "hour"),          // milliliter - volume unit
        Arguments.of("invalid_unit", "minute"), // non-existent unit
        Arguments.of("xyz", "second")        // non-existent unit
    );
  }

  @ParameterizedTest
  @MethodSource("unsupportedUcumToCalendarConversions")
  void testUnsupportedUcumToCalendarConversion(final String sourceUcumUnit,
      final String targetCalendarUnit) {
    final FhirPathQuantity source = FhirPathQuantity.ofUcum(new BigDecimal("1"), sourceUcumUnit);
    @Nullable final FhirPathQuantity result = source.convertToUnit(targetCalendarUnit).orElse(null);

    assertNull(result,
        String.format("Converting UCUM '%s' to calendar '%s' should return null (not supported)",
            sourceUcumUnit, targetCalendarUnit));
  }

  static Stream<Arguments> ucumConversions() {
    return Stream.of(
        // Multiplicative conversions (simple scaling factor)
        Arguments.of("kg", "g", new BigDecimal("1"), new BigDecimal("1000")),
        Arguments.of("g", "kg", new BigDecimal("2000"), new BigDecimal("2")),

        // Additive conversions (offset-based)
        Arguments.of("Cel", "K", new BigDecimal("0"), new BigDecimal("273.15")),
        Arguments.of("K", "Cel", new BigDecimal("373.15"), new BigDecimal("100"))
    );
  }

  @ParameterizedTest
  @MethodSource("ucumConversions")
  void testUcumConversion(final String sourceUnit, final String targetUnit,
      final BigDecimal sourceValue, final BigDecimal expectedValue) {
    final FhirPathQuantity source = FhirPathQuantity.ofUcum(sourceValue, sourceUnit);
    final FhirPathQuantity result = source.convertToUnit(targetUnit).orElse(null);

    assertNotNull(result,
        String.format("Converting %s '%s' to '%s' should succeed",
            sourceValue, sourceUnit, targetUnit));
    assertEquals(0, expectedValue.compareTo(result.getValue()),
        String.format("Converting %s '%s' to '%s' should produce %s, but got %s",
            sourceValue, sourceUnit, targetUnit, expectedValue, result.getValue()));

    // Check that result has correct system and unit
    assertEquals(UcumUnit.UCUM_SYSTEM_URI, result.getSystem(),
        "Result should be UCUM system");
    assertEquals(targetUnit, result.getUnitName(), "Result unit should be " + targetUnit);
  }

  static Stream<Arguments> ucumCanonicalization() {
    return Stream.of(
        // Multiplicative conversions to canonical units
        Arguments.of("kg", new BigDecimal("2"), "g", new BigDecimal("2000")),
        Arguments.of("cm", new BigDecimal("100"), "m", new BigDecimal("1")),
        Arguments.of("L", new BigDecimal("500"), "m+3", new BigDecimal("0.5")),

        // Additive conversion to canonical unit (Celsius -> Kelvin)
        Arguments.of("Cel", new BigDecimal("100"), "K", new BigDecimal("373.15"))
    );
  }

  @ParameterizedTest
  @MethodSource("ucumCanonicalization")
  void testUcumCanonicalization(final String sourceUnit, final BigDecimal sourceValue,
      final String expectedCanonicalUnit, final BigDecimal expectedCanonicalValue) {
    final FhirPathQuantity source = FhirPathQuantity.ofUcum(sourceValue, sourceUnit);
    final FhirPathQuantity canonical = source.asCanonical().orElse(null);

    assertNotNull(canonical,
        String.format("Canonicalizing %s '%s' should succeed", sourceValue, sourceUnit));
    assertEquals(expectedCanonicalUnit, canonical.getUnitName(),
        String.format("Canonical unit should be '%s'", expectedCanonicalUnit));
    assertEquals(0, expectedCanonicalValue.compareTo(canonical.getValue()),
        String.format("Canonical value should be %s, but got %s",
            expectedCanonicalValue, canonical.getValue()));
    assertEquals(UcumUnit.UCUM_SYSTEM_URI, canonical.getSystem(),
        "Canonical quantity should be UCUM system");
  }

  static Stream<Arguments> calendarDurationCanonicalization() {
    return Stream.of(
        // Only definite calendar durations (second and millisecond) can be canonicalized to UCUM
        Arguments.of(CalendarDurationUnit.SECOND, new BigDecimal("5"), "s", new BigDecimal("5")),
        Arguments.of(CalendarDurationUnit.MILLISECOND, new BigDecimal("1000"), "s",
            new BigDecimal("1"))
    );
  }

  @ParameterizedTest
  @MethodSource("calendarDurationCanonicalization")
  void testCalendarDurationCanonicalization(final CalendarDurationUnit sourceUnit,
      final BigDecimal sourceValue,
      final String expectedCanonicalUnit, final BigDecimal expectedCanonicalValue) {
    final FhirPathQuantity source = FhirPathQuantity.ofUnit(sourceValue, sourceUnit);
    final FhirPathQuantity canonical = source.asCanonical().orElse(null);

    assertNotNull(canonical,
        String.format("Canonicalizing %s %s should succeed", sourceValue, sourceUnit.code()));
    assertEquals(expectedCanonicalUnit, canonical.getUnitName(),
        String.format("Canonical unit should be '%s'", expectedCanonicalUnit));
    assertEquals(0, expectedCanonicalValue.compareTo(canonical.getValue()),
        String.format("Canonical value should be %s, but got %s",
            expectedCanonicalValue, canonical.getValue()));
    assertEquals(UcumUnit.UCUM_SYSTEM_URI, canonical.getSystem(),
        "Canonical quantity should be UCUM system");
  }

  static Stream<Arguments> indefiniteCalendarDurationCanonicalization() {
    return Stream.of(
        // Indefinite calendar durations cannot be canonicalized
        Arguments.of(CalendarDurationUnit.MINUTE, new BigDecimal("2")),
        Arguments.of(CalendarDurationUnit.HOUR, new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.WEEK, new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.MONTH, new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.YEAR, new BigDecimal("1"))
    );
  }

  @ParameterizedTest
  @MethodSource("indefiniteCalendarDurationCanonicalization")
  void testIndefiniteCalendarDurationCanonicalization(final CalendarDurationUnit sourceUnit,
      final BigDecimal sourceValue) {
    final FhirPathQuantity source = FhirPathQuantity.ofUnit(sourceValue, sourceUnit);
    @Nullable final FhirPathQuantity canonical = source.asCanonical().orElse(null);

    assertNull(canonical,
        String.format("Canonicalizing indefinite duration %s %s should return null",
            sourceValue, sourceUnit.code()));
  }
}
