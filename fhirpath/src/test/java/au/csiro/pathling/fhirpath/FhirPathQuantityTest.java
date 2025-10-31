package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.util.stream.Stream;
import au.csiro.pathling.fhirpath.unit.CalendarDurationUnit;
import jakarta.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FhirPathQuantityTest {

  static Stream<Arguments> quantityLiterals() {
    return Stream.of(
        // UCUM cases
        Arguments.of("5.4 'mg'", FhirPathQuantity.ofUCUM(new BigDecimal("5.4"), "mg")),
        Arguments.of("-2 'kg'", FhirPathQuantity.ofUCUM(new BigDecimal("-2"), "kg")),
        Arguments.of("1.0 'mL'", FhirPathQuantity.ofUCUM(new BigDecimal("1.0"), "mL")),
        // Calendar duration cases (singular and plural)
        Arguments.of("1 year",
            FhirPathQuantity.ofCalendar(new BigDecimal("1"), CalendarDurationUnit.YEAR)),
        Arguments.of("2 years",
            FhirPathQuantity.ofCalendar(new BigDecimal("2"), CalendarDurationUnit.YEAR, "years")),
        Arguments.of("3 month",
            FhirPathQuantity.ofCalendar(new BigDecimal("3"), CalendarDurationUnit.MONTH)),
        Arguments.of("4 months",
            FhirPathQuantity.ofCalendar(new BigDecimal("4"), CalendarDurationUnit.MONTH, "months")),
        Arguments.of("1 week",
            FhirPathQuantity.ofCalendar(new BigDecimal("1"), CalendarDurationUnit.WEEK)),
        Arguments.of("2 weeks",
            FhirPathQuantity.ofCalendar(new BigDecimal("2"), CalendarDurationUnit.WEEK, "weeks")),
        Arguments.of("5 day",
            FhirPathQuantity.ofCalendar(new BigDecimal("5"), CalendarDurationUnit.DAY)),
        Arguments.of("6 days",
            FhirPathQuantity.ofCalendar(new BigDecimal("6"), CalendarDurationUnit.DAY, "days")),
        Arguments.of("7 hour",
            FhirPathQuantity.ofCalendar(new BigDecimal("7"), CalendarDurationUnit.HOUR)),
        Arguments.of("8 hours",
            FhirPathQuantity.ofCalendar(new BigDecimal("8"), CalendarDurationUnit.HOUR, "hours")),
        Arguments.of("9 minute",
            FhirPathQuantity.ofCalendar(new BigDecimal("9"), CalendarDurationUnit.MINUTE)),
        Arguments.of("10 minutes",
            FhirPathQuantity.ofCalendar(new BigDecimal("10"), CalendarDurationUnit.MINUTE,
                "minutes")),
        Arguments.of("11 second",
            FhirPathQuantity.ofCalendar(new BigDecimal("11"), CalendarDurationUnit.SECOND)),
        Arguments.of("12 seconds",
            FhirPathQuantity.ofCalendar(new BigDecimal("12"), CalendarDurationUnit.SECOND,
                "seconds")),
        Arguments.of("13 millisecond",
            FhirPathQuantity.ofCalendar(new BigDecimal("13"), CalendarDurationUnit.MILLISECOND)),
        Arguments.of("14 milliseconds",
            FhirPathQuantity.ofCalendar(new BigDecimal("14"), CalendarDurationUnit.MILLISECOND,
                "milliseconds"))
    );
  }

  @ParameterizedTest
  @MethodSource("quantityLiterals")
  void testParseQuantity(String literal, FhirPathQuantity expected) {
    FhirPathQuantity actual = FhirPathQuantity.parse(literal);
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
        Arguments.of(CalendarDurationUnit.MINUTE, "minute", new BigDecimal("10"), new BigDecimal("10")),
        Arguments.of(CalendarDurationUnit.SECOND, "second", new BigDecimal("5"), new BigDecimal("5")),
        Arguments.of(CalendarDurationUnit.MILLISECOND, "millisecond", new BigDecimal("1000"), new BigDecimal("1000")),

        // 2. Direct Conversions (one-hop)
        Arguments.of(CalendarDurationUnit.YEAR, "months", new BigDecimal("1"), new BigDecimal("12")),
        Arguments.of(CalendarDurationUnit.YEAR, "days", new BigDecimal("1"), new BigDecimal("365")),
        Arguments.of(CalendarDurationUnit.MONTH, "days", new BigDecimal("1"), new BigDecimal("30")),
        Arguments.of(CalendarDurationUnit.WEEK, "days", new BigDecimal("1"), new BigDecimal("7")),
        Arguments.of(CalendarDurationUnit.DAY, "hours", new BigDecimal("1"), new BigDecimal("24")),
        Arguments.of(CalendarDurationUnit.HOUR, "minutes", new BigDecimal("1"), new BigDecimal("60")),
        Arguments.of(CalendarDurationUnit.MINUTE, "seconds", new BigDecimal("1"), new BigDecimal("60")),
        Arguments.of(CalendarDurationUnit.SECOND, "milliseconds", new BigDecimal("1"), new BigDecimal("1000")),

        // 3. Reverse Conversions
        Arguments.of(CalendarDurationUnit.MONTH, "years", new BigDecimal("12"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "years", new BigDecimal("365"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "months", new BigDecimal("30"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.DAY, "weeks", new BigDecimal("7"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.HOUR, "days", new BigDecimal("24"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.MINUTE, "hours", new BigDecimal("60"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.SECOND, "minutes", new BigDecimal("60"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.MILLISECOND, "seconds", new BigDecimal("1000"), new BigDecimal("1")),

        // 4. Transitive Conversions (multi-hop via shortest path)
        // year -> day -> hour
        Arguments.of(CalendarDurationUnit.YEAR, "hours", new BigDecimal("1"), new BigDecimal("8760")),
        // year -> day -> hour -> minute
        Arguments.of(CalendarDurationUnit.YEAR, "minutes", new BigDecimal("1"), new BigDecimal("525600")),
        // year -> day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.YEAR, "seconds", new BigDecimal("1"), new BigDecimal("31536000")),
        // year -> day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.YEAR, "milliseconds", new BigDecimal("1"), new BigDecimal("31536000000")),

        // month -> day -> hour
        Arguments.of(CalendarDurationUnit.MONTH, "hours", new BigDecimal("1"), new BigDecimal("720")),
        // month -> day -> hour -> minute
        Arguments.of(CalendarDurationUnit.MONTH, "minutes", new BigDecimal("1"), new BigDecimal("43200")),
        // month -> day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.MONTH, "seconds", new BigDecimal("1"), new BigDecimal("2592000")),
        // month -> day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.MONTH, "milliseconds", new BigDecimal("1"), new BigDecimal("2592000000")),

        // week -> day -> hour
        Arguments.of(CalendarDurationUnit.WEEK, "hours", new BigDecimal("1"), new BigDecimal("168")),
        // week -> day -> hour -> minute
        Arguments.of(CalendarDurationUnit.WEEK, "minutes", new BigDecimal("1"), new BigDecimal("10080")),
        // week -> day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.WEEK, "seconds", new BigDecimal("1"), new BigDecimal("604800")),
        // week -> day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.WEEK, "milliseconds", new BigDecimal("1"), new BigDecimal("604800000")),

        // day -> hour -> minute
        Arguments.of(CalendarDurationUnit.DAY, "minutes", new BigDecimal("1"), new BigDecimal("1440")),
        // day -> hour -> minute -> second
        Arguments.of(CalendarDurationUnit.DAY, "seconds", new BigDecimal("1"), new BigDecimal("86400")),
        // day -> hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.DAY, "milliseconds", new BigDecimal("1"), new BigDecimal("86400000")),

        // hour -> minute -> second
        Arguments.of(CalendarDurationUnit.HOUR, "seconds", new BigDecimal("1"), new BigDecimal("3600")),
        // hour -> minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.HOUR, "milliseconds", new BigDecimal("1"), new BigDecimal("3600000")),

        // minute -> second -> millisecond
        Arguments.of(CalendarDurationUnit.MINUTE, "milliseconds", new BigDecimal("1"), new BigDecimal("60000")),

        // 6. Calendar Duration to UCUM Conversions (only for second, millisecond)
        // Direct UCUM conversions
        Arguments.of(CalendarDurationUnit.SECOND, "s", new BigDecimal("1"), new BigDecimal("1")),
        Arguments.of(CalendarDurationUnit.SECOND, "ms", new BigDecimal("1"), new BigDecimal("1000")),
        Arguments.of(CalendarDurationUnit.MILLISECOND, "ms", new BigDecimal("1500"), new BigDecimal("1500")),

        // Transitive to UCUM (via second/millisecond conversion first)
        Arguments.of(CalendarDurationUnit.MINUTE, "s", new BigDecimal("2"), new BigDecimal("120")),
        Arguments.of(CalendarDurationUnit.HOUR, "s", new BigDecimal("1"), new BigDecimal("3600")),
        Arguments.of(CalendarDurationUnit.DAY, "s", new BigDecimal("1"), new BigDecimal("86400")),
        Arguments.of(CalendarDurationUnit.WEEK, "s", new BigDecimal("1"), new BigDecimal("604800")),
        Arguments.of(CalendarDurationUnit.MONTH, "s", new BigDecimal("1"), new BigDecimal("2592000")),
        Arguments.of(CalendarDurationUnit.YEAR, "s", new BigDecimal("1"), new BigDecimal("31536000")),

        Arguments.of(CalendarDurationUnit.MINUTE, "ms", new BigDecimal("1"), new BigDecimal("60000")),
        Arguments.of(CalendarDurationUnit.HOUR, "ms", new BigDecimal("1"), new BigDecimal("3600000")),
        Arguments.of(CalendarDurationUnit.DAY, "ms", new BigDecimal("1"), new BigDecimal("86400000"))
    );
  }

  @ParameterizedTest
  @MethodSource("calendarDurationConversions")
  void testCalendarDurationConversion(CalendarDurationUnit sourceUnit, String targetUnit,
      BigDecimal sourceValue, BigDecimal expectedValue) {
    FhirPathQuantity source = FhirPathQuantity.ofCalendar(sourceValue, sourceUnit);
    FhirPathQuantity result = source.convertToUnit(targetUnit).orElse(null);

    // Compare values with compareTo for proper BigDecimal equality (ignoring scale)
    assertEquals(0, expectedValue.compareTo(result.getValue()),
        String.format("Converting %s %s to %s should produce %s, but got %s",
            sourceValue, sourceUnit.code(), targetUnit, expectedValue, result.getValue()));

    // Check that result has correct unitCode
    if (targetUnit.equals("s") || targetUnit.equals("ms")) {
      // UCUM conversion
      assertEquals(FhirPathQuantity.UCUM_SYSTEM_URI, result.getSystem(),
          "Result should be UCUM system");
      assertEquals(targetUnit, result.getUnit(), "Result unitCode should be " + targetUnit);
    } else {
      // Calendar duration conversion - result uses targetUnit string as-is
      assertEquals(FhirPathQuantity.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI, result.getSystem(),
          "Result should be calendar duration system");
      assertEquals(targetUnit, result.getUnit(), "Result unitCode should be " + targetUnit);
    }
  }

  static Stream<Arguments> unsupportedCalendarDurationConversions() {
    return Stream.of(
        // 5. Blocked Conversions (week <-> month/year explicitly disallowed)
        Arguments.of(CalendarDurationUnit.WEEK, "months"),
        Arguments.of(CalendarDurationUnit.MONTH, "weeks"),
        Arguments.of(CalendarDurationUnit.WEEK, "years"),
        Arguments.of(CalendarDurationUnit.YEAR, "weeks"),

        // Calendar duration to UCUM (only 's' and 'ms' allowed as targets)
        Arguments.of(CalendarDurationUnit.DAY, "h"),
        Arguments.of(CalendarDurationUnit.HOUR, "min"),
        Arguments.of(CalendarDurationUnit.WEEK, "d"),
        Arguments.of(CalendarDurationUnit.YEAR, "a"),
        Arguments.of(CalendarDurationUnit.MONTH, "mo"),
        Arguments.of(CalendarDurationUnit.MINUTE, "min"),

        // Invalid targets (non-time units)
        Arguments.of(CalendarDurationUnit.DAY, "kg"),
        Arguments.of(CalendarDurationUnit.HOUR, "invalid_unit"),
        Arguments.of(CalendarDurationUnit.SECOND, "m") // 'm' is meter, not a time unit
    );
  }

  @ParameterizedTest
  @MethodSource("unsupportedCalendarDurationConversions")
  void testUnsupportedCalendarDurationConversion(CalendarDurationUnit sourceUnit,
      String targetUnit) {
    FhirPathQuantity source = FhirPathQuantity.ofCalendar(new BigDecimal("1"), sourceUnit);
    @Nullable
    FhirPathQuantity result = source.convertToUnit(targetUnit).orElse(null);

    assertNull(result,
        String.format("Converting %s to %s should return null (not supported)",
            sourceUnit.code(), targetUnit));
  }
}
