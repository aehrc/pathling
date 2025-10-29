package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.util.stream.Stream;
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
        // Conversions to 'second' (singular) - all calendar durations should convert
        Arguments.of(CalendarDurationUnit.SECOND, "second", new BigDecimal("5"), new BigDecimal("5")),
        Arguments.of(CalendarDurationUnit.MINUTE, "second", new BigDecimal("2"), new BigDecimal("120")),
        Arguments.of(CalendarDurationUnit.HOUR, "second", new BigDecimal("1"), new BigDecimal("3600")),
        Arguments.of(CalendarDurationUnit.DAY, "second", new BigDecimal("1"), new BigDecimal("86400")),
        Arguments.of(CalendarDurationUnit.WEEK, "second", new BigDecimal("1"), new BigDecimal("604800")),
        Arguments.of(CalendarDurationUnit.MONTH, "second", new BigDecimal("1"), new BigDecimal("2592000")),
        Arguments.of(CalendarDurationUnit.YEAR, "second", new BigDecimal("1"), new BigDecimal("31536000")),

        // Conversions to 'millisecond' (singular) - all calendar durations should convert
        Arguments.of(CalendarDurationUnit.MILLISECOND, "millisecond", new BigDecimal("1000"), new BigDecimal("1000")),
        Arguments.of(CalendarDurationUnit.SECOND, "millisecond", new BigDecimal("5"), new BigDecimal("5000")),
        Arguments.of(CalendarDurationUnit.MINUTE, "millisecond", new BigDecimal("4"), new BigDecimal("240000")),
        Arguments.of(CalendarDurationUnit.DAY, "millisecond", new BigDecimal("1"), new BigDecimal("86400000")),

        // Conversions to UCUM 's' - all calendar durations should convert
        Arguments.of(CalendarDurationUnit.SECOND, "s", new BigDecimal("5"), new BigDecimal("5")),
        Arguments.of(CalendarDurationUnit.MINUTE, "s", new BigDecimal("2"), new BigDecimal("120")),
        Arguments.of(CalendarDurationUnit.HOUR, "s", new BigDecimal("1"), new BigDecimal("3600")),
        Arguments.of(CalendarDurationUnit.DAY, "s", new BigDecimal("1"), new BigDecimal("86400")),
        Arguments.of(CalendarDurationUnit.WEEK, "s", new BigDecimal("1"), new BigDecimal("604800")),

        // Conversions to UCUM 'ms' - all calendar durations should convert
        Arguments.of(CalendarDurationUnit.MILLISECOND, "ms", new BigDecimal("1500"), new BigDecimal("1500")),
        Arguments.of(CalendarDurationUnit.SECOND, "ms", new BigDecimal("5"), new BigDecimal("5000")),
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
            sourceValue, sourceUnit.getUnit(), targetUnit, expectedValue, result.getValue()));

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
        // Calendar duration to calendar duration (except seconds/milliseconds)
        Arguments.of(CalendarDurationUnit.DAY, "weeks"),
        Arguments.of(CalendarDurationUnit.DAY, "hours"),
        Arguments.of(CalendarDurationUnit.HOUR, "minutes"),
        Arguments.of(CalendarDurationUnit.YEAR, "months"),
        Arguments.of(CalendarDurationUnit.YEAR, "days"),
        Arguments.of(CalendarDurationUnit.MONTH, "days"),

        // Calendar duration to UCUM (except 's' and 'ms')
        Arguments.of(CalendarDurationUnit.DAY, "h"),
        Arguments.of(CalendarDurationUnit.HOUR, "min"),
        Arguments.of(CalendarDurationUnit.WEEK, "d"),
        Arguments.of(CalendarDurationUnit.YEAR, "a"),
        Arguments.of(CalendarDurationUnit.MONTH, "mo"),

        // Invalid targets
        Arguments.of(CalendarDurationUnit.DAY, "kg"),
        Arguments.of(CalendarDurationUnit.HOUR, "invalid_unit")
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
            sourceUnit.getUnit(), targetUnit));
  }
}
