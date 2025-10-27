package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.stream.Stream;
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
}
