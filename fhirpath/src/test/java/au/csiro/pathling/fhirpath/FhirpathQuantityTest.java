package au.csiro.pathling.fhirpath;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FhirpathQuantityTest {

  static Stream<Arguments> quantityLiterals() {
    return Stream.of(
      // UCUM cases
      Arguments.of("5.4 'mg'", FhirpathQuantity.ofUCUM(new BigDecimal("5.4"), "mg")),
      Arguments.of("-2 'kg'", FhirpathQuantity.ofUCUM(new BigDecimal("-2"), "kg")),
      Arguments.of("1.0 'mL'", FhirpathQuantity.ofUCUM(new BigDecimal("1.0"), "mL")),
      // Calendar duration cases (singular and plural)
      Arguments.of("1 year", FhirpathQuantity.ofCalendar(new BigDecimal("1"), CalendarDurationUnit.YEAR)),
      Arguments.of("2 years", FhirpathQuantity.ofCalendar(new BigDecimal("2"), CalendarDurationUnit.YEAR, "years")),
      Arguments.of("3 month", FhirpathQuantity.ofCalendar(new BigDecimal("3"), CalendarDurationUnit.MONTH)),
      Arguments.of("4 months", FhirpathQuantity.ofCalendar(new BigDecimal("4"), CalendarDurationUnit.MONTH, "months")),
      Arguments.of("5 day", FhirpathQuantity.ofCalendar(new BigDecimal("5"), CalendarDurationUnit.DAY)),
      Arguments.of("6 days", FhirpathQuantity.ofCalendar(new BigDecimal("6"), CalendarDurationUnit.DAY, "days")),
      Arguments.of("7 hour", FhirpathQuantity.ofCalendar(new BigDecimal("7"), CalendarDurationUnit.HOUR)),
      Arguments.of("8 hours", FhirpathQuantity.ofCalendar(new BigDecimal("8"), CalendarDurationUnit.HOUR, "hours")),
      Arguments.of("9 minute", FhirpathQuantity.ofCalendar(new BigDecimal("9"), CalendarDurationUnit.MINUTE)),
      Arguments.of("10 minutes", FhirpathQuantity.ofCalendar(new BigDecimal("10"), CalendarDurationUnit.MINUTE, "minutes")),
      Arguments.of("11 second", FhirpathQuantity.ofCalendar(new BigDecimal("11"), CalendarDurationUnit.SECOND)),
      Arguments.of("12 seconds", FhirpathQuantity.ofCalendar(new BigDecimal("12"), CalendarDurationUnit.SECOND, "seconds")),
      Arguments.of("13 millisecond", FhirpathQuantity.ofCalendar(new BigDecimal("13"), CalendarDurationUnit.MILLISECOND)),
      Arguments.of("14 milliseconds", FhirpathQuantity.ofCalendar(new BigDecimal("14"), CalendarDurationUnit.MILLISECOND, "milliseconds"))
    );
  }

  @ParameterizedTest
  @MethodSource("quantityLiterals")
  void testParseQuantity(String literal, FhirpathQuantity expected) {
    FhirpathQuantity actual = FhirpathQuantity.parse(literal);
    assertEquals(expected, actual, "Parsed quantity should match expected");
  }
}
