package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.fhirpath.FhirpathDateTime.TemporalPrecision;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class FhirpathDateTimeTest {

  FhirpathDateTime fromDateTimeString(String dateTimeString, TemporalPrecision precision) {
    return FhirpathDateTime.fromDateTime(OffsetDateTime.parse(dateTimeString), precision);
  }

  static Stream<Arguments> parseTestProvider() {
    return Stream.of(
        Arguments.of("2023", "2023-01-01T00:00:00Z", TemporalPrecision.YEAR, "Year only"),
        Arguments.of("2023-10", "2023-10-01T00:00:00Z", TemporalPrecision.MONTH, "Year and month"),
        Arguments.of("2023-10-05T13", "2023-10-05T13:00:00Z", TemporalPrecision.HOUR, "Up to hour"),
        Arguments.of("2023-10-05T13:30", "2023-10-05T13:30:00Z", TemporalPrecision.MINUTE,
            "Up to minute"),
        Arguments.of("2023-10-05T13:30:01", "2023-10-05T13:30:01Z", TemporalPrecision.SECOND,
            "Up to second"),
        Arguments.of("2023-10-05T13:30:01.245", "2023-10-05T13:30:01.245Z", TemporalPrecision.FRACS,
            "With milliseconds"),
        Arguments.of("2023-10-05T13:30:01.123456789Z", "2023-10-05T13:30:01.123456789Z",
            TemporalPrecision.FRACS, "With nanoseconds and Z timezone"),
        Arguments.of("2023-10-05T13:30:01.245+00:00", "2023-10-05T13:30:01.245Z",
            TemporalPrecision.FRACS, "With explicit UTC timezone")
    );
  }

  @ParameterizedTest(name = "{0} -> {1} ({3})")
  @MethodSource("parseTestProvider")
  void testParse(String input, String expectedInstant, TemporalPrecision expectedPrecision,
      String ignoredDescription) {
    assertEquals(
        fromDateTimeString(expectedInstant, expectedPrecision),
        FhirpathDateTime.parse(input)
    );
  }

  static Stream<Arguments> lowerBoundaryProvider() {
    return Stream.of(
        // Format: input string, expected lower boundary, description
        Arguments.of("2023", "2023-01-01T00:00:00Z", "YEAR precision"),
        Arguments.of("2023-06", "2023-06-01T00:00:00Z", "MONTH precision"),
        Arguments.of("2023-06-15", "2023-06-15T00:00:00Z", "DAY precision"),
        Arguments.of("2023-06-15T14", "2023-06-15T14:00:00Z", "HOUR precision"),
        Arguments.of("2023-06-15T14:30", "2023-06-15T14:30:00Z", "MINUTE precision"),
        Arguments.of("2023-06-15T14:30:45", "2023-06-15T14:30:45Z", "SECOND precision"),
        Arguments.of("2023-06-15T14:30:45.123456", "2023-06-15T14:30:45.123456Z", "FRACS precision"),
        Arguments.of("2023-06-15T14:30:45+02:00", "2023-06-15T14:30:45+02:00",
            "With explicit positive timezone"),
        Arguments.of("2023-06-15T14:30:45-05:00", "2023-06-15T14:30:45-05:00",
            "With explicit negative timezone"),
        Arguments.of("2024-02-29", "2024-02-29T00:00:00Z", "Leap year date")
    );
  }

  @ParameterizedTest(name = "{0} -> {1} ({2})")
  @MethodSource("lowerBoundaryProvider")
  void testGetLowerBoundary(String input, String expectedLowerBound, String ignoredDescription) {
    final FhirpathDateTime dateTime = FhirpathDateTime.parse(input);
    assertEquals(Instant.parse(expectedLowerBound), dateTime.getLowerBoundary());
  }

  static Stream<Arguments> upperBoundaryProvider() {
    return Stream.of(
        // Format: input string, expected upper boundary, description
        Arguments.of("2023", "2023-12-31T23:59:59.999999999Z", "YEAR precision"),
        Arguments.of("2023-06", "2023-06-30T23:59:59.999999999Z", "MONTH precision - 30 days"),
        Arguments.of("2023-02", "2023-02-28T23:59:59.999999999Z",
            "MONTH precision - February non-leap year"),
        Arguments.of("2024-02", "2024-02-29T23:59:59.999999999Z", "MONTH precision - February leap year"),
        Arguments.of("2023-06-15", "2023-06-15T23:59:59.999999999Z", "DAY precision"),
        Arguments.of("2023-06-15T14", "2023-06-15T14:59:59.999999999Z", "HOUR precision"),
        Arguments.of("2023-06-15T14:30", "2023-06-15T14:30:59.999999999Z", "MINUTE precision"),
        Arguments.of("2023-06-15T14:30:45", "2023-06-15T14:30:45.999999999Z", "SECOND precision"),
        Arguments.of("2023-06-15T14:30:45.123", "2023-06-15T14:30:45.123Z",
            "FRACS precision (unchanged)"),
        Arguments.of("2023-06-15T14:30:45+02:00", "2023-06-15T12:30:45.999999999Z",
            "SECONDS With explicit timezone (adjusted to UTC)"),
        Arguments.of("2023-06-15T14:30:45.123-05:00", "2023-06-15T19:30:45.123Z",
            "FRACS With negative timezone (unchanged)")
    );
  }

  @ParameterizedTest(name = "{0} -> {1} ({2})")
  @MethodSource("upperBoundaryProvider")
  void testGetUpperBoundary(String input, String expectedUpperBound, String ignoredDescription) {
    final FhirpathDateTime dateTime = FhirpathDateTime.parse(input);
    assertEquals(Instant.parse(expectedUpperBound), dateTime.getUpperBoundary());
  }

  @ParameterizedTest(name = "Parse error with null or empty value: \"{0}\"")
  @NullAndEmptySource
  @ValueSource(strings = {" "})
  void testParseErrorsNullOrEmpty(String input) {
    assertThrows(Exception.class, () -> FhirpathDateTime.parse(input));
  }

  @ParameterizedTest(name = "Parse error with invalid format: {0}")
  @ValueSource(strings = {
      "not-a-date",
      "2023/05/20",       // Wrong date separators
      "05-20-2023",       // Wrong date order
      "2023-5-20",        // Missing leading zero in month
      "2023-05-2",        // Missing leading zero in day
      "202",              // Incomplete year
      "2023-05-15T12-30-45", // Wrong time separators
      "2023-05-15T12:30:45." // Dot with no fraction
  })
  void testParseErrorsInvalidFormat(String input) {
    assertThrows(DateTimeParseException.class, () -> FhirpathDateTime.parse(input));
  }

  static Stream<Arguments> invalidValueProvider() {
    return Stream.of(
        Arguments.of("2023-00-15", "Month 0 (invalid)"),
        Arguments.of("2023-13-15", "Month 13 (invalid)"),
        Arguments.of("2023-05-00", "Day 0 (invalid)"),
        Arguments.of("2023-05-32", "Day 32 (invalid)"),
        Arguments.of("2023-04-31", "April 31 (invalid)"),
        Arguments.of("2023-02-30", "February 30 (invalid)"),
        Arguments.of("2023-05-15T24:00:00", "Hour 24 (invalid)"),
        Arguments.of("2023-05-15T12:60:00", "Minute 60 (invalid)"),
        Arguments.of("2023-05-15T12:30:60", "Second 60 (invalid)"),
        Arguments.of("2023-05-15T12:30:45+24:00", "Invalid hours offset"),
        Arguments.of("2023-05-15T12:30:45+12:60", "Invalid minutes offset"),
        Arguments.of("2023-05-15T12:30:45+1:30", "Missing leading zero in offset")
    );
  }

  @ParameterizedTest(name = "{0} - {1}")
  @MethodSource("invalidValueProvider")
  void testParseErrorsInvalidValues(String input, String ignoredDescription) {
    assertThrows(DateTimeParseException.class,
        () -> FhirpathDateTime.parse(input));
  }
}
