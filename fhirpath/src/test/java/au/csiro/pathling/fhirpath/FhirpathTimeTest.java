package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class FhirpathTimeTest {

  FhirPathTime fromTimeString(String timeString, TemporalPrecision precision) {
    return FhirPathTime.fromLocalTime(LocalTime.parse(timeString), precision);
  }

  static Stream<Arguments> parseTestProvider() {
    return Stream.of(
        Arguments.of("12", "12:00:00", TemporalPrecision.HOUR, "Hour only"),
        Arguments.of("14:30", "14:30:00", TemporalPrecision.MINUTE, "Hour and minute"),
        Arguments.of("14:30:14", "14:30:14", TemporalPrecision.SECOND, "Up to second"),
        Arguments.of("14:30:14.559", "14:30:14.559", TemporalPrecision.FRACS, "With milliseconds"),
        Arguments.of("00:00:00.000000001", "00:00:00.000000001", TemporalPrecision.FRACS, "With nanoseconds")
    );
  }

  @ParameterizedTest(name = "{0} -> {1} ({3})")
  @MethodSource("parseTestProvider")
  void testParse(String input, String expectedTime, TemporalPrecision expectedPrecision,
      String ignoredDescription) {
    assertEquals(
        fromTimeString(expectedTime, expectedPrecision),
        FhirPathTime.parse(input)
    );
  }

  static Stream<Arguments> lowerBoundaryProvider() {
    return Stream.of(
        Arguments.of("12", "1970-01-01T12:00:00Z", "HOUR precision"),
        Arguments.of("14:30", "1970-01-01T14:30:00Z", "MINUTE precision"),
        Arguments.of("14:30:45", "1970-01-01T14:30:45Z", "SECOND precision"),
        Arguments.of("14:30:45.123456", "1970-01-01T14:30:45.123456Z", "FRACS precision"),
        Arguments.of("00:00:00.000000001", "1970-01-01T00:00:00.000000001Z", "FRACS precision")
    );
  }

  @ParameterizedTest(name = "{0} -> {1} ({2})")
  @MethodSource("lowerBoundaryProvider")
  void testGetLowerBoundary(String input, String expectedLowerBound, String ignoredDescription) {
    final FhirPathTime time = FhirPathTime.parse(input);
    assertEquals(Instant.parse(expectedLowerBound), time.getLowerBoundary());
  }

  static Stream<Arguments> upperBoundaryProvider() {
    return Stream.of(
        Arguments.of("12", "1970-01-01T12:59:59.999999999Z", "HOUR precision"),
        Arguments.of("14:30", "1970-01-01T14:30:59.999999999Z", "MINUTE precision"),
        Arguments.of("14:30:45", "1970-01-01T14:30:45Z", "SECOND precision"),
        Arguments.of("14:30:45.123", "1970-01-01T14:30:45.123Z", "FRACS precision (unchanged)")
    );
  }

  @ParameterizedTest(name = "{0} -> {1} ({2})")
  @MethodSource("upperBoundaryProvider")
  void testGetUpperBoundary(String input, String expectedUpperBound, String ignoredDescription) {
    final FhirPathTime time = FhirPathTime.parse(input);
    assertEquals(Instant.parse(expectedUpperBound), time.getUpperBoundary());
  }

  @ParameterizedTest(name = "Parse error with null or empty value: \"{0}\"")
  @NullAndEmptySource
  @ValueSource(strings = {" "})
  void testParseErrorsNullOrEmpty(String input) {
    assertThrows(Exception.class, () -> FhirPathTime.parse(input));
  }

  @ParameterizedTest(name = "Parse error with invalid format: {0}")
  @ValueSource(strings = {
      "not-a-time",
      "12:60",      // Invalid minute
      "24:00",      // Invalid hour
      "12:30:60",   // Invalid second
      "12:30:45.",  // Dot with no fraction
      "12:30:45.abc" // Non-numeric fraction
  })
  void testParseErrorsInvalidFormat(String input) {
    assertThrows(DateTimeParseException.class, () -> FhirPathTime.parse(input));
  }
}

