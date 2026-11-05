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

package au.csiro.pathling.io;

import static au.csiro.pathling.io.TransformFixtures.DEFINITIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.schema.date.FhirDateTime;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests the range annotations the annotation step stores beside a date (T053, FR-004, decision
 * 135): {@code __start} and {@code __end} are the last fields of the date's metadata group, and
 * hold the earliest and the latest instant the precision the value states allows.
 */
class DateRangeAnnotationTest {

  private static final String START = LayoutFields.START_ANNOTATION;

  private static final String END = LayoutFields.END_ANNOTATION;

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull
  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"birthDate\":\"1980-06\","
          + "\"deceasedDateTime\":\"2020-01-02T03:04:05+10:00\","
          + "\"meta\":{\"lastUpdated\":\"2024-05-06T07:08:09.123Z\"},"
          + "\"name\":[{\"family\":\"A\",\"period\":{\"start\":\"2001\"}},"
          + "{\"family\":\"B\"},"
          + "{\"family\":\"C\",\"period\":{\"start\":\"2002-03-04\"}}]}";

  @Nonnull
  private static final String TIMING =
      "{\"resourceType\":\"MedicationRequest\",\"id\":\"m1\",\"status\":\"active\","
          + "\"intent\":\"order\",\"subject\":{\"reference\":\"Patient/p\"},"
          + "\"medicationCodeableConcept\":{\"text\":\"x\"},"
          + "\"dosageInstruction\":[{\"timing\":{\"event\":[\"2020\",\"2021-02\"]}}]}";

  @Nonnull
  private static Dataset<Row> read(@Nonnull final String resourceType, @Nonnull final String json) {
    return TransformFixtures.reader()
        .read(
            resourceType,
            TransformFixtures.spark().createDataset(List.of(json), Encoders.STRING()));
  }

  @Nonnull
  private static Dataset<Row> annotate(
      @Nonnull final SchemaConfiguration configuration,
      @Nonnull final String resourceType,
      @Nonnull final String json) {
    return Annotator.of(DEFINITIONS, configuration)
        .annotate(resourceType, read(resourceType, json));
  }

  @Nonnull
  private static Dataset<Row> annotate(
      @Nonnull final String resourceType, @Nonnull final String json) {
    return annotate(SchemaConfiguration.builder().build(), resourceType, json);
  }

  @Nonnull
  private static Dataset<Row> patientWith(
      @Nonnull final String element, @Nonnull final String value) {
    return annotate(
        "Patient",
        "{\"resourceType\":\"Patient\",\"id\":\"p\",\"" + element + "\":\"" + value + "\"}");
  }

  @Nonnull
  private static StructType structure(@Nonnull final DataType type) {
    return (StructType) (type instanceof final ArrayType array ? array.elementType() : type);
  }

  /** Asserts that a metadata group carries the two bounds last, in order, as timestamps. */
  private static void assertCarriesTheRangeLast(@Nonnull final DataType group) {
    final StructField[] fields = structure(group).fields();
    assertEquals(START, fields[fields.length - 2].name());
    assertEquals(END, fields[fields.length - 1].name());
    assertEquals(DataTypes.TimestampType, fields[fields.length - 2].dataType());
    assertEquals(DataTypes.TimestampType, fields[fields.length - 1].dataType());
  }

  /**
   * Asserts the bounds of a row's two columns, given as microseconds since the epoch, as instants.
   * The bounds are read as Spark holds them, rather than as {@link java.sql.Timestamp}, which
   * shifts a date before the Gregorian cutover to the Julian calendar in the JVM's time zone.
   */
  private static void assertRange(
      @Nonnull final String start, @Nonnull final String end, @Nonnull final Row row) {
    assertEquals(micros(Instant.parse(start)), row.getLong(0));
    assertEquals(micros(Instant.parse(end)), row.getLong(1));
  }

  private static long micros(@Nonnull final Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  @ParameterizedTest
  @CsvSource({
    "1980, 1980-01-01T00:00:00Z, 1980-12-31T23:59:59.999999Z",
    "1980-02, 1980-02-01T00:00:00Z, 1980-02-29T23:59:59.999999Z",
    "1980-02-03, 1980-02-03T00:00:00Z, 1980-02-03T23:59:59.999999Z",
    "1980-02-03T04:05:06Z, 1980-02-03T04:05:06Z, 1980-02-03T04:05:06Z",
    "1980-02-03T04:05:06+10:00, 1980-02-02T18:05:06Z, 1980-02-02T18:05:06Z",
    "1980-02-03T04:05:06.789-03:30, 1980-02-03T07:35:06.789Z, 1980-02-03T07:35:06.789Z",
    "1500-02-03, 1500-02-03T00:00:00Z, 1500-02-03T23:59:59.999999Z"
  })
  void eachPrecisionCarriesTheRangeItImplies(
      @Nonnull final String date, @Nonnull final String start, @Nonnull final String end) {
    final Dataset<Row> annotated = patientWith("deceasedDateTime", date);

    assertCarriesTheRangeLast(annotated.schema().apply("_deceasedDateTime").dataType());
    assertRange(
        start,
        end,
        annotated
            .selectExpr(
                "unix_micros(_deceasedDateTime.__start)", "unix_micros(_deceasedDateTime.__end)")
            .first());
  }

  @ParameterizedTest
  @CsvSource({"1980-02-03T04:05:06", "1980-02-03T04:05:06.5", "1980-02-03T04:05"})
  void aTimeWithoutATimezoneCarriesTheRangeTheEngineComputes(@Nonnull final String dateTime) {
    final Dataset<Row> annotated = patientWith("deceasedDateTime", dateTime);

    // The engine's own boundaries, which take a time without a timezone as UTC.
    final FhirDateTime parsed = FhirDateTime.parse(dateTime);
    assertRange(
        parsed.getLowerBoundary().toString(),
        parsed.getUpperBoundary().truncatedTo(ChronoUnit.MICROS).toString(),
        annotated
            .selectExpr(
                "unix_micros(_deceasedDateTime.__start)", "unix_micros(_deceasedDateTime.__end)")
            .first());
  }

  @Test
  void aDateAndAnInstantCarryTheRange() {
    final Dataset<Row> annotated = annotate("Patient", PATIENT);

    assertCarriesTheRangeLast(annotated.schema().apply("_birthDate").dataType());
    final StructType meta = structure(annotated.schema().apply("meta").dataType());
    assertCarriesTheRangeLast(meta.apply("_lastUpdated").dataType());
    assertRange(
        "1980-06-01T00:00:00Z",
        "1980-06-30T23:59:59.999999Z",
        annotated
            .selectExpr("unix_micros(_birthDate.__start)", "unix_micros(_birthDate.__end)")
            .first());
    assertRange(
        "2024-05-06T07:08:09.123Z",
        "2024-05-06T07:08:09.123Z",
        annotated
            .selectExpr(
                "unix_micros(meta._lastUpdated.__start)", "unix_micros(meta._lastUpdated.__end)")
            .first());
  }

  @Test
  void aDateUnderARepeatingParentCarriesTheRangeAtEachPosition() {
    final Dataset<Row> annotated = annotate("Patient", PATIENT);

    final StructType period =
        structure(
            structure(annotated.schema().apply("name").dataType()).apply("period").dataType());
    assertCarriesTheRangeLast(period.apply("_start").dataType());
    final Row row =
        annotated
            .selectExpr(
                "name[0].period._start.__start",
                "name[0].period._start.__end",
                "name[1].period._start",
                "name[2].period._start.__start",
                "name[2].period._start.__end")
            .first();
    assertEquals(Instant.parse("2001-01-01T00:00:00Z"), row.getTimestamp(0).toInstant());
    assertEquals(Instant.parse("2001-12-31T23:59:59.999999Z"), row.getTimestamp(1).toInstant());
    assertNull(row.get(2));
    assertEquals(Instant.parse("2002-03-04T00:00:00Z"), row.getTimestamp(3).toInstant());
    assertEquals(Instant.parse("2002-03-04T23:59:59.999999Z"), row.getTimestamp(4).toInstant());
  }

  @Test
  void aRepeatingDateCarriesTheRangeAtEachPosition() {
    final Dataset<Row> annotated = annotate("MedicationRequest", TIMING);

    final Row row =
        annotated
            .selectExpr(
                "dosageInstruction[0].timing._event[0].__start",
                "dosageInstruction[0].timing._event[0].__end",
                "dosageInstruction[0].timing._event[1].__start",
                "dosageInstruction[0].timing._event[1].__end")
            .first();
    assertEquals(Instant.parse("2020-01-01T00:00:00Z"), row.getTimestamp(0).toInstant());
    assertEquals(Instant.parse("2020-12-31T23:59:59.999999Z"), row.getTimestamp(1).toInstant());
    assertEquals(Instant.parse("2021-02-01T00:00:00Z"), row.getTimestamp(2).toInstant());
    assertEquals(Instant.parse("2021-02-28T23:59:59.999999Z"), row.getTimestamp(3).toInstant());
  }

  @Test
  void theSwitchLeavesOutTheRange() {
    final Dataset<Row> annotated =
        annotate(
            SchemaConfiguration.builder().enableRangeAnnotation(false).build(), "Patient", PATIENT);

    final List<String> columns = Arrays.asList(annotated.columns());
    assertFalse(columns.contains("_birthDate"));
    assertFalse(columns.contains("_deceasedDateTime"));
  }

  @Test
  void bothBoundsComeFromOneParse() {
    // A singular date, a date under a repeating parent, and a repeating date.
    assertParsedOnce(annotate("Patient", PATIENT).select("_birthDate"));
    assertParsedOnce(annotate("Patient", PATIENT).select("name"));
    assertParsedOnce(annotate("MedicationRequest", TIMING).select("dosageInstruction"));
  }

  /** Asserts that the optimised plan computes the range of each date once, for both its bounds. */
  private static void assertParsedOnce(@Nonnull final Dataset<Row> annotated) {
    final String plan = annotated.queryExecution().optimizedPlan().toString();
    final Matcher matcher = Pattern.compile("date_range\\(").matcher(plan);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    assertEquals(1, count, plan);
  }

  @Test
  void egressWritesTheDatesWithoutTheRange() throws IOException {
    final String written =
        TransformFixtures.fhirWriter()
            .json()
            .write("Patient", annotate("Patient", PATIENT))
            .first();

    assertEquals(MAPPER.readTree(PATIENT), MAPPER.readTree(written));
  }
}
