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

package au.csiro.pathling.views;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Iso8601StringAdapter}, exercised through the production Gson factory {@link
 * ViewDefinitionGson#create()} so that the adapter registration wiring is verified along with the
 * adapter behaviour itself.
 *
 * <p>The canonical strings asserted here are the JDK {@code toString()} forms, which are the
 * strings that the tabular {@code json}, {@code ndjson} and {@code csv} outputs of {@code $sql-run}
 * and {@code $viewdefinition-run} are contracted to emit.
 *
 * @author John Grimes
 */
class Iso8601StringAdapterTest {

  Gson gson;

  @BeforeEach
  void setUp() {
    gson = ViewDefinitionGson.create();
  }

  /**
   * Verifies that a whole-minute {@link LocalDateTime} serialises as a quoted JSON string in the
   * canonical form, with the zero seconds elided as per ISO-8601.
   */
  @Test
  void serialisesLocalDateTimeAsIso8601String() {
    // Arrange.
    final LocalDateTime value = LocalDateTime.parse("2020-01-01T12:00:00");

    // Act.
    final String json = gson.toJson(value);

    // Assert.
    assertThat(json).isEqualTo("\"2020-01-01T12:00\"");
  }

  /**
   * Verifies that fractional seconds present in a {@link LocalDateTime} are preserved, in contrast
   * to the whole-minute case where the seconds are elided.
   */
  @Test
  void preservesFractionalSecondsInLocalDateTime() {
    // Arrange.
    final LocalDateTime wholeMinute = LocalDateTime.parse("2020-01-01T12:00:00");
    final LocalDateTime fractional = LocalDateTime.parse("2020-01-01T12:00:00.123");

    // Act.
    final String wholeMinuteJson = gson.toJson(wholeMinute);
    final String fractionalJson = gson.toJson(fractional);

    // Assert.
    assertThat(wholeMinuteJson).isEqualTo("\"2020-01-01T12:00\"");
    assertThat(fractionalJson).isEqualTo("\"2020-01-01T12:00:00.123\"");
  }

  /**
   * Verifies that a day-time interval value serialises using the JDK {@link Duration} form,
   * including the sign form for negative values and the normalisation of days into hours.
   */
  @Test
  void serialisesDurationAsIso8601String() {
    // Arrange, act and assert for a positive value.
    assertThat(gson.toJson(Duration.ofHours(1))).isEqualTo("\"PT1H\"");

    // A negative duration carries the sign on the field, not in front of the P.
    assertThat(gson.toJson(Duration.ofHours(-1))).isEqualTo("\"PT-1H\"");

    // The JDK normalises whole days into hours, so one day is rendered as 24 hours.
    assertThat(gson.toJson(Duration.ofDays(1))).isEqualTo("\"PT24H\"");
  }

  /**
   * Verifies that a year-month interval value serialises using the JDK {@link Period} form,
   * including the sign form for negative values.
   */
  @Test
  void serialisesPeriodAsIso8601String() {
    // Arrange, act and assert for a positive value.
    assertThat(gson.toJson(Period.ofYears(1))).isEqualTo("\"P1Y\"");

    // A negative period carries the sign on the field, not in front of the P.
    assertThat(gson.toJson(Period.ofYears(-1))).isEqualTo("\"P-1Y\"");
  }

  /** Verifies that a {@link LocalDate} serialises as a plain ISO-8601 date string. */
  @Test
  void serialisesLocalDateAsIso8601String() {
    // Arrange.
    final LocalDate value = LocalDate.parse("2020-01-01");

    // Act.
    final String json = gson.toJson(value);

    // Assert.
    assertThat(json).isEqualTo("\"2020-01-01\"");
  }

  /**
   * Verifies that an {@link Instant} serialises as an ISO-8601 instant string, retaining the UTC
   * designator.
   */
  @Test
  void serialisesInstantAsIso8601String() {
    // Arrange.
    final Instant value = Instant.parse("2020-01-01T00:00:00Z");

    // Act.
    final String json = gson.toJson(value);

    // Assert.
    assertThat(json).isEqualTo("\"2020-01-01T00:00:00Z\"");
  }

  /**
   * Verifies that a null value of an affected type serialises to JSON null, so that the adapter is
   * null-safe and the null-omission convention used by the result streaming helper is unaffected.
   */
  @Test
  void serialisesNullValueAsJsonNull() {
    // Arrange, act and assert for each affected type.
    assertThat(gson.toJson(null, LocalDateTime.class)).isEqualTo("null");
    assertThat(gson.toJson(null, Duration.class)).isEqualTo("null");
    assertThat(gson.toJson(null, Period.class)).isEqualTo("null");
    assertThat(gson.toJson(null, LocalDate.class)).isEqualTo("null");
    assertThat(gson.toJson(null, Instant.class)).isEqualTo("null");
  }

  /**
   * Verifies that values nested within a container serialise with the same canonical strings. This
   * mirrors the way that {@code ResultStreamingHelper} serialises a result row, which is a map of
   * column name to value.
   */
  @Test
  void serialisesNestedValuesWithinMapAsIso8601Strings() {
    // Arrange.
    final Map<String, Object> row = new LinkedHashMap<>();
    row.put("ts_ntz", LocalDateTime.parse("2020-01-01T12:00:00"));
    row.put("day_time_interval", Duration.ofHours(1));
    row.put("year_month_interval", Period.ofYears(1));

    // Act.
    final String json = gson.toJson(row);

    // Assert.
    assertThat(json)
        .isEqualTo(
            "{\"ts_ntz\":\"2020-01-01T12:00\",\"day_time_interval\":\"PT1H\","
                + "\"year_month_interval\":\"P1Y\"}");
  }

  /**
   * Verifies that deserialisation is not supported: the adapter is write-only, because these types
   * only ever appear on the result-streaming path.
   */
  @Test
  void deserialisationThrowsUnsupportedOperationException() {
    // Arrange.
    final TypeAdapter<LocalDateTime> localDateTimeAdapter = gson.getAdapter(LocalDateTime.class);
    final TypeAdapter<Duration> durationAdapter = gson.getAdapter(Duration.class);

    // Act and assert.
    assertThatThrownBy(() -> localDateTimeAdapter.fromJson("\"2020-01-01T12:00\""))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> durationAdapter.fromJson("\"PT1H\""))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  /**
   * Verifies that types unaffected by this feature continue to serialise with Gson's built-in
   * behaviour.
   */
  @Test
  void leavesUnaffectedTypesUnchanged() {
    // Arrange, act and assert.
    assertThat(gson.toJson("some value")).isEqualTo("\"some value\"");
    assertThat(gson.toJson(42)).isEqualTo("42");
  }
}
