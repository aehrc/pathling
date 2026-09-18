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

package au.csiro.pathling.io.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that a primitive value contradicting its declared type fails the transform, rather than
 * being stored as a silently different value (decision 57).
 *
 * <p>Every case runs under both settings of {@code spark.sql.ansi.enabled}, because that setting
 * belongs to the caller's session rather than to Pathling. A plain cast raises only when it is on,
 * and becomes a silent null — or, for an integer written with a fraction, a silently truncated
 * number — when it is off. Every case also runs under both settings of the strictness switch,
 * because a value that contradicts its own declared type is not content the definitions fail to
 * describe, and ignoring it is not a choice the switch offers.
 */
class PrimitiveValueTest {

  @Nonnull private static final String ANSI = "spark.sql.ansi.enabled";

  /**
   * Values that do not conform, with the path the failure must name and the type it was declared
   * as. Two are accepted by Spark's own cast even with ANSI on, which is why a stricter check than
   * the cast is needed at all.
   */
  @Nonnull
  static Stream<Arguments> malformed() {
    final List<Arguments> cases =
        List.of(
            Arguments.of(
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\",\"active\":\"not-a-boolean\"}",
                "Patient.active",
                "boolean"),
            // Spark casts yes, y, t and 1 to true; FHIR admits only true and false.
            Arguments.of(
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\",\"active\":\"yes\"}",
                "Patient.active",
                "boolean"),
            Arguments.of(
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\",\"multipleBirthInteger\":\"two\"}",
                "Patient.multipleBirthInteger",
                "integer"),
            // With ANSI off, a plain cast truncates this to 1.
            Arguments.of(
                "Observation",
                "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                    + "\"valueInteger\":1.5}",
                "Observation.valueInteger",
                "integer"),
            // Lexically an integer, but outside the range the type is stored in.
            Arguments.of(
                "Observation",
                "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                    + "\"valueInteger\":99999999999}",
                "Observation.valueInteger",
                "integer"),
            Arguments.of(
                "ImagingStudy",
                "{\"resourceType\":\"ImagingStudy\",\"id\":\"1\",\"status\":\"available\","
                    + "\"subject\":{\"reference\":\"Patient/1\"},\"numberOfSeries\":-1}",
                "ImagingStudy.numberOfSeries",
                "unsignedInt"),
            // A repeating primitive, nested in a repeating structure.
            Arguments.of(
                "ClaimResponse",
                "{\"resourceType\":\"ClaimResponse\",\"id\":\"1\",\"status\":\"active\","
                    + "\"item\":[{\"itemSequence\":1,\"noteNumber\":[1,0]}]}",
                "ClaimResponse.item.noteNumber",
                "positiveInt"),
            // A decimal is stored as text, so no cast notices; egress would leak its marks.
            Arguments.of(
                "Observation",
                "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                    + "\"valueQuantity\":{\"value\":\"abc\"}}",
                "Observation.valueQuantity.value",
                "decimal"));
    return cases.stream()
        .flatMap(
            c ->
                Stream.of(true, false)
                    .flatMap(
                        ansi ->
                            Stream.of(true, false)
                                .map(
                                    strict -> {
                                      final Object[] a = c.get();
                                      return Arguments.of(a[0], a[1], a[2], a[3], ansi, strict);
                                    })));
  }

  @ParameterizedTest(name = "{2} as {3}, ansi={4}, strict={5}")
  @MethodSource("malformed")
  void failsOnAValueThatContradictsItsDeclaredType(
      @Nonnull final String resourceType,
      @Nonnull final String document,
      @Nonnull final String path,
      @Nonnull final String type,
      final boolean ansi,
      final boolean strict,
      @TempDir @Nonnull final Path directory) {
    final Exception error =
        assertThrows(
            Exception.class,
            () -> withAnsi(ansi, () -> transform(directory, resourceType, strict, document)));

    final String message = messages(error);
    assertTrue(message.contains(path), "the failure must name the element: " + message);
    assertTrue(message.contains(type), "the failure must name the declared type: " + message);
  }

  /**
   * The positive control: values at the edges of each type's lexical space are stored, under both
   * settings, so the check is not simply rejecting everything it sees.
   */
  @ParameterizedTest(name = "ansi={0}")
  @MethodSource("ansiSettings")
  void storesValuesThatConform(final boolean ansi, @TempDir @Nonnull final Path directory) {
    final Row row =
        withAnsi(
            ansi,
            () ->
                transform(
                    directory,
                    "ClaimResponse",
                    true,
                    "{\"resourceType\":\"ClaimResponse\",\"id\":\"1\",\"status\":\"active\","
                        + "\"item\":[{\"itemSequence\":2147483647,\"noteNumber\":[1,2]}],"
                        + "\"total\":[{\"category\":{\"text\":\"t\"},"
                        + "\"amount\":{\"value\":-1.50E-22}}]}"));
    final Row item = row.<Row>getList(row.fieldIndex("item")).get(0);
    assertEquals(2147483647, item.<Integer>getAs("itemSequence"));
    assertEquals(List.of(1, 2), item.getList(item.fieldIndex("noteNumber")));
    final Row total = row.<Row>getList(row.fieldIndex("total")).get(0);
    assertEquals("-1.50E-22", total.<Row>getAs("amount").<String>getAs("value"));

    final Row patient =
        withAnsi(
            ansi,
            () ->
                transform(
                    directory.resolve("patient"),
                    "Patient",
                    true,
                    "{\"resourceType\":\"Patient\",\"id\":\"1\",\"active\":false,"
                        + "\"multipleBirthInteger\":-2147483648}"));
    assertEquals(false, patient.<Boolean>getAs("active"));
    assertEquals(-2147483648, patient.<Integer>getAs("multipleBirthInteger"));
  }

  @Nonnull
  static Stream<Boolean> ansiSettings() {
    return Stream.of(true, false);
  }

  @Nonnull
  private static Row transform(
      @Nonnull final Path directory,
      @Nonnull final String resourceType,
      final boolean strict,
      @Nonnull final String document) {
    directory.toFile().mkdirs();
    final ResourceTransformer transformer =
        TransformFixtures.transformer(
            SchemaConfiguration.builder().failOnNonConformantContent(strict).build());
    return transformer
        .read(
            TransformFixtures.spark(), resourceType, TransformFixtures.corpus(directory, document))
        .collectAsList()
        .get(0);
  }

  /** Runs an action with ANSI mode set as asked, restoring the session's setting afterwards. */
  @Nonnull
  private static <T> T withAnsi(final boolean ansi, @Nonnull final Supplier<T> action) {
    final SparkSession spark = TransformFixtures.spark();
    final String previous = spark.conf().get(ANSI);
    spark.conf().set(ANSI, String.valueOf(ansi));
    try {
      return action.get();
    } finally {
      spark.conf().set(ANSI, previous);
    }
  }

  /** Returns the messages of an error and every cause, since Spark wraps what a task raised. */
  @Nonnull
  private static String messages(@Nonnull final Throwable error) {
    final StringBuilder messages = new StringBuilder();
    for (Throwable t = error; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    return messages.toString();
  }
}
