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

package au.csiro.pathling.library.io.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.source.DatasetSource;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

/**
 * Tests how the range annotations of a date are stored (decision 138): as Parquet {@code INT64}
 * with {@code TIMESTAMP(isAdjustedToUTC=true, MICROS)} by a session set as Pathling sets the
 * sessions it builds, to Parquet and to Delta, including a merge. A session set otherwise is warned
 * of when the context is created, and refuses to write data that carries them.
 */
class RangeAnnotationTypeTest {

  /** The Spark setting that says how timestamps are written to Parquet. */
  private static final String OUTPUT_TIMESTAMP_TYPE = "spark.sql.parquet.outputTimestampType";

  private static final String TIMESTAMP_MICROS = "TIMESTAMP_MICROS";

  /** The type of a bound in the footer of a file written in microseconds. */
  private static final String MICROS = "optional int64 %s (TIMESTAMP(MICROS,true))";

  /** The type of a bound in the footer of a file written as INT96. */
  private static final String INT96 = "optional int96 %s";

  @TempDir Path directory;

  private static SparkSession spark;

  private static PathlingContext context;

  private ListAppender<ILoggingEvent> logs;

  @BeforeAll
  static void setUp() {
    spark =
        TestHelpers.sparkBuilder().config(OUTPUT_TIMESTAMP_TYPE, TIMESTAMP_MICROS).getOrCreate();
    spark.conf().set(OUTPUT_TIMESTAMP_TYPE, TIMESTAMP_MICROS);
    context = PathlingContext.create(spark);
  }

  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  @AfterEach
  void restoreTheSession() {
    spark.conf().set(OUTPUT_TIMESTAMP_TYPE, TIMESTAMP_MICROS);
    if (logs != null) {
      logger().detachAppender(logs);
    }
  }

  @Test
  void parquetStoresTheBoundsAsMicroseconds() throws IOException {
    new DataSinkBuilder(context, patients(context, "p1:1980")).parquet(directory.toString());

    assertBounds(directory, MICROS);
  }

  @Test
  void deltaStoresTheBoundsAsMicrosecondsWhenWrittenAndMerged() throws IOException {
    final String path = directory.resolve("delta").toString();
    new DataSinkBuilder(context, patients(context, "p1:1980")).delta(path);
    new DataSinkBuilder(context, patients(context, "p2:1980-06")).saveMode("merge").delta(path);

    assertBounds(directory.resolve("delta"), MICROS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"INT96", "TIMESTAMP_MILLIS"})
  void aSessionNotInMicrosecondsRefusesToWriteDates(@Nonnull final String type) throws IOException {
    spark.conf().set(OUTPUT_TIMESTAMP_TYPE, type);
    final DatasetSource source = patients(context, "p1:1980");

    for (final Executable write :
        List.<Executable>of(
            () -> new DataSinkBuilder(context, source).parquet(directory.toString()),
            () -> new DataSinkBuilder(context, source).delta(directory.resolve("d").toString()))) {
      final IllegalStateException error = assertThrows(IllegalStateException.class, write);
      assertTrue(error.getMessage().contains(OUTPUT_TIMESTAMP_TYPE + "=" + TIMESTAMP_MICROS));
    }
    try (final Stream<Path> written = Files.list(directory)) {
      assertEquals(0, written.count());
    }
  }

  @Test
  void aSessionNotInMicrosecondsWritesDataWithoutDates() {
    spark.conf().set(OUTPUT_TIMESTAMP_TYPE, "INT96");
    final DatasetSource source =
        context
            .read()
            .datasets()
            .dataset(
                "Patient",
                context.encode(
                    spark
                        .createDataset(
                            List.of("{\"resourceType\":\"Patient\",\"id\":\"p1\"}"),
                            Encoders.STRING())
                        .toDF(),
                    "Patient"));

    assertDoesNotThrow(() -> new DataSinkBuilder(context, source).parquet(directory.toString()));
  }

  @Test
  void aSuppliedSessionLeftAtInt96IsWarnedOf() {
    captureLogs();
    spark.conf().set(OUTPUT_TIMESTAMP_TYPE, "INT96");
    PathlingContext.create(spark);

    assertTrue(
        logs.list.stream()
            .anyMatch(
                event ->
                    event.getLevel() == Level.WARN
                        && event
                            .getFormattedMessage()
                            .contains("as INT96 (`" + OUTPUT_TIMESTAMP_TYPE + "`)")
                        && event.getFormattedMessage().contains("will fail")
                        && event
                            .getFormattedMessage()
                            .contains(OUTPUT_TIMESTAMP_TYPE + "=" + TIMESTAMP_MICROS)));
  }

  @Test
  void aSuppliedSessionInMicrosecondsIsNotWarnedOf() {
    captureLogs();
    PathlingContext.create(spark);

    assertFalse(
        logs.list.stream()
            .anyMatch(event -> event.getFormattedMessage().contains(OUTPUT_TIMESTAMP_TYPE)));
  }

  @Test
  void aBuiltSessionWritesMicroseconds() {
    // Without an active or default session, the context builds one, which shares the test's
    // SparkContext and so is not stopped.
    spark.conf().set(OUTPUT_TIMESTAMP_TYPE, "INT96");
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    try {
      final PathlingContext builtContext = PathlingContext.create();

      assertNotSame(spark, builtContext.getSpark());
      assertEquals(TIMESTAMP_MICROS, builtContext.getSpark().conf().get(OUTPUT_TIMESTAMP_TYPE));
    } finally {
      SparkSession.setActiveSession(spark);
      SparkSession.setDefaultSession(spark);
    }
  }

  @Test
  void anActiveSessionIsUsedAsItIs() {
    // The default session is the active one where there is one, and its settings are the user's.
    spark.conf().set(OUTPUT_TIMESTAMP_TYPE, "INT96");
    captureLogs();
    final PathlingContext defaultContext = PathlingContext.create();

    assertSame(spark, defaultContext.getSpark());
    assertEquals("INT96", spark.conf().get(OUTPUT_TIMESTAMP_TYPE));
    assertTrue(
        logs.list.stream()
            .anyMatch(event -> event.getFormattedMessage().contains(OUTPUT_TIMESTAMP_TYPE)));
  }

  private void captureLogs() {
    logs = new ListAppender<>();
    logs.start();
    logger().addAppender(logs);
  }

  @Nonnull
  private static Logger logger() {
    return (Logger) LoggerFactory.getLogger(PathlingContext.class);
  }

  /** Returns Patients, each given as its id and its birth date, separated by a colon. */
  @Nonnull
  private static DatasetSource patients(
      @Nonnull final PathlingContext pathling, @Nonnull final String... patients) {
    return pathling
        .read()
        .datasets()
        .dataset(
            "Patient",
            pathling.encode(
                spark
                    .createDataset(
                        Stream.of(patients)
                            .map(patient -> patient.split(":"))
                            .map(
                                patient ->
                                    "{\"resourceType\":\"Patient\",\"id\":\""
                                        + patient[0]
                                        + "\",\"birthDate\":\""
                                        + patient[1]
                                        + "\"}")
                            .toList(),
                        Encoders.STRING())
                    .toDF(),
                "Patient"));
  }

  /** Asserts that every Parquet file under a directory stores both bounds as the given type. */
  private static void assertBounds(@Nonnull final Path root, @Nonnull final String type)
      throws IOException {
    final List<Path> files;
    try (final Stream<Path> walk = Files.walk(root)) {
      files =
          walk.filter(Files::isRegularFile)
              .filter(file -> file.toString().endsWith(".parquet"))
              .toList();
    }
    assertFalse(files.isEmpty());
    for (final Path file : files) {
      try (final ParquetFileReader reader =
          ParquetFileReader.open(
              HadoopInputFile.fromPath(
                  new org.apache.hadoop.fs.Path(file.toString()), new Configuration()))) {
        final String group =
            reader.getFooter().getFileMetaData().getSchema().getType("_birthDate").toString();
        assertEquals(
            List.of(String.format(type, "__start"), String.format(type, "__end")),
            group
                .lines()
                .map(String::trim)
                .filter(line -> line.startsWith("optional int"))
                .map(line -> line.replace(";", ""))
                .toList(),
            file.toString());
      }
    }
  }
}
