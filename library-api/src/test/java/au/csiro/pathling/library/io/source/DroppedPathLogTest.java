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

package au.csiro.pathling.library.io.source;

import static au.csiro.pathling.library.io.source.SchemaSetSources.PATIENT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.utilities.AnnotationNames;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

/**
 * Tests the debug log of the stored paths a schema set drops (T067, FR-006a). The stored Patients
 * carry {@code active} and {@code name.given}, which the defined schema lacks.
 */
class DroppedPathLogTest {

  @Nonnull private static final String SOURCES = "au.csiro.pathling.library.io.source";

  @TempDir static Path warehouse;

  private static SparkSession spark;
  private static PathlingContext context;

  @TempDir Path directory;

  private String database;
  private ListAppender<ILoggingEvent> logs;
  private Level level;

  /** Set up Spark, with a warehouse in a temporary directory, and the Pathling context. */
  @BeforeAll
  static void setUp() {
    spark =
        TestHelpers.sparkBuilder()
            .config("spark.sql.warehouse.dir", warehouse.resolve("spark-warehouse").toString())
            .getOrCreate();
    context = PathlingContext.create(spark);
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /** Give each test a database of its own, and capture the sources' log at debug level. */
  @BeforeEach
  void captureLogs() {
    database = "db_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql("CREATE DATABASE " + database);
    level = logger().getLevel();
    logger().setLevel(Level.DEBUG);
    logs = new ListAppender<>();
    logs.start();
    logger().addAppender(logs);
  }

  /** Stop capturing the log and restore its level. */
  @AfterEach
  void restoreLogs() {
    logger().detachAppender(logs);
    logger().setLevel(level);
  }

  /**
   * A source whose stored schema is known without reading rows logs each path the defined schema
   * drops, other than annotations, by the time {@code read} returns and with no Spark job beyond
   * those the same read runs without a schema set (FR-006a).
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "delta", "tables", "datasets"})
  void aStoredReadLogsEachDroppedPathWithNoSparkJob(@Nonnull final String kind)
      throws TimeoutException {
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    SchemaSetSources.write(kind, context, directory, database);
    // A first read warms up the session, so that the two reads compared below do the same work.
    jobsToRead(kind, context.read());
    final int inferred = jobsToRead(kind, context.read());
    logs.list.clear();

    final int defined = jobsToRead(kind, context.read().withSchemas(Map.of(PATIENT, schema)));

    final List<String> messages = messages();
    assertTrue(defined <= inferred, () -> defined + " jobs, against " + inferred + " without");
    assertTrue(mentions(messages, "active"), () -> "should log active: " + messages);
    assertTrue(mentions(messages, "name.given"), () -> "should log name.given: " + messages);
    assertFalse(
        mentions(messages, AnnotationNames.PREFIX), () -> "should log no annotation: " + messages);
  }

  /**
   * A text source logs nothing about dropped content, since finding it would need per-row work
   * (FR-006a).
   */
  @ParameterizedTest
  @ValueSource(strings = {"ndjson", "bundlesJson", "bundlesXml"})
  void aTextReadLogsNoDroppedPath(@Nonnull final String kind) {
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    SchemaSetSources.write(kind, context, directory, database);

    SchemaSetSources.open(
            kind, context.read().withSchemas(Map.of(PATIENT, schema)), directory, database)
        .read(PATIENT);

    final List<String> messages = messages();
    assertFalse(mentions(messages, "active"), () -> "should log no dropped path: " + messages);
    assertFalse(mentions(messages, "name.given"), () -> "should log no dropped path: " + messages);
  }

  /** Returns the number of Spark jobs that opening a source and reading the Patients runs. */
  private int jobsToRead(@Nonnull final String kind, @Nonnull final DataSourceBuilder builder)
      throws TimeoutException {
    final AtomicInteger jobs = new AtomicInteger();
    final SparkListener listener =
        new SparkListener() {
          @Override
          public void onJobEnd(final SparkListenerJobEnd jobEnd) {
            jobs.incrementAndGet();
          }
        };
    spark.sparkContext().addSparkListener(listener);
    try {
      SchemaSetSources.open(kind, builder, directory, database).read(PATIENT);
      spark.sparkContext().listenerBus().waitUntilEmpty();
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }
    return jobs.get();
  }

  @Nonnull
  private List<String> messages() {
    return logs.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
  }

  private static boolean mentions(
      @Nonnull final List<String> messages, @Nonnull final String text) {
    return messages.stream().anyMatch(message -> message.contains(text));
  }

  @Nonnull
  private static Logger logger() {
    return (Logger) LoggerFactory.getLogger(SOURCES);
  }
}
