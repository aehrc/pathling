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

import static au.csiro.pathling.library.io.LayoutFixtures.newConditions;
import static au.csiro.pathling.library.io.LayoutFixtures.newPatients;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.PersistenceError;
import au.csiro.pathling.library.io.source.DatasetSource;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;

/**
 * Tests that the sinks writing a directory of files, Parquet and NDJSON, honour each save mode when
 * the directory already holds files of the type from an earlier write: an append keeps the rows of
 * both writes, an overwrite replaces them, an error-if-exists write is refused, and an ignored
 * write leaves them as they were.
 *
 * @author Piotr Szul
 */
class FileSinkSaveModeTest {

  /** The name a write gives each of its files: the type, the partition and the write's id. */
  private static final Pattern WRITTEN_FILE =
      Pattern.compile(
          "^Patient\\.(\\d{5})-([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\\.(\\w+)$");

  /**
   * The name a write gives each of its files where Spark splits a partition into several: the type,
   * the partition and the write's id, then the file counter for every file but the first.
   */
  @Nonnull
  private static final Pattern SPLIT_FILE =
      Pattern.compile(
          "^Patient\\.(\\d{5})-([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})"
              + "(?:-c(\\d+))?\\.(\\w+)$");

  /** The Spark setting that splits a partition into files holding at most so many rows. */
  @Nonnull private static final String MAX_RECORDS_PER_FILE = "spark.sql.files.maxRecordsPerFile";

  /** A file name for the type that is not only letters, which the file source does not read. */
  private static final String MAPPED_NAME = "Patient_v2";

  @TempDir Path directory;

  private static SparkSession spark;
  private static PathlingContext context;

  /** Set up Spark and the Pathling context. */
  @BeforeAll
  static void setUp() {
    spark = TestHelpers.spark();
    context = PathlingContext.create(spark);
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /** A second append adds its rows beside those of the first. */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void appendKeepsTheRowsOfAnEarlierWrite(@Nonnull final String format) {
    write(format, "append", patients("a1", "a2"));
    write(format, "append", patients("b1", "b2"));

    assertEquals(List.of("a1", "a2", "b1", "b2"), readIds(format));
  }

  /** An overwrite replaces the rows of an earlier write, and the files that held them. */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void overwriteReplacesTheRowsOfAnEarlierWrite(@Nonnull final String format) throws IOException {
    write(format, "error", patients("a1", "a2"));
    final Set<String> before = writeIds(fileNames(directory));
    write(format, "overwrite", patients("b1"));

    assertEquals(List.of("b1"), readIds(format));
    final Set<String> after = writeIds(fileNames(directory));
    assertEquals(1, after.size(), after::toString);
    assertTrue(before.stream().noneMatch(after::contains), after::toString);
  }

  /**
   * An error-if-exists write is refused where the type already has files, before any type is
   * written, and the directory is left as it was.
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void errorIfExistsIsRefusedWhereTheTypeHasFiles(@Nonnull final String format) throws IOException {
    write(format, "error", patients("a1", "a2"));
    final List<String> before = fileNames(directory);
    final DataSource both =
        context
            .read()
            .datasets()
            .dataset("Condition", newConditions(context, "c1"))
            .dataset("Patient", newPatients(context, "b1"));

    final PersistenceError error =
        assertThrows(PersistenceError.class, () -> write(format, "error", both));

    assertTrue(error.getMessage().contains("Patient"), error.getMessage());
    assertEquals(before, fileNames(directory));
    assertEquals(List.of("a1", "a2"), readIds(format));
  }

  /** An ignored write leaves the files an earlier write gave the type as they were. */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void ignoreLeavesTheFilesOfAnEarlierWrite(@Nonnull final String format) throws IOException {
    write(format, "error", patients("a1", "a2"));
    final List<String> before = fileNames(directory);
    write(format, "ignore", patients("b1"));

    assertEquals(before, fileNames(directory));
    assertEquals(List.of("a1", "a2"), readIds(format));
  }

  /**
   * A single write names each file for the type, its partition and the id of the write, one id
   * across all of its files, and leaves no partition directory behind.
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void aWriteNamesItsFilesForTheTypeThePartitionAndTheWrite(@Nonnull final String format)
      throws IOException {
    final List<String> written =
        write(format, "error", patients("a1", "a2", "a3")).fileInfos().stream()
            .map(info -> new org.apache.hadoop.fs.Path(info.absoluteUrl()).getName())
            .sorted()
            .toList();

    final List<String> names = fileNames(directory);
    assertEquals(names, written);
    assertFalse(names.isEmpty());
    for (final String name : names) {
      final Matcher matcher = WRITTEN_FILE.matcher(name);
      assertTrue(matcher.matches(), name);
      assertEquals(format, matcher.group(3));
    }
    assertEquals(1, writeIds(names).size(), names::toString);
    try (final Stream<Path> entries = Files.list(directory)) {
      assertTrue(entries.noneMatch(Files::isDirectory));
    }
  }

  /**
   * The Parquet source reads files named by earlier releases, {@code Patient.00000.parquet}, and
   * qualified files, {@code Patient.ICU.parquet}, as the type, beside those named by this release,
   * and an append beside them keeps every row.
   */
  @Test
  void parquetFilesNamedByEarlierReleasesAreReadBesideNewOnes() {
    newPatients(context, "o1").coalesce(1).write().parquet(directory.resolve("o").toString());
    newPatients(context, "q1").coalesce(1).write().parquet(directory.resolve("q").toString());
    moveOnlyPartFile(directory.resolve("o"), directory.resolve("Patient.00000.parquet"));
    moveOnlyPartFile(directory.resolve("q"), directory.resolve("Patient.ICU.parquet"));

    write("parquet", "append", patients("b1"));

    assertEquals(List.of("b1", "o1", "q1"), readIds("parquet"));
  }

  /**
   * The NDJSON source reads files named by earlier releases, {@code Patient.00000.ndjson}, beside
   * those named by this release.
   */
  @Test
  void ndjsonFilesNamedByEarlierReleasesAreReadBesideNewOnes() throws IOException {
    Files.writeString(
        directory.resolve("Patient.00000.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"o1\"}\n");

    write("ndjson", "append", patients("b1"));

    assertEquals(List.of("b1", "o1"), readIds("ndjson"));
  }

  /**
   * An append into a directory whose table was assembled by hand, holding a file Spark did not
   * name, succeeds and keeps every row: the file is left in the table, which is still read as the
   * type.
   */
  @Test
  void appendBesideAHandAssembledTableKeepsEveryRow() throws IOException {
    newPatients(context, "h1").coalesce(1).write().parquet(directory.resolve("h").toString());
    Files.createDirectories(directory.resolve("Patient.parquet"));
    moveOnlyPartFile(
        directory.resolve("h"), directory.resolve("Patient.parquet/part-00000.snappy.parquet"));

    write("parquet", "append", patients("b1"));

    assertEquals(List.of("b1", "h1"), readIds("parquet"));
    assertTrue(Files.exists(directory.resolve("Patient.parquet/part-00000.snappy.parquet")));
  }

  /**
   * An overwrite under a file name that is not only letters replaces the files of an earlier write
   * under that name. Such files are not read by the file source, so the files are compared.
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void overwriteUnderANameNotOnlyLettersReplacesTheEarlierFiles(@Nonnull final String format)
      throws IOException {
    writeMapped(format, "error", patients("a1", "a2"));
    final List<String> before = fileNames(directory);
    writeMapped(format, "overwrite", patients("b1"));

    final List<String> after = fileNames(directory);
    assertFalse(after.isEmpty());
    assertTrue(
        after.stream().allMatch(name -> name.startsWith(MAPPED_NAME + ".")), after::toString);
    assertTrue(before.stream().noneMatch(after::contains), after::toString);
  }

  /**
   * An append under a file name that is not only letters checks the layout of the earlier files,
   * which it now finds, and adds its files beside them.
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void appendUnderANameNotOnlyLettersKeepsTheEarlierFiles(@Nonnull final String format)
      throws IOException {
    writeMapped(format, "error", patients("a1"));
    final List<String> before = fileNames(directory);
    writeMapped(format, "append", patients("b1"));

    final List<String> after = fileNames(directory);
    assertTrue(after.containsAll(before), after::toString);
    assertTrue(after.size() > before.size(), after::toString);
    assertTrue(
        after.stream().allMatch(name -> name.startsWith(MAPPED_NAME + ".")), after::toString);
  }

  /** An error-if-exists write under a file name that is not only letters sees the earlier files. */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void errorIfExistsUnderANameNotOnlyLettersIsRefused(@Nonnull final String format)
      throws IOException {
    writeMapped(format, "error", patients("a1"));
    final List<String> before = fileNames(directory);
    final DatasetSource second = patients("b1");

    assertThrows(PersistenceError.class, () -> writeMapped(format, "error", second));
    assertEquals(before, fileNames(directory));
  }

  /** An ignored write under a file name that is not only letters leaves the earlier files. */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void ignoreUnderANameNotOnlyLettersLeavesTheEarlierFiles(@Nonnull final String format)
      throws IOException {
    writeMapped(format, "error", patients("a1"));
    final List<String> before = fileNames(directory);
    writeMapped(format, "ignore", patients("b1"));

    assertEquals(before, fileNames(directory));
  }

  /**
   * A write that Spark splits into several files for one partition, as {@code
   * spark.sql.files.maxRecordsPerFile} makes it, gives each file a name of its own that keeps the
   * file counter, and every row is read back as the type.
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void aPartitionSplitIntoSeveralFilesKeepsEveryRow(@Nonnull final String format)
      throws IOException {
    final List<String> written =
        withOneRowPerFile(() -> write(format, "error", patientsInOnePartition("a1", "a2", "a3")))
            .fileInfos()
            .stream()
            .map(info -> new org.apache.hadoop.fs.Path(info.absoluteUrl()).getName())
            .sorted()
            .toList();

    assertEquals(List.of("a1", "a2", "a3"), readIds(format));
    final List<String> names = fileNames(directory);
    assertEquals(names, written);
    assertEquals(3, names.size(), names::toString);
    final List<Matcher> matchers = names.stream().map(SPLIT_FILE::matcher).toList();
    assertTrue(matchers.stream().allMatch(Matcher::matches), names::toString);
    assertEquals(
        1, matchers.stream().map(matcher -> matcher.group(2)).distinct().count(), names::toString);
    assertEquals(
        2, matchers.stream().filter(matcher -> matcher.group(3) != null).count(), names::toString);
  }

  /**
   * An overwrite under a file name that is not only letters replaces every file of an earlier write
   * that Spark split into several files for one partition.
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "ndjson"})
  void overwriteUnderANameNotOnlyLettersReplacesEveryFileOfASplitWrite(@Nonnull final String format)
      throws IOException {
    withOneRowPerFile(() -> writeMapped(format, "error", patientsInOnePartition("a1", "a2", "a3")));
    final List<String> before = fileNames(directory);
    writeMapped(format, "overwrite", patients("b1"));

    final List<String> after = fileNames(directory);
    assertEquals(3, before.size(), before::toString);
    assertFalse(after.isEmpty());
    assertTrue(before.stream().noneMatch(after::contains), after::toString);
  }

  @Nonnull
  private static DatasetSource patientsInOnePartition(@Nonnull final String... ids) {
    return context.read().datasets().dataset("Patient", newPatients(context, ids).coalesce(1));
  }

  /**
   * Runs a write with Spark limited to one row per file, restoring the setting afterwards so that
   * it does not reach other tests sharing the session.
   */
  @Nonnull
  private static <T> T withOneRowPerFile(@Nonnull final Supplier<T> write) {
    final Option<String> previous = spark.conf().getOption(MAX_RECORDS_PER_FILE);
    spark.conf().set(MAX_RECORDS_PER_FILE, 1L);
    try {
      return write.get();
    } finally {
      if (previous.isDefined()) {
        spark.conf().set(MAX_RECORDS_PER_FILE, previous.get());
      } else {
        spark.conf().unset(MAX_RECORDS_PER_FILE);
      }
    }
  }

  @Nonnull
  private static DatasetSource patients(@Nonnull final String... ids) {
    return context.read().datasets().dataset("Patient", newPatients(context, ids));
  }

  @Nonnull
  private WriteDetails write(
      @Nonnull final String format, @Nonnull final String mode, @Nonnull final DataSource source) {
    final DataSinkBuilder builder = new DataSinkBuilder(context, source).saveMode(mode);
    return "parquet".equals(format)
        ? builder.parquet(directory.toString())
        : builder.ndjson(directory.toString());
  }

  @Nonnull
  private WriteDetails writeMapped(
      @Nonnull final String format, @Nonnull final String mode, @Nonnull final DataSource source) {
    final DataSinkBuilder builder = new DataSinkBuilder(context, source).saveMode(mode);
    return "parquet".equals(format)
        ? builder.parquet(directory.toString(), type -> MAPPED_NAME)
        : builder.ndjson(directory.toString(), type -> MAPPED_NAME);
  }

  @Nonnull
  private List<String> readIds(@Nonnull final String format) {
    final Dataset<Row> patients =
        ("parquet".equals(format)
                ? context.read().parquet(directory.toString())
                : context.read().ndjson(directory.toString()))
            .read("Patient");
    return patients.select("id").as(Encoders.STRING()).collectAsList().stream().sorted().toList();
  }

  /** Returns the names of the data files in a directory, leaving out Hadoop's checksum files. */
  @Nonnull
  private static List<String> fileNames(@Nonnull final Path root) throws IOException {
    try (final Stream<Path> entries = Files.list(root)) {
      return entries
          .map(entry -> entry.getFileName().toString())
          .filter(name -> !name.startsWith("."))
          .sorted()
          .toList();
    }
  }

  @Nonnull
  private static Set<String> writeIds(@Nonnull final List<String> names) {
    return names.stream()
        .map(WRITTEN_FILE::matcher)
        .filter(Matcher::matches)
        .map(matcher -> matcher.group(2))
        .collect(Collectors.toSet());
  }

  private static void moveOnlyPartFile(
      @Nonnull final Path sparkOutput, @Nonnull final Path target) {
    try (final Stream<Path> entries = Files.list(sparkOutput)) {
      final Path part =
          entries
              .filter(entry -> entry.getFileName().toString().endsWith(".parquet"))
              .findFirst()
              .orElseThrow();
      Files.move(part, target);
      try (final Stream<Path> rest = Files.walk(sparkOutput)) {
        rest.sorted((a, b) -> b.compareTo(a)).forEach(FileSinkSaveModeTest::deleteQuietly);
      }
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void deleteQuietly(@Nonnull final Path path) {
    try {
      Files.delete(path);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
