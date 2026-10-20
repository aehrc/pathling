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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.io.PersistenceError;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests the renaming of a sink's partition files that keeps the id Spark gave the write.
 *
 * @author Piotr Szul
 */
class TypeFilesTest {

  private static final String WRITE_ID = "7a60a041-0e7d-4e24-b23d-82405a665677";

  @TempDir private File tempDir;

  private FileSystem fs;
  private Path directory;

  @BeforeEach
  void setUp() throws IOException {
    fs = FileSystem.get(new Configuration());
    directory = new Path(tempDir.toURI());
    final FileSystem localFs = FileSystem.getLocal(new Configuration());
    for (final FileStatus file :
        localFs.listStatus(new Path("src/test/resources/test-data/spark-partitioned-data/"))) {
      fs.copyFromLocalFile(file.getPath(), new Path(directory, file.getPath().getName()));
    }
  }

  @Test
  void filesKeepTheWriteId() throws IOException {
    final Path table = new Path(directory, "Patient.ndjson");

    final List<String> renamed = TypeFiles.renameKeepingWriteId(fs, table.toString(), "txt");

    assertEquals(4, renamed.size());
    for (final String part : List.of("00000", "00001", "00002", "00003")) {
      assertTrue(fs.exists(new Path(directory, "Patient." + part + "-" + WRITE_ID + ".ndjson")));
    }
    assertFalse(fs.exists(table));
  }

  /**
   * Databricks names the files its commit protocol writes with a transaction id between the
   * partition id and the id of the write, and a task attempt and counter after it.
   */
  @Test
  void filesNamedByDatabricksKeepTheWriteId() throws IOException {
    final Path table = new Path(directory, "Condition.ndjson");
    fs.mkdirs(table);
    fs.create(new Path(table, "part-00000-tid-5046671251912249212-" + WRITE_ID + "-5-1-c000.txt"))
        .close();
    fs.create(new Path(table, "part-00001-tid-5046671251912249212-" + WRITE_ID + "-6-1-c000.txt"))
        .close();

    final List<String> renamed = TypeFiles.renameKeepingWriteId(fs, table.toString(), "txt");

    assertEquals(2, renamed.size());
    for (final String part : List.of("00000", "00001")) {
      assertTrue(fs.exists(new Path(directory, "Condition." + part + "-" + WRITE_ID + ".ndjson")));
    }
    assertFalse(fs.exists(table));
  }

  /**
   * Spark splits a partition into several files, numbered by a file counter, where {@code
   * spark.sql.files.maxRecordsPerFile} limits the rows of each. The first keeps the name a file
   * that is not split is given, and the others keep their counter, so no two take the same name.
   * Databricks names its files with a task attempt before the counter, which is not kept. Some of
   * Spark's writers introduce the counter with a period rather than a hyphen.
   *
   * @param prefix the start of each file's name, before the id of the write
   * @param suffix the end of each file's name, after the id of the write and before the counter
   */
  @ParameterizedTest
  @CsvSource({
    "'part-00000-', '-'",
    "'part-00000-', '.'",
    "'part-00000-tid-5046671251912249212-', '-5-1-'"
  })
  void filesOfOnePartitionKeepTheFileCounter(
      @Nonnull final String prefix, @Nonnull final String suffix) throws IOException {
    final Path table = new Path(directory, "Observation.ndjson");
    fs.mkdirs(table);
    for (final String counter : List.of("c000", "c001", "c010", "c1000")) {
      fs.create(new Path(table, prefix + WRITE_ID + suffix + counter + ".txt")).close();
    }

    final List<String> renamed = TypeFiles.renameKeepingWriteId(fs, table.toString(), "txt");

    assertEquals(4, renamed.size());
    for (final String qualifier :
        List.of(
            "00000-" + WRITE_ID,
            "00000-" + WRITE_ID + "-c001",
            "00000-" + WRITE_ID + "-c010",
            "00000-" + WRITE_ID + "-c1000")) {
      assertTrue(fs.exists(new Path(directory, "Observation." + qualifier + ".ndjson")));
    }
    assertFalse(fs.exists(table));
  }

  /**
   * Databricks' task attempt is dropped from the name, so two attempts of one partition take the
   * same name. The write is refused before any file is renamed, so the table is left in place with
   * both of its files rather than one file replacing the other, or one being moved beside a table
   * that still holds the other.
   */
  @Test
  void twoAttemptsOfOnePartitionAreRefused() throws IOException {
    final Path table = new Path(directory, "Encounter.ndjson");
    fs.mkdirs(table);
    for (final String attempt : List.of("1", "2")) {
      fs.create(
              new Path(
                  table,
                  "part-00000-tid-5046671251912249212-" + WRITE_ID + "-5-" + attempt + "-c000.txt"))
          .close();
    }
    final String tableUrl = table.toString();

    final PersistenceError exception =
        assertThrows(
            PersistenceError.class, () -> TypeFiles.renameKeepingWriteId(fs, tableUrl, "txt"));

    assertTrue(exception.getMessage().contains("would both be renamed"), exception::getMessage);
    assertTrue(fs.exists(table));
    assertEquals(2, fs.listStatus(table, p -> p.getName().endsWith(".txt")).length);
    assertEquals(0, fs.listStatus(directory, p -> p.getName().startsWith("Encounter.0")).length);
  }

  @Test
  void aFileNotNamedBySparkIsRefused() throws IOException {
    final Path table = new Path(directory, "NotSpark.ndjson");
    fs.mkdirs(table);
    fs.create(new Path(table, "part-00000-hash.txt")).close();
    final String tableUrl = table.toString();

    final PersistenceError exception =
        assertThrows(
            PersistenceError.class, () -> TypeFiles.renameKeepingWriteId(fs, tableUrl, "txt"));

    assertTrue(exception.getMessage().contains("part-<partitionId>-<writeId>"));
    assertTrue(fs.exists(table));
  }

  @Test
  void aFileNotNamedBySparkBesideTheWriteIsLeftInTheTable() throws IOException {
    final Path table = new Path(directory, "Patient.ndjson");
    final Path foreign = new Path(table, "part-00000.txt");
    fs.create(foreign).close();

    final List<String> renamed = TypeFiles.renameKeepingWriteId(fs, table.toString(), "txt");

    assertEquals(4, renamed.size());
    for (final String part : List.of("00000", "00001", "00002", "00003")) {
      assertTrue(fs.exists(new Path(directory, "Patient." + part + "-" + WRITE_ID + ".ndjson")));
    }
    assertTrue(fs.exists(foreign));
  }

  @Test
  void aFailedRenameIsAnErrorAndKeepsTheTable() throws IOException {
    final Path table = new Path(directory, "Patient.ndjson");
    final FileSystem refusingRenames =
        new FilterFileSystem(fs) {
          @Override
          public boolean rename(final Path src, final Path dst) {
            return false;
          }
        };
    final String tableUrl = table.toString();

    final PersistenceError exception =
        assertThrows(
            PersistenceError.class,
            () -> TypeFiles.renameKeepingWriteId(refusingRenames, tableUrl, "txt"));

    assertTrue(exception.getMessage().contains("Could not rename the partition file"));
    assertEquals(4, fs.listStatus(table, p -> p.getName().endsWith(".txt")).length);
  }

  /**
   * A rename that fails after others have succeeded returns those files to the table, so the rows
   * of the write are read from the table only, not also from beside it.
   */
  @Test
  void aRenameFailingPartWayReturnsTheRenamedFilesToTheTable() throws IOException {
    final Path table = new Path(directory, "Patient.ndjson");
    final AtomicInteger renames = new AtomicInteger();
    final FileSystem refusingTheSecondRename =
        new FilterFileSystem(fs) {
          @Override
          public boolean rename(final Path src, final Path dst) throws IOException {
            // Only renames out of the table are counted, so that the restoring ones succeed.
            if (src.getParent().getName().equals(table.getName())
                && renames.incrementAndGet() == 2) {
              return false;
            }
            return super.rename(src, dst);
          }
        };
    final String tableUrl = table.toString();

    final PersistenceError exception =
        assertThrows(
            PersistenceError.class,
            () -> TypeFiles.renameKeepingWriteId(refusingTheSecondRename, tableUrl, "txt"));

    assertTrue(exception.getMessage().contains("Could not rename the partition file"));
    assertFalse(exception.getMessage().contains("could not be returned"), exception::getMessage);
    assertEquals(4, fs.listStatus(table, p -> p.getName().endsWith(".txt")).length);
    assertEquals(0, fs.listStatus(directory, p -> p.getName().startsWith("Patient.0")).length);
  }

  @Test
  void aTableWithoutPartitionFilesIsRefused() throws IOException {
    final Path table = new Path(directory, "Empty.ndjson");
    fs.mkdirs(table);
    final String tableUrl = table.toString();

    final PersistenceError exception =
        assertThrows(
            PersistenceError.class, () -> TypeFiles.renameKeepingWriteId(fs, tableUrl, "txt"));

    assertTrue(exception.getMessage().contains("No partition files were found"));
  }

  @Test
  void aTableNotNamedForItsTypeAndExtensionIsRefused() {
    final String tableUrl = new Path(directory, "Patient.backup.ndjson").toString();

    final PersistenceError exception =
        assertThrows(
            PersistenceError.class, () -> TypeFiles.renameKeepingWriteId(fs, tableUrl, "txt"));

    assertTrue(exception.getMessage().contains("<name>.<extension>"));
  }
}
