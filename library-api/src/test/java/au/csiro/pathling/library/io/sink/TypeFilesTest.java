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
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
