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

package au.csiro.pathling.library.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.library.TestHelpers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Guards the pinned Delta release against binary incompatibility with the pinned Spark release.
 *
 * <p>Delta calls Spark internals that can change between Spark patch releases. {@code CONVERT TO
 * DELTA} by path reads the Parquet footers through Spark's {@code ParquetToSparkSchemaConverter},
 * which Pathling's own Delta reads and writes do not reach, so a mismatch there would otherwise go
 * unnoticed until a user hit it (#2824).
 *
 * @author John Grimes
 */
class DeltaSparkCompatibilityTest {

  private static SparkSession spark;
  private static Path temporaryDirectory;

  /** Set up Spark and a temporary directory. */
  @BeforeAll
  static void setUp() throws IOException {
    temporaryDirectory = Files.createTempDirectory("pathling-delta-compatibility-test-");
    spark = TestHelpers.sparkBuilder().getOrCreate();
  }

  /** Tear down Spark and remove the temporary directory. */
  @AfterAll
  static void tearDown() throws IOException {
    spark.stop();
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  /**
   * Converting a Parquet directory to Delta by path produces a Delta table that reads back the rows
   * that were written as Parquet.
   */
  @Test
  void convertToDeltaByPathPreservesRows() {
    final String path = temporaryDirectory.resolve("patients").toString();

    // Arrange: write a plain Parquet directory.
    spark.sql("SELECT * FROM VALUES ('a', 1), ('b', 2) AS t(id, count)").write().parquet(path);

    // Act: convert it in place by path, which infers the schema from the Parquet footers.
    spark.sql("CONVERT TO DELTA parquet.`" + path + "`");

    // Assert: the directory is now a Delta table holding the original rows.
    final List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(List.of(RowFactory.create("a", 1), RowFactory.create("b", 2)), rows);
  }
}
