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

package au.csiro.pathling.io.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.transform.TransformFixtures;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that the writer's two outputs agree: the files it writes hold exactly the documents it
 * returns as a dataset, with the null fields left out of both (decision 70, FR-019).
 *
 * <p>The files are written by the JSON writer from the shaped dataset directly and the documents by
 * {@code to_json}, so the two go through different serialisers and could drift apart.
 */
class FhirJsonWriterTest {

  /**
   * The second name has no family and the first no given names, so both leave nulls in the stored
   * rows for the writer to omit, and the base64 value is encoded again on the way out.
   */
  @Nonnull
  private static final List<String> DOCUMENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"1\","
              + "\"name\":[{\"family\":\"Smith\"},{\"given\":[\"Jane\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"2\",\"gender\":\"male\","
              + "\"photo\":[{\"data\":\"aGVsbG8=\"}]}");

  @Test
  void writesFilesHoldingTheDocumentsItReturns(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Dataset<Row> stored = stored(directory.resolve("in"));
    final Path out = directory.resolve("out");

    TransformFixtures.writer().write("Patient", stored, out.toString(), SaveMode.ErrorIfExists);

    final List<String> returned =
        TransformFixtures.writer().write("Patient", stored).collectAsList();
    assertEquals(returned.stream().sorted().toList(), lines(out));
  }

  @Test
  void writesADecimalToFilesAsTheNumberItReturns(@TempDir @Nonnull final Path directory)
      throws IOException {
    // A decimal reaches the writer as a double and an integer as an integer, and a double's
    // rendering is where two serialisers would most plausibly disagree.
    final Path in = Files.createDirectories(directory.resolve("in"));
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read(
                "Observation",
                TransformFixtures.corpus(
                    in,
                    "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                        + "\"valueQuantity\":{\"value\":1.50,\"unit\":\"1.50\"}}",
                    "{\"resourceType\":\"Observation\",\"id\":\"2\",\"status\":\"final\","
                        + "\"valueInteger\":7}"));
    final Path out = directory.resolve("out");

    TransformFixtures.writer().write("Observation", stored, out.toString(), SaveMode.ErrorIfExists);

    final List<String> written = lines(out);
    assertEquals(
        TransformFixtures.writer().write("Observation", stored).collectAsList().stream()
            .sorted()
            .toList(),
        written);
    assertTrue(written.get(0).contains("\"value\":1.5,"), written.get(0));
    assertTrue(written.get(1).contains("\"valueInteger\":7"), written.get(1));
  }

  @Test
  void leavesNullFieldsOutOfTheFilesItWrites(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path out = directory.resolve("out");

    TransformFixtures.writer()
        .write("Patient", stored(directory.resolve("in")), out.toString(), SaveMode.ErrorIfExists);

    final List<String> written = lines(out);
    assertEquals(DOCUMENTS.size(), written.size());
    written.forEach(line -> assertFalse(line.contains("null"), line));
  }

  @Nonnull
  private static Dataset<Row> stored(@Nonnull final Path directory) throws IOException {
    Files.createDirectories(directory);
    return TransformFixtures.reader()
        .read("Patient", TransformFixtures.corpus(directory, DOCUMENTS.toArray(String[]::new)));
  }

  /** Returns every line of every part file the JSON writer wrote, sorted. */
  @Nonnull
  private static List<String> lines(@Nonnull final Path directory) throws IOException {
    try (final Stream<Path> files = Files.list(directory)) {
      return files
          .filter(file -> file.getFileName().toString().startsWith("part-"))
          .flatMap(
              file -> {
                try {
                  return Files.readAllLines(file).stream();
                } catch (final IOException e) {
                  throw new UncheckedIOException(e);
                }
              })
          .filter(line -> !line.isBlank())
          .sorted()
          .toList();
    }
  }
}
