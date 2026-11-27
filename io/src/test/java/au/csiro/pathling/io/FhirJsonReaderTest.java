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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that the reader gives the same result from a dataset of documents as from a file of the
 * same documents, and that text which is not JSON fails the read from either (decision 70).
 */
class FhirJsonReaderTest {

  @Nonnull
  private static final List<String> DOCUMENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":\"female\","
              + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Jane\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"2\",\"multipleBirthInteger\":2,"
              + "\"birthDate\":\"1980-01-01\"}");

  @Test
  void readsADatasetOfDocumentsAsItReadsAFileOfThem(@TempDir @Nonnull final Path directory) {
    final String path = TransformFixtures.corpus(directory, DOCUMENTS.toArray(String[]::new));

    final Dataset<Row> fromFile = TransformFixtures.reader().read("Patient", path);
    final Dataset<Row> fromDataset =
        TransformFixtures.reader().read("Patient", documents(DOCUMENTS));

    assertEquals(fromFile.schema(), fromDataset.schema());
    assertEquals(sorted(fromFile), sorted(fromDataset));
  }

  @Test
  void failsOnADocumentThatIsNotJson() {
    final Dataset<String> documents =
        documents(List.of(DOCUMENTS.get(0), "{\"resourceType\":\"Patient\",\"id\":"));

    assertThrows(
        Exception.class,
        () -> TransformFixtures.reader().read("Patient", documents).collectAsList());
  }

  /**
   * A schema that leaves nothing to parse, such as an empty one, still fails the read on text that
   * is not JSON, by every route, and keeps a row for each document that is.
   */
  @ParameterizedTest
  @ValueSource(strings = {"files", "documents", "bundles"})
  void failsOnTextThatIsNotJsonWithASchemaThatLeavesNothingToParse(
      @Nonnull final String route, @TempDir @Nonnull final Path directory) throws IOException {
    final StructType empty = new StructType();

    assertEquals(DOCUMENTS.size(), readWithSchema(route, directory, DOCUMENTS, empty).count());
    final List<String> malformed = List.of(DOCUMENTS.get(0), "{not json");
    assertThrows(Exception.class, () -> readWithSchema(route, directory, malformed, empty).count());
  }

  /** Reads documents with a schema by a route: files, a dataset of them, or a bundle of them. */
  @Nonnull
  private static Dataset<Row> readWithSchema(
      @Nonnull final String route,
      @Nonnull final Path directory,
      @Nonnull final List<String> documents,
      @Nonnull final StructType schema)
      throws IOException {
    final FhirJsonReader reader = TransformFixtures.reader();
    return switch (route) {
      case "files" ->
          reader.read(
              "Patient",
              TransformFixtures.corpus(
                  Files.createTempDirectory(directory, "corpus"), documents.toArray(String[]::new)),
              schema);
      case "documents" -> reader.read("Patient", documents(documents), schema);
      case "bundles" ->
          reader.readBundles(
              "Patient",
              documents(
                  List.of(DefinedSchemaReadTest.jsonBundle(documents.toArray(String[]::new)))),
              schema);
      default -> throw new IllegalArgumentException(route);
    };
  }

  @Nonnull
  private static Dataset<String> documents(@Nonnull final List<String> documents) {
    return TransformFixtures.spark().createDataset(documents, Encoders.STRING());
  }

  @Nonnull
  private static List<Row> sorted(@Nonnull final Dataset<Row> dataset) {
    return dataset.collectAsList().stream()
        .sorted(Comparator.comparing(row -> row.<String>getAs("id")))
        .toList();
  }
}
