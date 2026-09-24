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
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.io.transform.TransformFixtures;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
