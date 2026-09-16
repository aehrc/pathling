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

import au.csiro.pathling.io.transform.DecimalTransform;
import au.csiro.pathling.io.transform.TransformFixtures;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the limitation R-009 accepted and FR-020 requires to be documented per ingest path: a
 * resource supplied as a dataset of strings does not keep the lexical form of its decimals.
 *
 * <p>Spark's string-backed JSON parser re-serialises every number it reads, so the value arrives
 * having been through a double whatever the reader is asked for. Nothing downstream can recover it,
 * and this is not a defect in the layout. Asserting the loss is what keeps it from silently
 * widening — a change that made this test fail is a change that fixed the limitation or moved it,
 * and either deserves to be noticed.
 */
class StringDatasetLimitationTest {

  @Nonnull
  private static final String OBSERVATION =
      "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
          + "\"valueQuantity\":{\"value\":1.50}}";

  @Test
  void losesTheLexicalFormOnADatasetOfStrings() {
    final Dataset<String> documents =
        TransformFixtures.spark().createDataset(List.of(OBSERVATION), Encoders.STRING());
    final Dataset<Row> source =
        TransformFixtures.spark()
            .read()
            .options(DecimalTransform.lexicalReadOptions())
            .json(documents);

    final Dataset<Row> stored = TransformFixtures.transformer().transform("Observation", source);

    assertEquals("1.5", value(stored));
  }

  @Test
  void keepsTheLexicalFormOnTheFilePath(@TempDir @Nonnull final Path directory) {
    final String path = TransformFixtures.corpus(directory, OBSERVATION);

    final Dataset<Row> stored =
        TransformFixtures.transformer().read(TransformFixtures.spark(), "Observation", path);

    assertEquals("1.50", value(stored));
  }

  @Nonnull
  private static String value(@Nonnull final Dataset<Row> stored) {
    return stored.selectExpr("valueQuantity.value").first().getString(0);
  }
}
