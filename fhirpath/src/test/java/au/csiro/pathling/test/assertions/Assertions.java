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

package au.csiro.pathling.test.assertions;

import static au.csiro.pathling.test.TestResources.getResourceAsUrl;

import au.csiro.pathling.fhirpath.evaluation.CollectionDataset;
import jakarta.annotation.Nonnull;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opentest4j.AssertionFailedError;

@Slf4j
public abstract class Assertions {

  @Nonnull
  public static CollectionAssert assertThat(@Nonnull final CollectionDataset datasetResult) {
    return new CollectionAssert(datasetResult);
  }

  @Nonnull
  public static DatasetAssert assertThat(@Nonnull final Dataset<Row> rowDataset) {
    return new DatasetAssert(rowDataset);
  }

  @SuppressWarnings("unused")
  public static void assertMatches(
      @Nonnull final String expectedRegex, @Nonnull final String actualString) {
    if (!Pattern.matches(expectedRegex, actualString)) {
      fail(
          String.format("'%s' does not match expected regex: `%s`", actualString, expectedRegex),
          actualString,
          expectedRegex);
    }
  }

  public static void assertDatasetAgainstTsv(
      @Nonnull final SparkSession spark,
      @Nonnull final String expectedCsvPath,
      @Nonnull final Dataset<Row> actualDataset,
      final boolean header) {
    final URL url = getResourceAsUrl(expectedCsvPath);
    final String decodedUrl = URLDecoder.decode(url.toString(), StandardCharsets.UTF_8);
    final DataFrameReader reader =
        spark.read().schema(actualDataset.schema()).option("delimiter", "\t");
    if (header) {
      reader.option("header", true);
    }
    final Dataset<Row> expectedDataset = reader.csv(decodedUrl);
    new DatasetAssert(actualDataset).hasRowsUnordered(expectedDataset);
  }

  public static <T> T fail(final String message, final Object expected, final Object actual) {
    throw new AssertionFailedError(message, expected, actual);
  }
}
