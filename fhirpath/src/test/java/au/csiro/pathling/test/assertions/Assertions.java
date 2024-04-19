/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import jakarta.annotation.Nonnull;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opentest4j.AssertionFailedError;

@Slf4j
public abstract class Assertions {

  @Nonnull
  public static FhirPathAssertion assertThat(@Nonnull final FhirPath fhirPath) {
    return new FhirPathAssertion(fhirPath);
  }

  @Nonnull
  public static ResourcePathAssertion assertThat(@Nonnull final ResourcePath fhirPath) {
    return new ResourcePathAssertion(fhirPath);
  }

  @Nonnull
  public static ElementPathAssertion assertThat(@Nonnull final ElementPath fhirPath) {
    return new ElementPathAssertion(fhirPath);
  }

  @Nonnull
  public static DatasetAssert assertThat(@Nonnull final Dataset<Row> rowDataset) {
    return new DatasetAssert(rowDataset);
  }

  @SuppressWarnings("unused")
  public static void assertMatches(@Nonnull final String expectedRegex,
      @Nonnull final String actualString) {
    if (!Pattern.matches(expectedRegex, actualString)) {
      fail(String.format("'%s' does not match expected regex: `%s`", actualString, expectedRegex),
          actualString, expectedRegex);
    }
  }

  public static void assertDatasetAgainstCsv(@Nonnull final SparkSession spark,
      @Nonnull final String expectedCsvPath, @Nonnull final Dataset<Row> actualDataset) {
    final URL url = getResourceAsUrl(expectedCsvPath);
    final String decodedUrl = URLDecoder.decode(url.toString(), StandardCharsets.UTF_8);
    final Dataset<Row> expectedDataset = spark.read()
        .schema(actualDataset.schema())
        .csv(decodedUrl);
    new DatasetAssert(actualDataset)
        .hasRowsUnordered(expectedDataset);
  }

  public static <T> T fail(String message, Object expected, Object actual) {
    throw new AssertionFailedError(message, expected, actual);
  }
}
