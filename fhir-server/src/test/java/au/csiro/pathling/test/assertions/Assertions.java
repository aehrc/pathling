/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static au.csiro.pathling.test.TestResources.getResourceAsUrl;
import static org.junit.jupiter.api.Assertions.fail;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
  public static UntypedResourcePathAssertion assertThat(
      @Nonnull final UntypedResourcePath fhirPath) {
    return new UntypedResourcePathAssertion(fhirPath);
  }

  @Nonnull
  public static DatasetAssert assertThat(@Nonnull final Dataset<Row> rowDataset) {
    return new DatasetAssert(rowDataset);
  }

  public static void assertMatches(@Nonnull final String expectedRegex,
      @Nonnull final String actualString) {
    if (!Pattern.matches(expectedRegex, actualString)) {
      fail(String.format("'%s' does not match expected regex: `%s`", actualString, expectedRegex));
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

}
