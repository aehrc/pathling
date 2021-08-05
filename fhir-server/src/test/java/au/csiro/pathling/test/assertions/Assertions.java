/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.fail;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.helpers.TestHelpers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.net.URL;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

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

  public static void assertJson(@Nonnull final String expectedPath,
      @Nonnull final String actualJson) {
    assertJson(expectedPath, actualJson, JSONCompareMode.NON_EXTENSIBLE);
  }

  public static void assertMatches(@Nonnull final String expectedRegex,
      @Nonnull final String actualString) {
    if (!Pattern.matches(expectedRegex, actualString)) {
      fail(String.format("'%s' does not match expected regex: `%s`", actualString, expectedRegex));
    }
  }


  public static void assertJson(@Nonnull final String expectedPath,
      @Nonnull final String actualJson, @Nonnull final JSONCompareMode compareMode) {
    final String expectedJson;
    try {
      expectedJson = TestHelpers.getResourceAsString(expectedPath);
      try {
        JSONAssert.assertEquals(expectedJson, actualJson, compareMode);
      } catch (final AssertionError e) {
        final Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        final JsonElement jsonElement = JsonParser.parseString(actualJson);
        final String prettyJsonString = gson.toJson(jsonElement);
        log.info("Expected response: {}", expectedJson);
        log.info("Actual response: {}", prettyJsonString);
        throw e;
      }
    } catch (final JSONException e) {
      throw new RuntimeException("Problem checking JSON against test resource", e);
    }
  }

  public static void assertDatasetAgainstCsv(@Nonnull final SparkSession spark,
      @Nonnull final String expectedCsvPath, @Nonnull final Dataset<Row> actualDataset) {
    final URL url = TestHelpers.getResourceAsUrl(expectedCsvPath);
    final Dataset<Row> expectedDataset = spark.read()
        .schema(actualDataset.schema())
        .csv(url.toString());
    new DatasetAssert(actualDataset)
        .hasRowsUnordered(expectedDataset);
  }

}
