/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.helpers.TestHelpers;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONException;
import org.junit.jupiter.api.function.Executable;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

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

  public static void assertTrue(final boolean condition) {
    org.junit.jupiter.api.Assertions.assertTrue(condition);
  }

  public static void assertEquals(@Nonnull final Object expected, @Nonnull final Object actual) {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

  public static <T extends java.lang.Throwable> T assertThrows(
      @Nonnull final Class<T> expectedType, @Nonnull final Executable executable,
      @Nonnull final String message) {
    return org.junit.jupiter.api.Assertions.assertThrows(expectedType, executable, message);
  }

  public static void assertJson(@Nonnull final String expectedPath,
      @Nonnull final String actualJson) {
    assertJson(expectedPath, actualJson, JSONCompareMode.NON_EXTENSIBLE);
  }

  public static void assertJson(@Nonnull final String expectedPath,
      @Nonnull final String actualJson, @Nonnull final JSONCompareMode compareMode) {
    final String expectedJson;
    try {
      expectedJson = TestHelpers.getResourceAsString(expectedPath);
      JSONAssert.assertEquals(expectedJson, actualJson, compareMode);
    } catch (final JSONException e) {
      throw new RuntimeException("Problem checking JSON against test resource", e);
    }
  }
}
