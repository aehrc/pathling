/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import au.csiro.pathling.io.ResourceReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.mockito.stubbing.OngoingStubbing;

/**
 * @author John Grimes
 */
public abstract class TestHelpers {

  public static final String LOINC_URL = "http://loinc.org";
  public static final String SNOMED_URL = "http://snomed.info/sct";

  @Nonnull
  private static ClassLoader getClassLoader() {
    return checkNotNull(Thread.currentThread().getContextClassLoader());
  }

  @Nonnull
  public static URL getResourceAsUrl(@Nonnull final String name) {
    final ClassLoader loader = getClassLoader();
    return checkNotNull(loader.getResource(name));
  }

  @Nonnull
  public static InputStream getResourceAsStream(@Nonnull final String name) {
    final ClassLoader loader = getClassLoader();
    return checkNotNull(loader.getResourceAsStream(name));
  }

  @Nonnull
  public static String getResourceAsString(@Nonnull final String name) {
    try {
      final InputStream expectedStream = getResourceAsStream(name);
      final StringWriter writer = new StringWriter();
      IOUtils.copy(expectedStream, writer, UTF_8);
      return writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Problem retrieving test resource", e);
    }
  }

  @Nonnull
  public static OngoingStubbing<Set<ResourceType>> mockAvailableResourceTypes(
      @Nonnull final ResourceReader mockReader, @Nonnull final ResourceType... types) {
    return when(mockReader.getAvailableResourceTypes())
        .thenReturn(new HashSet<>(Arrays.asList(types)));
  }

  public static void mockResourceReader(@Nonnull final ResourceReader mockReader,
      @Nonnull final SparkSession spark, @Nonnull final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      try {
        final File parquetFile =
            new File("src/test/resources/test-data/parquet/" + resourceType.toCode() + ".parquet");
        @Nullable final URL parquetUrl;
        parquetUrl = parquetFile.getAbsoluteFile().toURI().toURL();
        assertNotNull(parquetUrl);
        @Nullable final Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
        assertNotNull(dataset);

        when(mockReader.read(resourceType)).thenReturn(dataset);
        when(mockReader.getAvailableResourceTypes())
            .thenReturn(new HashSet<>(Arrays.asList(resourceTypes)));
      } catch (final MalformedURLException e) {
        throw new RuntimeException("Problem mocking resource reader", e);
      }
    }
  }

}
