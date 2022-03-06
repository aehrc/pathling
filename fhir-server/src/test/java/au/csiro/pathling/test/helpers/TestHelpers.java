/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.FhirServer;
import au.csiro.pathling.io.Database;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.http.MediaType;

/**
 * @author John Grimes
 */
public abstract class TestHelpers {

  public static final String LOINC_URL = "http://loinc.org";
  public static final String SNOMED_URL = "http://snomed.info/sct";
  public static final String PARQUET_PATH = "src/test/resources/test-data/parquet";
  public static final MediaType FHIR_MEDIA_TYPE = new MediaType("application", "fhir+json");

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
    final InputStream inputStream = loader.getResourceAsStream(name);
    check(Objects.nonNull(inputStream), "Failed to load resource from : '%s'", name);
    return checkNotNull(inputStream);
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

  public static void mockResource(@Nonnull final Database database,
      @Nonnull final SparkSession spark, @Nonnull final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      final Dataset<Row> dataset = getDatasetForResourceType(spark, resourceType);
      when(database.read(resourceType)).thenReturn(dataset);
    }
  }

  public static void mockResource(@Nonnull final Database database,
      @Nonnull final SparkSession spark, final int numPartitions,
      @Nonnull final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      Dataset<Row> dataset = getDatasetForResourceType(spark, resourceType);
      dataset = dataset.repartition(numPartitions);
      when(database.read(resourceType)).thenReturn(dataset);
    }
  }

  public static void mockResource(@Nonnull final Database database,
      @Nonnull final SparkSession spark, @Nonnull final ResourceType resourceType,
      @Nonnull final String parquetPath) {
    final Dataset<Row> dataset = getDatasetFromParquetFile(spark, parquetPath);
    when(database.read(resourceType)).thenReturn(dataset);
  }

  public static void mockEmptyResource(@Nonnull final Database database,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      final Dataset<Row> dataset = QueryHelpers.createEmptyDataset(spark, fhirEncoders,
          resourceType);
      when(database.read(resourceType)).thenReturn(dataset);
    }
  }

  public static void mockAllEmptyResources(@Nonnull final Database database,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders) {
    final Set<ResourceType> resourceTypes = FhirServer.supportedResourceTypes();
    for (final ResourceType resourceType : resourceTypes) {
      final Dataset<Row> dataset = QueryHelpers.createEmptyDataset(spark, fhirEncoders,
          resourceType);
      when(database.read(resourceType)).thenReturn(dataset);
    }
  }

  @Nonnull
  private static Dataset<Row> getDatasetForResourceType(@Nonnull final SparkSession spark,
      @Nonnull final ResourceType resourceType) {
    return getDatasetFromParquetFile(spark,
        getParquetPathForResourceType(resourceType));
  }

  @Nonnull
  public static String getParquetPathForResourceType(final @Nonnull ResourceType resourceType) {
    return PARQUET_PATH + "/" + resourceType.toCode() + ".parquet";
  }

  @Nonnull
  public static Dataset<Row> getDatasetFromParquetFile(@Nonnull final SparkSession spark,
      @Nonnull final String parquetPath) {
    final File parquetFile = new File(parquetPath);
    @Nullable final URL parquetUrl;
    try {
      parquetUrl = parquetFile.getAbsoluteFile().toURI().toURL();
    } catch (final MalformedURLException e) {
      throw new RuntimeException("Problem getting dataset", e);
    }
    assertNotNull(parquetUrl);
    final String decodedUrl = URLDecoder.decode(parquetUrl.toString(), StandardCharsets.UTF_8);
    @Nullable final Dataset<Row> dataset = spark.read().parquet(decodedUrl);
    assertNotNull(dataset);
    return dataset;
  }

}
