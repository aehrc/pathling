/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.FhirServer;
import au.csiro.pathling.io.Database;
import io.delta.tables.DeltaTable;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
  public static Dataset<Row> getDatasetForResourceType(@Nonnull final SparkSession spark,
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
      parquetUrl = new URL("file://" + parquetFile.getAbsoluteFile().toPath());
    } catch (final MalformedURLException e) {
      throw new RuntimeException("Problem getting dataset", e);
    }
    assertNotNull(parquetUrl);
    final String decodedUrl = URLDecoder.decode(parquetUrl.toString(), StandardCharsets.UTF_8);
    @Nullable final Dataset<Row> dataset = DeltaTable.forPath(spark, decodedUrl).toDF();
    assertNotNull(dataset);
    return dataset;
  }

}
