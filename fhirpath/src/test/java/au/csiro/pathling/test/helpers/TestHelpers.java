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

package au.csiro.pathling.test.helpers;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.io.source.DataSource;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public abstract class TestHelpers {

  public static final String LOINC_URL = "http://loinc.org";
  public static final String SNOMED_URL = "http://snomed.info/sct";
  public static final String WAREHOUSE_URL = System.getProperty("pathling.storage.warehouseUrl",
      "src/test/resources/test-data");
  public static final String DATABASE_NAME = System.getProperty("pathling.storage.databaseName",
      "parquet");
  public static final String UCUM_URL = "http://unitsofmeasure.org";

  public static void mockResource(@Nonnull final DataSource dataSource,
      @Nonnull final SparkSession spark, @Nonnull final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      final Dataset<Row> dataset = getDatasetForResourceType(spark, resourceType);
      when(dataSource.read(resourceType)).thenReturn(dataset);
    }
  }

  public static void mockCachedResource(@Nonnull final DataSource dataSource,
      @Nonnull final SparkSession spark, @Nonnull final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      final Dataset<Row> dataset = getDatasetForResourceType(spark, resourceType).cache();
      when(dataSource.read(resourceType)).thenReturn(dataset);
    }
  }

  public static void mockResource(@Nonnull final DataSource dataSource,
      @Nonnull final SparkSession spark, final int numPartitions,
      @Nonnull final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      Dataset<Row> dataset = getDatasetForResourceType(spark, resourceType);
      dataset = dataset.repartition(numPartitions);
      when(dataSource.read(resourceType)).thenReturn(dataset);
    }
  }

  public static void mockResource(@Nonnull final DataSource dataSource,
      @Nonnull final SparkSession spark, @Nonnull final ResourceType resourceType,
      @Nonnull final String parquetPath) {
    final Dataset<Row> dataset = getDatasetFromParquetFile(spark, parquetPath);
    when(dataSource.read(resourceType)).thenReturn(dataset);
  }

  public static void mockAllEmptyResources(@Nonnull final DataSource dataSource,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders) {
    final Set<ResourceType> resourceTypes = ResourceCollection.supportedResourceTypes();
    for (final ResourceType resourceType : resourceTypes) {
      final Dataset<Row> dataset = QueryHelpers.createEmptyDataset(spark, fhirEncoders,
          resourceType);
      when(dataSource.read(resourceType)).thenReturn(dataset);
    }
  }

  @Nonnull
  public static Dataset<Row> getDatasetForResourceType(@Nonnull final SparkSession spark,
      @Nonnull final ResourceType resourceType) {
    return getDatasetFromParquetFile(spark,
        getParquetUrlForResourceType(resourceType));
  }

  @Nonnull
  public static String getDatabaseUrl() {
    return String.join("/", WAREHOUSE_URL, DATABASE_NAME);
  }

  @Nonnull
  public static String getParquetUrlForResourceType(final @Nonnull ResourceType resourceType) {
    return getDatabaseUrl() + "/" + resourceType.toCode() + ".parquet";
  }

  @Nonnull
  public static Dataset<Row> getDatasetFromParquetFile(@Nonnull final SparkSession spark,
      @Nonnull final String parquetUrl) {
    final String decodedUrl = URLDecoder.decode(parquetUrl, StandardCharsets.UTF_8);
    @Nullable final Dataset<Row> dataset = DeltaTable.forPath(spark, decodedUrl).toDF();
    assertNotNull(dataset);
    return dataset;
  }

}
