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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link DynamicDeltaSource}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class DynamicDeltaSourceTest {

  private static final String DATABASE_PATH =
      Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath().toString();

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  private DynamicDeltaSource dynamicDeltaSource;

  @BeforeEach
  void setUp() {
    dynamicDeltaSource = newDynamicDeltaSource(true);
  }

  // Verifies that reading a resource type that doesn't exist in the database returns an empty
  // dataset instead of throwing an exception.
  @Test
  void readReturnsEmptyDatasetForMissingResourceType() {
    // ImmunizationEvaluation is a valid FHIR resource type that doesn't have a table in our test
    // data.
    final Dataset<Row> result = dynamicDeltaSource.read("ImmunizationEvaluation");

    assertThat(result).isNotNull();
    assertThat(result.count()).isZero();
    // Verify the schema has the expected structure for an ImmunizationEvaluation resource.
    assertThat(result.schema().fieldNames()).contains("id", "status");
  }

  // Verifies that when StorageConfiguration.cacheDatasets is true, datasets returned by read()
  // are marked for caching. Guards against accidental changes to the caching contract introduced
  // by cacheIfEnabled().
  @Test
  void readCachesDatasetWhenCachingEnabled() {
    final DynamicDeltaSource source = newDynamicDeltaSource(true);

    final Dataset<Row> result = source.read("Patient");

    try {
      // Dataset.cache() applies MEMORY_AND_DISK; assert on the exact level rather than just
      // "anything but NONE" so accidental changes to the persistence level are caught.
      assertThat(result.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());
    } finally {
      result.unpersist();
    }
  }

  // Verifies that when StorageConfiguration.cacheDatasets is false, datasets returned by read()
  // are not cached. Pairs with readCachesDatasetWhenCachingEnabled() to lock in cacheIfEnabled()
  // behaviour for both configuration values.
  @Test
  void readDoesNotCacheDatasetWhenCachingDisabled() {
    final DynamicDeltaSource source = newDynamicDeltaSource(false);

    final Dataset<Row> result = source.read("Patient");

    assertThat(result.storageLevel()).isEqualTo(StorageLevel.NONE());
  }

  @Nonnull
  private DynamicDeltaSource newDynamicDeltaSource(final boolean cacheDatasets) {
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setCacheDatasets(cacheDatasets);
    final QueryableDataSource baseSource = pathlingContext.read().delta(DATABASE_PATH);
    return new DynamicDeltaSource(
        baseSource, sparkSession, DATABASE_PATH, fhirEncoders, storageConfiguration);
  }
}
