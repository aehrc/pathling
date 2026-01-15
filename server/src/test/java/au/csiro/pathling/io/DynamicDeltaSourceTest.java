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

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  private DynamicDeltaSource dynamicDeltaSource;

  @BeforeEach
  void setUp() {
    // Use an empty directory as the database path to simulate no data.
    final String databasePath =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath().toString();
    final QueryableDataSource baseSource = pathlingContext.read().delta(databasePath);
    dynamicDeltaSource =
        new DynamicDeltaSource(baseSource, sparkSession, databasePath, fhirEncoders);
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
}
