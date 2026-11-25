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

package au.csiro.pathling.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

/**
 * Tests for {@link Spark} verifying that the SparkSession is properly configured within the Spring
 * Boot test context.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class SparkTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private ServerConfiguration serverConfiguration;

  @Test
  void sparkSessionIsCreatedAndConfigured() {
    // The Spring context should provide a configured SparkSession.
    assertNotNull(sparkSession);
    assertNotNull(sparkSession.sparkContext());
  }

  @Test
  void sparkSessionHasDeltaLakeExtension() {
    // The SparkSession should be configured with Delta Lake extensions.
    final String extensions = sparkSession.conf().get("spark.sql.extensions");
    assertNotNull(extensions);
  }

  // The Parquet on FHIR layout requires microsecond timestamps, which Spark takes from the session.
  // The server's session must carry that setting even where the Spark configuration in a profile
  // asks for something else, or its writes would store timestamps as int96.
  @Test
  void sessionStoresTimestampsAsMicrosRegardlessOfConfiguration() {
    final StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "profile", Map.of("spark.sql.parquet.outputTimestampType", "INT96")));

    try {
      final SparkSession spark =
          Spark.build(serverConfiguration, environment, Optional.empty(), List.of());

      assertThat(spark.conf().get("spark.sql.parquet.outputTimestampType"))
          .isEqualTo("TIMESTAMP_MICROS");
    } finally {
      // The pass-through of Spark configuration sets system properties, which must not leak into
      // other tests.
      System.clearProperty("spark.sql.parquet.outputTimestampType");
    }
  }

  // Spark's parser replaces ${...} references in SQL text unless spark.sql.variable.substitute is
  // false. The server's session must run SQL as written even where that setting has been turned
  // back on.
  @Test
  void sessionReturnsDollarBraceTextUnchangedRegardlessOfConfiguration() {
    final StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource("profile", Map.of("spark.sql.variable.substitute", "true")));
    // Spark.build reuses the running session, so the setting is also turned on directly.
    sparkSession.conf().set("spark.sql.variable.substitute", "true");

    try {
      final SparkSession spark =
          Spark.build(serverConfiguration, environment, Optional.empty(), List.of());

      assertThat(spark.conf().get("spark.sql.variable.substitute")).isEqualTo("false");
      final Row row =
          spark
              .sql(
                  "SELECT '${spark:spark.app.name}' AS app, '${env:PATH}' AS path,"
                      + " '${system:user.dir}' AS dir")
              .first();
      assertThat(row.getString(0)).isEqualTo("${spark:spark.app.name}");
      assertThat(row.getString(1)).isEqualTo("${env:PATH}");
      assertThat(row.getString(2)).isEqualTo("${system:user.dir}");
    } finally {
      sparkSession.conf().set("spark.sql.variable.substitute", "false");
      System.clearProperty("spark.sql.variable.substitute");
    }
  }
}
