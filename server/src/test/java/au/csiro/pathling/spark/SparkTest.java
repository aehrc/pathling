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
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
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

  // Hadoop properties are logged as they are applied. Any property whose name suggests a credential
  // has its value masked, regardless of case and of where in the name the word appears.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "fs.s3a.secret.key",
        "fs.s3a.access.key",
        "fs.s3a.session.token",
        "fs.s3a.bucket.my-bucket.secret.key",
        "fs.s3a.encryption.key",
        "fs.https.proxy.password",
        "fs.s3a.Custom.SECRET"
      })
  void loggableValueMasksSensitiveProperties(final String property) {
    assertThat(Spark.loggableValue(property, "sensitive-value")).isEqualTo(Spark.MASKED_VALUE);
  }

  // Properties that carry no credential are logged as they are, so that the log remains useful for
  // diagnosing storage configuration.
  @ParameterizedTest
  @ValueSource(strings = {"fs.s3a.endpoint", "fs.s3a.path.style.access", "fs.http.impl"})
  void loggableValuePassesThroughOtherProperties(final String property) {
    assertThat(Spark.loggableValue(property, "plain-value")).isEqualTo("plain-value");
  }

  // Building the session at DEBUG must log the name of every Hadoop property it applies, but never
  // the value of the S3 secret key.
  @Test
  void buildDoesNotLogS3SecretKey() {
    final Map<String, Object> properties =
        Map.of(
            "fs.s3a.secret.key", "top-secret-value",
            "fs.s3a.pathling.test.flag", "true");
    final StandardEnvironment environment = new StandardEnvironment();
    environment.getPropertySources().addFirst(new MapPropertySource("profile", properties));
    // The session is shared with the rest of the test context, so the Hadoop properties applied
    // here are restored afterwards.
    final Configuration hadoopConfiguration = sparkSession.sparkContext().hadoopConfiguration();
    final Map<String, Optional<String>> originalValues =
        properties.keySet().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(), key -> Optional.ofNullable(hadoopConfiguration.get(key))));
    final Logger logger = (Logger) LoggerFactory.getLogger(Spark.class);
    final Level originalLevel = logger.getLevel();
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.DEBUG);

    try {
      Spark.build(serverConfiguration, environment, Optional.empty(), List.of());
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(originalLevel);
      originalValues.forEach(
          (key, value) ->
              value.ifPresentOrElse(
                  v -> hadoopConfiguration.set(key, v), () -> hadoopConfiguration.unset(key)));
    }

    final List<String> messages =
        appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    assertThat(messages)
        .anyMatch(message -> message.contains("fs.s3a.secret.key = " + Spark.MASKED_VALUE))
        .anyMatch(message -> message.contains("fs.s3a.pathling.test.flag = true"))
        .noneMatch(message -> message.contains("top-secret-value"));
  }
}
