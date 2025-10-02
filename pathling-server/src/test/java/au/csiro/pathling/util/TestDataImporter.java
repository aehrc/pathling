package au.csiro.pathling.util;

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

import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/**
 * Converts the test fhir data in `src/test/resources/test-data/fhir` to their parquet version in
 * `src/test/resources/test-data/parquet`.
 */
@SpringBootApplication
@ComponentScan(basePackages = "au.csiro.pathling")
@Import(TestDataSetup.class)
@Profile("cli")
@Slf4j
public class TestDataImporter implements CommandLineRunner {

  @Nonnull
  protected final SparkSession spark;
  private final TestDataSetup testDataSetup;

  @Autowired
  public TestDataImporter(@Nonnull final SparkSession spark, TestDataSetup testDataSetup) {
    this.spark = spark;
    this.testDataSetup = testDataSetup;
  }

  public static void main(final String[] args) {
    ConfigurableApplicationContext ctx = new SpringApplicationBuilder(TestDataImporter.class)
        .properties("spring.main.allow-bean-definition-overriding=true")
        .run(args);
    ctx.close();
    System.exit(0);
  }

  @Override
  public void run(final String... args) {
    final String sourcePath = args[0];
    final boolean skip = Boolean.parseBoolean(args[1].split("=")[1]);
    if (skip) {
      log.info("Skipping test data setup.");
      return;
    }
    log.info("Setting up test data at: {}", sourcePath);
    testDataSetup.downloadFromSmartHealthBlocking();
    testDataSetup.setupTestData(Path.of(sourcePath));
  }

}

