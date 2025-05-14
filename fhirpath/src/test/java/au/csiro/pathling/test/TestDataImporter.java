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

package au.csiro.pathling.test;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.UnitTestDependencies;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.utilities.Streams;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.FileFilter;
import java.util.Iterator;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Converts the test fhir data in `src/test/resources/test-data/fhir` to their parquet version in
 * `src/test/resources/test-data/parquet`.
 *
 * @author Piotr Szul
 */
@SpringBootApplication
@ComponentScan(basePackageClasses = UnitTestDependencies.class)
@Slf4j
public class TestDataImporter implements CommandLineRunner {

  @Nonnull
  protected final SparkSession spark;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Autowired
  public TestDataImporter(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders) {
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
  }

  public static void main(final String[] args) {
    SpringApplication app = new SpringApplication(TestDataImporter.class);
    app.setWebApplicationType(WebApplicationType.NONE); // Disable web server
    app.setAdditionalProfiles("unit-test");
    app.run(args);
  }


  static class EncodePartition implements MapPartitionsFunction<String, IBaseResource> {

    final FhirVersionEnum fhirVersion;

    EncodePartition(final FhirVersionEnum fhirVersion) {
      this.fhirVersion = fhirVersion;
    }

    @Override
    public Iterator<IBaseResource> call(Iterator<String> it) throws Exception {
      final IParser parser = FhirEncoders.contextFor(fhirVersion).newJsonParser();
      return Streams.streamOf(it)
          .map(parser::parseResource).iterator();

    }
  }

  @Override
  public void run(final String... args) {
    final String sourcePath = args[0];
    final String targetPath = args[1];

    System.out.println("Source path: " + sourcePath);
    System.out.println("Target path: " + targetPath);

    final File srcNdJsonDir = new File(sourcePath);
    final FileFilter fileFilter = WildcardFileFilter.builder().setWildcards("*.ndjson").get();
    final File[] srcNdJsonFiles = srcNdJsonDir.listFiles(fileFilter);

    Stream.of(requireNonNull(srcNdJsonFiles))
        .forEach(file -> {

          System.out.println("Importing file: " + file.getAbsolutePath());

          final String resourceName = FilenameUtils.getBaseName(file.getName());
          final ResourceType subjectResource = ResourceType
              .valueOf(resourceName.toUpperCase());

          final EncodePartition partitionEncoder = new EncodePartition(
              fhirEncoders.getFhirVersion());
          spark.read().
              text(file.getAbsolutePath())
              .as(Encoders.STRING())
              .mapPartitions(partitionEncoder, fhirEncoders.of(subjectResource.toCode()))
              .write()
              .mode(SaveMode.Overwrite)
              .format("delta")
              .save(targetPath + "/" + subjectResource.toCode() + ".parquet");
        });
  }

}
