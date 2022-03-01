/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import static org.apache.spark.sql.functions.asc;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.FhirContextFactory;
import java.io.File;
import java.io.FileFilter;
import java.util.Objects;
import javax.annotation.Nonnull;
import jodd.io.filter.WildcardFileFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;

/**
 * Converts the test fhir data in `src/test/resources/test-data/fhir` to their parquet version in
 * `src/test/resources/test-data/parquet`.
 *
 * @author Piotr Szul
 */
@SpringBootApplication
@ComponentScan(basePackages = "au.csiro.pathling")
@Profile("cli")
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
    SpringApplication.run(TestDataImporter.class, args);
  }

  @Override
  public void run(final String... args) {
    final String sourcePath = args[0];
    final String targetPath = args[1];

    final File srcNdJsonDir = new File(sourcePath);
    final FileFilter fileFilter = new WildcardFileFilter("*.ndjson");
    final File[] srcNdJsonFiles = srcNdJsonDir.listFiles(fileFilter);

    log.info("Ensuring directory exists: " + targetPath);
    //noinspection ResultOfMethodCallIgnored
    new File(targetPath).mkdirs();

    for (final File srcFile : Objects.requireNonNull(srcNdJsonFiles)) {
      log.info("Loading source NDJSON file: " + srcFile);
      final String resourceName = FilenameUtils.getBaseName(srcFile.getName());
      final Enumerations.ResourceType subjectResource = Enumerations.ResourceType
          .valueOf(resourceName.toUpperCase());
      final Dataset<String> jsonStrings = spark.read().textFile(srcFile.getPath());
      final ExpressionEncoder<IBaseResource> fhirEncoder = fhirEncoders
          .of(subjectResource.toCode());
      final FhirContextFactory localFhirContextFactory = new FhirContextFactory(
          fhirEncoders.getFhirVersion().newContext());
      final Dataset<IBaseResource> resourcesDataset = jsonStrings
          .map((MapFunction<String, IBaseResource>) json -> localFhirContextFactory
              .build().newJsonParser().parseResource(json), fhirEncoder);
      final String outputParquet =
          targetPath + "/" + subjectResource.toCode() + ".parquet";

      log.info("Writing: " + outputParquet);
      resourcesDataset.orderBy(asc("id"))
          .write()
          .mode(SaveMode.Overwrite)
          .format("delta")
          .save(outputParquet);
    }
  }

}
