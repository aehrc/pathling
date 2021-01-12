/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.FhirContextFactory;
import au.csiro.pathling.test.helpers.SparkHelpers;
import java.io.File;
import java.io.FileFilter;
import java.util.Objects;
import jodd.io.filter.WildcardFileFilter;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;

/**
 * Converts the test fhir data in `src/test/resources/test-data/fhir` to their parquet version in
 * `src/test/resources/test-data/parquet`.
 *
 * @author Piotr Szul
 */
public class TestDataImporter {

  protected SparkSession spark;
  private final FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();

  public static void main(final String[] args) {
    new TestDataImporter().run(args);
  }

  public void setUp() {
    spark = SparkHelpers.getSparkSession();
  }

  private void run(final String[] args) {
    setUp();
    final String sourcePath = args[0];
    final String targetPath = args[1];

    final File srcNdJsonDir = new File(sourcePath);
    final FileFilter fileFilter = new WildcardFileFilter("*.ndjson");
    final File[] srcNdJsonFiles = srcNdJsonDir.listFiles(fileFilter);

    for (final File srcFile : Objects.requireNonNull(srcNdJsonFiles)) {
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
      resourcesDataset.write().mode(SaveMode.Overwrite).parquet(outputParquet);
    }

    tearDown();
  }

  private void tearDown() {
    spark.stop();
  }

}
