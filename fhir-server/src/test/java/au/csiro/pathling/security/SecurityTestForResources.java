/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import java.io.File;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;


/**
 * @see <a href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@ActiveProfiles({"core", "unit-test"})
public abstract class SecurityTestForResources extends SecurityTest {

  @TempDir
  @SuppressWarnings({"unused", "WeakerAccess"})
  static File testRootDir;

  @DynamicPropertySource
  @SuppressWarnings("unused")
  static void registerProperties(final DynamicPropertyRegistry registry) {
    // TODO: This is a bit messy - maybe we should abstract the physical warehouse out, so that it
    //  could be mocked
    final File warehouseDir = new File(testRootDir, "default");
    assertTrue(warehouseDir.mkdir());
    registry.add("pathling.storage.warehouseUrl",
        () -> testRootDir.toURI().toString().replaceFirst("/$", ""));
  }

  @Autowired
  private ResourceReader resourceReader;

  @Autowired
  private ResourceWriter resourceWriter;

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirEncoders fhirEncoders;


  public void assertWriteSuccess() {
    resourceWriter.write(ResourceType.ACCOUNT,
        new ResourceDatasetBuilder(spark).withIdColumn().build());
  }

  public void assertAppendSuccess() {
    resourceWriter.append(ResourceType.ACCOUNT,
        new ResourceDatasetBuilder(spark).withIdColumn().build());
  }

  public void assertUpdateSuccess() {
    resourceWriter.update(ResourceType.ACCOUNT, resourceReader,
        spark.emptyDataset(fhirEncoders.of(ResourceType.ACCOUNT.toCode())).toDF());
  }

  public void assertReadSuccess() {
    resourceReader.read(ResourceType.ACCOUNT);
  }
}
