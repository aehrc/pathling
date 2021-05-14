package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import java.io.File;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;


/**
 * See: https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html See
 * (ContextInitializer) https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property
 */
@SpringBootTest
@Tag("UnitTest")
@ActiveProfiles({"core"})
public abstract class SecurityTestForResources extends SecurityTest {

  @TempDir
  static File testRootDir;

  @DynamicPropertySource
  static void registerProperties(DynamicPropertyRegistry registry) {
    // TODO: This is a bit messy - maybe we should abstract the phycisal
    // warehouse out, so that it couild me mocked
    final File warehouseDir = new File(testRootDir, "default");
    assertTrue(warehouseDir.mkdir());
    registry.add("pathling.storage.warehouseUrl",
        () -> testRootDir.toURI());
  }

  @Autowired
  private ResourceReader resourceReader;

  @Autowired
  private ResourceWriter resourceWriter;

  @Autowired
  private SparkSession sparkSession;


  public void assertWriteSuccess() {
    resourceWriter.write(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT,
        new ResourceDatasetBuilder(sparkSession).withIdColumn().build());
  }

  public void assertReadSuccess() {

    try {
      resourceReader.read(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT);
    } catch (ResourceNotFoundError ex) {
      // expected
    }
  }
}
