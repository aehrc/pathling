package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import java.io.File;
import java.lang.reflect.UndeclaredThrowableException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;


/**
 * See: https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html See
 * (ContextInitializer) https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property
 */
@TestPropertySource(
    properties = {
        "pathling.auth.enabled=true",
        "pathling.caching.enabled=false"
    })
@SpringBootTest
@Tag("UnitTest")
@ActiveProfiles({"core"})
public class SecurityEnabledTestForResources {

  @TempDir
  static File testRootDir;

  @DynamicPropertySource
  static void registerProperties(DynamicPropertyRegistry registry) {
    // TODO: This is a bit messy - maybe we should abstract the phycisal
    // warehouse out, so that it couild me mocked
    final File warehouseDir = new File(testRootDir, "default");
    warehouseDir.mkdir();
    registry.add("pathling.storage.warehouseUrl",
        () -> testRootDir.toURI());
  }

  @Autowired
  @Nonnull
  private ResourceReader resourceReader;

  @Autowired
  @Nonnull
  private ResourceWriter resourceWriter;

  @Autowired
  @Nonnull
  private SparkSession sparkSession;


  private static void assertThrowsAccessDenied(@Nonnull final String expectedMissingAuthority,
      @Nonnull final Executable executable) {
    final AccessDeniedError ex = assertThrows(AccessDeniedError.class,
        executable);
    assertEquals(expectedMissingAuthority, ex.getMissingAuthority());
  }

  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfResourceWriteWithoutAuthority() {
    assertThrowsAccessDenied("user/Account.write",
        () -> resourceWriter.write(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT,
            sparkSession.emptyDataFrame()));
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/Account.write"})
  public void testPassIfResourceWriteWithSpecficAuthority() {
    resourceWriter.write(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT,
        new ResourceDatasetBuilder(sparkSession).withIdColumn().build());
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/*.write"})
  public void testPassIfResourceWriteWithWildcardAuthority() {
    final UndeclaredThrowableException ex = assertThrows(UndeclaredThrowableException.class, () ->
        resourceWriter.write(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT,
            sparkSession.emptyDataFrame()));
  }

  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfResourceReadWithoutAuthority() {
    // TODO: Can we use either org.hl7.fhir.r4.model.ResourceType here?
    assertThrowsAccessDenied("user/Account.read",
        () -> resourceReader.read(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT));
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/Account.read"})
  public void testPassIfResourceReadWithSpecficAuthority() {
    assertThrows(ResourceNotFoundError.class,
        () -> resourceReader.read(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT),
        "Requested resource type not available within selected database: Account");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/*.read"})
  public void testPassIfResourceReadWithWildcardAuthority() {
    assertThrows(ResourceNotFoundError.class,
        () -> resourceReader.read(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT),
        "Requested resource type not available within selected database: Account");
  }
}
