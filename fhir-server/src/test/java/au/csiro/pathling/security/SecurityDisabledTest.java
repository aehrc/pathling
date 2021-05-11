package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.update.ImportProvider;
import ca.uhn.fhir.rest.server.exceptions.ForbiddenOperationException;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;


/**
 * See: https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html
 */
@TestPropertySource(properties = {
    "pathling.auth.enabled=false",
    "pathling.caching.enabled=false"
})
//@TestPropertySource(locations = {"classpath:/configuration/authorisation.properties"})
@SpringBootTest
@Tag("UnitTest")
@ActiveProfiles({"core", "server"})
public class SecurityDisabledTest {


  @DynamicPropertySource
  static void registerPgProperties(DynamicPropertyRegistry registry) {
    registry.add("pathling.storage.warehouseUrl",
        () -> "file:///Users/szu004/dev/pathling/fhir-server/src/test/resources/test-data");
    registry.add("pathling.storage.databaseName", () -> "parquet");

  }

  @Autowired
  @Nonnull
  private ImportProvider importProvider;

  @Autowired
  @Nonnull
  private ResourceReader resourceReader;

  @Test
  public void testPassIfImportWithoutAutentication() {
    assertThrows(InvalidUserInputError.class,
        () -> importProvider.importOperation(new Parameters()),
        "Must provide at least one source parameter");
  }

  @Test
  public void testPassResourceReadWithoutAuthentication() {
    assertThrows(ResourceNotFoundError.class,
        () -> resourceReader.read(ResourceType.ACCOUNT),
        "Requested resource type not available within selected database: Account");
  }

}
