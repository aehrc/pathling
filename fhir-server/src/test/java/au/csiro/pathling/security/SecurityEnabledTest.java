package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.fhir.ResourceProviderFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.update.ImportProvider;
import ca.uhn.fhir.context.FhirContext;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.ResourceType;
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
@TestPropertySource(locations = {"classpath:/configuration/authorisation.properties"},
    properties = {
        "pathling.caching.enabled=false"
    })
@SpringBootTest
@Tag("UnitTest")
@ActiveProfiles({"core", "server"})
public class SecurityEnabledTest {

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

  @Autowired
  @Nonnull
  private ResourceProviderFactory resourceProviderFactory;

  @Autowired
  FhirContext fhirContext;

  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfImportWithoutAuthority() {
    assertThrows(AccessDeniedError.class,
        () -> importProvider.importOperation(new Parameters()),
        "Requires `operation:import`");
  }
  @Test
  @WithMockUser(username = "admin", authorities = {"operation:import"})
  public void testPassIfImportWithAuthority() {
    assertThrows(InvalidUserInputError.class,
        () -> importProvider.importOperation(new Parameters()),
        "Must provide at least one source parameter");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfAggregateWithoutAuthority() {
    AggregateProvider aggregateProvider = (AggregateProvider) resourceProviderFactory
        .createAggregateResourceProvider(ResourceType.Patient);
    // TODO: Why the assertion exception messages do not work? (e.g. do not fail)
    assertThrows(AccessDeniedError.class,
        () -> aggregateProvider.aggregate(null, null, null),
        "Requires `operation:import`");
  }
  
  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfResourceReadWithoutAuthority() {
    // TODO: Can we use either org.hl7.fhir.r4.model.ResourceType here?
    assertThrows(AccessDeniedError.class,
        () -> resourceReader.read(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT),
        "Requires `user/account.read`");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/Account.read"})
  public void testPassIfResourceReadWithAuthority() {
    assertThrows(ResourceNotFoundError.class,
        () -> resourceReader.read(org.hl7.fhir.r4.model.Enumerations.ResourceType.ACCOUNT),
        "Requested resource type not available within selected database: Account");
  }


}
