package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.ResourceProviderFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.search.CachingSearchProvider;
import au.csiro.pathling.search.SearchProvider;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.update.ImportProvider;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;


/**
 * See: https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html See
 * (ContextInitializer) https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property
 */
@TestPropertySource(locations = {"classpath:/configuration/authorisation.properties"},
    properties = {
        "pathling.caching.enabled=false"
    })
@SpringBootTest
@Tag("UnitTest")
@ActiveProfiles({"core", "server"})
public class SecurityEnabledTestForOperations {

  @Autowired
  @Nonnull
  private ImportProvider importProvider;

  @Autowired
  @Nonnull
  private ResourceProviderFactory resourceProviderFactory;

  @MockBean
  @Nonnull
  private ResourceReader resourceReader;

  @MockBean
  @Nonnull
  private ResourceWriter resourceWriter;

  @Autowired
  private SparkSession sparkSession;

  @BeforeEach
  public void setUp() {
    when(resourceReader.read(any()))
        .thenReturn(new ResourceDatasetBuilder(sparkSession).withIdColumn().build());
  }

  private static void assertThrowsAccessDenied(@Nonnull final String expectedMissingAuthority,
      @Nonnull final Executable executable) {
    final AccessDeniedError ex = assertThrows(AccessDeniedError.class,
        executable);
    assertEquals(expectedMissingAuthority, ex.getMissingAuthority());
  }

  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfImportWithoutAuthority() {
    assertThrowsAccessDenied("operation:import",
        () -> importProvider.importOperation(new Parameters()));
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
    final AggregateProvider aggregateProvider = (AggregateProvider) resourceProviderFactory
        .createAggregateResourceProvider(ResourceType.Patient);
    assertThrowsAccessDenied("operation:aggregate",
        () -> aggregateProvider.aggregate(null, null, null));
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:aggregate"})
  public void testPassIfAggregateWithAuthority() {
    final AggregateProvider aggregateProvider = (AggregateProvider) resourceProviderFactory
        .createAggregateResourceProvider(ResourceType.Patient);
    assertThrows(InvalidUserInputError.class,
        () -> aggregateProvider.aggregate(null, null, null)
    );
  }

  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfSearchWithoutAuthority() {
    // TODO: this become somewhat messy because of the current cachig implementation
    // Perhps we could use AOP here as well to simplify?
    final SearchProvider searchProvider = (SearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, false);
    assertThrowsAccessDenied("operation:search",
        () -> searchProvider.search());
    assertThrowsAccessDenied("operation:search",
        () -> searchProvider.search(null));
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:search"})
  public void testPassIfSearchWithAuthority() {
    final SearchProvider searchProvider = (SearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, false);
    searchProvider.search();
    searchProvider.search(null);
  }

  @Test
  @WithMockUser(username = "admin", authorities = {})
  public void testForbidenIfCachingSearchWithoutAuthority() {
    // TODO: this become somewhat messy because of the current cachig implementation
    // Perhps we could use AOP here as well to simplify?
    final CachingSearchProvider cachingSearchProvider = (CachingSearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, true);
    assertThrowsAccessDenied("operation:search",
        () -> cachingSearchProvider.search());
    assertThrowsAccessDenied("operation:search",
        () -> cachingSearchProvider.search(null));
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:search"})
  public void testPassIfCachingSearchWithAuthority() {
    final CachingSearchProvider cachingSearchProvider = (CachingSearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, true);
    cachingSearchProvider.search();
    cachingSearchProvider.search(null);
  }
}
