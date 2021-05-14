package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;


/**
 * See: https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html See
 * (ContextInitializer) https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property
 */
@TestPropertySource(locations = {"classpath:/configuration/authorisation.properties"},
    properties = {
        "pathling.caching.enabled=false"
    })
public class SecurityEnabledTestForOperations extends SecurityTestForOperations {

  @Test
  @WithMockUser(username = "admin")
  public void testForbidenIfImportWithoutAuthority() {
    assertThrowsAccessDenied("operation:import",
        this::assertImportSuccess);
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:import"})
  public void testPassIfImportWithAuthority() {
    assertImportSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbidenIfAggregateWithoutAuthority() {
    assertThrowsAccessDenied("operation:aggregate",
        this::assertAggregateSuccess);
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:aggregate"})
  public void testPassIfAggregateWithAuthority() {
    assertAggregateSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbidenIfSearchWithoutAuthority() {
    assertThrowsAccessDenied("operation:search",
        this::assertSearchSuccess);
    assertThrowsAccessDenied("operation:search",
        this::assertSearchWithFilterSuccess);
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:search"})
  public void testPassIfSearchWithAuthority() {
    assertSearchSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbidenIfCachingSearchWithoutAuthority() {
    // TODO: this become somewhat messy because of the current cachig implementation
    // Perhps we could use AOP here as well to simplify?
    assertThrowsAccessDenied("operation:search",
        this::assertCachingSearchSuccess);
    assertThrowsAccessDenied("operation:search",
        this::assertCachingSearchWithFilterSuccess);
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:search"})
  public void testPassIfCachingSearchWithAuthority() {
    assertCachingSearchSuccess();
  }
}
