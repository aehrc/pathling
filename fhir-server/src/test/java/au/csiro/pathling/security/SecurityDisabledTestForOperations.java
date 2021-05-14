package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;


/**
 * See: https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html See
 * (ContextInitializer) https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property
 */
@TestPropertySource(properties = {
    "pathling.auth.enabled=false",
    "pathling.caching.enabled=false"
})
public class SecurityDisabledTestForOperations extends SecurityTestForOperations {

  @Test
  public void testPassIfImportWithNoAuth() {
    assertImportSuccess();
  }

  @Test
  public void testPassIfAggregateWithNoAuth() {
    assertAggregateSuccess();
  }

  @Test
  public void testPassIfSearchWithNoAuth() {
    assertSearchSuccess();
    assertSearchWithFilterSuccess();
  }

  @Test
  public void testPassIfCachingSearchNoAuth() {
    assertCachingSearchSuccess();
    assertCachingSearchWithFilterSuccess();
  }
}
